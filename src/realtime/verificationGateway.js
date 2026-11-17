import { WebSocketServer } from 'ws';
import { config } from '../config.js';
import { keys } from '../infra/keys.js';
import { redisGateway } from '../infra/redis.js';
import { logger, serializeError } from '../lib/logger.js';
import { metrics } from '../metrics.js';
import { presenceService } from '../services/presenceService.js';
import {
  claimCase,
  getAuditorStats,
  getCoordinatorSnapshot,
  publishVerification,
  releaseAllForAuditor,
  releaseCase,
  renewAuditorLocks,
} from '../services/verificationService.js';
import { connectionLimiter } from '../services/rateLimiter.js';

const PATH = '/ws/verification';

const reject = (socket, status, message) => {
  try {
    socket.write(`HTTP/1.1 ${status} ${message}\r\nConnection: Close\r\nContent-Length: 0\r\n\r\n`);
  } catch {
    // Already gone.
  }
  socket.destroy();
};

/**
 * Verification transport: WebSocket.
 *
 * Collaborative and bidirectional: 120 auditors share one queue, see each
 * other taking cases and update their consoles without reloading. Presence and
 * locks live in Redis, so the console behaves the same on any of the eight
 * instances.
 */
export const createVerificationGateway = (httpServer) => {
  const wss = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024, perMessageDeflate: false });
  const byAuditor = new Map();
  let connections = 0;

  metrics.setGauge('verificationConnections', () => connections);

  const subscribe = async () => {
    try {
      await redisGateway.subscribe(keys.verificationChannel(), (message) => {
        for (const sockets of byAuditor.values()) {
          for (const ws of sockets) {
            if (ws.readyState !== ws.OPEN) continue;
            if (ws.bufferedAmount > config.fanout.closeBytes) {
              metrics.inc('slowClientsClosed');
              ws.close(1013, 'slow_consumer');
              continue;
            }
            ws.send(message);
            metrics.inc('wsMessagesOut');
          }
        }
      });
    } catch (error) {
      logger.warn('Verification bus subscription failed; retrying on Redis ready', {
        message: error?.message,
      });
    }
  };

  void subscribe();
  redisGateway.on('status', (status) => {
    if (status === 'ready') void subscribe();
  });

  httpServer.on('upgrade', async (req, socket, head) => {
    let url;
    try {
      url = new URL(req.url, 'http://internal');
    } catch {
      socket.destroy();
      return;
    }
    if (url.pathname !== PATH) return;

    if (url.searchParams.get('token') !== config.tokens.auditor) {
      reject(socket, 401, 'Unauthorized');
      return;
    }

    const auditorId = (url.searchParams.get('auditorId') ?? '').trim().toUpperCase();
    if (auditorId.length < 3 || auditorId.length > 40) {
      reject(socket, 400, 'Bad Request');
      return;
    }

    const ip = req.socket.remoteAddress ?? 'unknown';
    try {
      const limit = await connectionLimiter(ip);
      if (!limit.allowed) {
        reject(socket, 429, 'Too Many Requests');
        return;
      }
    } catch {
      // Fail open.
    }

    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req, { auditorId });
    });
  });

  wss.on('connection', (ws, req, context = {}) => {
    const auditorId = context.auditorId;
    connections += 1;
    metrics.inc('connectionsOpened');
    ws.isAlive = true;
    ws.auditorId = auditorId;

    if (!byAuditor.has(auditorId)) byAuditor.set(auditorId, new Set());
    byAuditor.get(auditorId).add(ws);

    const send = (payload) => {
      if (ws.readyState !== ws.OPEN) return;
      if (ws.bufferedAmount > config.fanout.closeBytes) {
        metrics.inc('slowClientsClosed');
        ws.close(1013, 'slow_consumer');
        return;
      }
      ws.send(JSON.stringify(payload));
      metrics.inc('wsMessagesOut');
    };

    void (async () => {
      try {
        await presenceService.heartbeat(auditorId);
        await publishVerification({ type: 'presence.updated', auditorId, action: 'connected' });
        const [snapshot, stats] = await Promise.all([
          getCoordinatorSnapshot(),
          getAuditorStats(auditorId),
        ]);
        send({
          type: 'init',
          instanceId: config.instanceId,
          auditorId,
          leaseMs: config.locks.leaseMs,
          heartbeatMs: config.presence.heartbeatMs,
          ...snapshot,
          stats,
        });
      } catch (error) {
        logger.error('Verification init failed', { auditorId, ...serializeError(error) });
        send({
          type: 'degraded',
          reason: 'redis_unavailable',
          message: 'Verification service is temporarily unavailable. Retrying.',
        });
      }
    })();

    ws.on('pong', () => {
      ws.isAlive = true;
    });

    ws.on('message', async (raw) => {
      metrics.inc('wsMessagesIn');
      let message;
      try {
        message = JSON.parse(raw.toString());
      } catch {
        send({ type: 'error', code: 'BAD_JSON', message: 'Message must be valid JSON.' });
        return;
      }

      try {
        switch (message.type) {
          case 'heartbeat': {
            await presenceService.heartbeat(auditorId);
            const renewal = await renewAuditorLocks(auditorId);
            send({ type: 'heartbeat.ack', ...renewal, serverTime: Date.now() });
            break;
          }

          case 'queue.refresh': {
            const [snapshot, stats] = await Promise.all([
              getCoordinatorSnapshot(),
              getAuditorStats(auditorId),
            ]);
            send({ type: 'state', ...snapshot, stats });
            break;
          }

          case 'case.claim': {
            if (!message.actaId) {
              send({ type: 'error', code: 'BAD_REQUEST', message: 'actaId is required.' });
              break;
            }
            const result = await claimCase({ auditorId, actaId: String(message.actaId) });
            if (result.ok) {
              await publishVerification({ type: 'presence.updated', auditorId, action: 'claimed' });
            }
            send({ type: 'claim.result', actaId: message.actaId, ...result });
            break;
          }

          case 'case.release': {
            const result = await releaseCase({
              auditorId,
              actaId: String(message.actaId),
              token: message.token,
              requeue: true,
              resolution: 'released',
            });
            await publishVerification({ type: 'presence.updated', auditorId, action: 'released' });
            send({ type: 'release.result', actaId: message.actaId, ...result });
            break;
          }

          case 'case.complete': {
            const result = await releaseCase({
              auditorId,
              actaId: String(message.actaId),
              token: message.token,
              requeue: false,
              resolution: String(message.resolution ?? 'reviewed'),
            });
            const stats = await getAuditorStats(auditorId);
            await publishVerification({ type: 'presence.updated', auditorId, action: 'completed' });
            send({ type: 'complete.result', actaId: message.actaId, ...result, stats });
            break;
          }

          case 'case.renew': {
            const renewal = await renewAuditorLocks(auditorId);
            send({ type: 'lock.renewed', ...renewal, serverTime: Date.now() });
            break;
          }

          case 'ping':
            send({ type: 'pong', serverTime: Date.now() });
            break;

          default:
            send({ type: 'error', code: 'UNSUPPORTED', message: `Unsupported message type: ${message.type}` });
        }
      } catch (error) {
        logger.error('Verification message failed', {
          auditorId,
          type: message?.type,
          ...serializeError(error),
        });
        send({
          type: 'error',
          code: 'INTERNAL',
          message: 'The operation could not be completed. Retry in a few seconds.',
          retryable: true,
        });
      }
    });

    ws.on('close', async () => {
      connections -= 1;
      const set = byAuditor.get(auditorId);
      if (set) {
        set.delete(ws);
        if (set.size === 0) {
          byAuditor.delete(auditorId);
          try {
            await releaseAllForAuditor(auditorId, 'disconnected');
            await presenceService.remove(auditorId);
            await publishVerification({ type: 'presence.updated', auditorId, action: 'disconnected' });
          } catch (error) {
            logger.warn('Auditor cleanup failed', { auditorId, message: error?.message });
          }
        }
      }
    });

    ws.on('error', (error) => {
      logger.debug('Verification socket error', { auditorId, message: error?.message });
    });
  });

  const heartbeat = setInterval(() => {
    for (const sockets of byAuditor.values()) {
      for (const ws of sockets) {
        if (ws.isAlive === false) {
          ws.terminate();
          continue;
        }
        ws.isAlive = false;
        ws.ping();
      }
    }
  }, config.jury.heartbeatMs);
  if (heartbeat.unref) heartbeat.unref();

  return {
    wss,
    close: () => {
      clearInterval(heartbeat);
      for (const sockets of byAuditor.values()) {
        for (const ws of sockets) ws.close(1001, 'server_shutdown');
      }
      wss.close();
    },
  };
};
