import { randomUUID } from 'node:crypto';
import { WebSocketServer } from 'ws';
import { config } from '../config.js';
import { logger, serializeError } from '../lib/logger.js';
import { metrics } from '../metrics.js';
import { submitActa } from '../services/actaService.js';
import { connectionLimiter } from '../services/rateLimiter.js';

const PATH = '/ws/jury';

const reject = (socket, status, message) => {
  try {
    socket.write(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  } catch {
    // Socket already closed.
  }
  socket.destroy();
};

/**
 * Jury transport: WebSocket.
 *
 * Bidirectional by nature: the juror submits an acta and needs an explicit,
 * per-acta acknowledgement (accepted / duplicate / superseded / unavailable).
 * HTTP polling would add a round trip per retry on already poor mobile links.
 */
export const createJuryGateway = (httpServer) => {
  const wss = new WebSocketServer({ noServer: true, maxPayload: 512 * 1024, perMessageDeflate: false });
  let connections = 0;
  const sockets = new Set();

  metrics.setGauge('juryConnections', () => connections);

  httpServer.on('upgrade', async (req, socket, head) => {
    let url;
    try {
      url = new URL(req.url, 'http://internal');
    } catch {
      socket.destroy();
      return;
    }
    if (url.pathname !== PATH) return;

    const token = url.searchParams.get('token');
    if (token !== config.tokens.jury) {
      reject(socket, 401, 'Unauthorized');
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
      // Fail open on the connection limiter: the acta limiter still protects.
    }

    const jurorId = (url.searchParams.get('jurorId') ?? '').slice(0, 64) || `JUR-${randomUUID().slice(0, 8)}`;

    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req, { jurorId });
    });
  });

  wss.on('connection', (ws, req, context = {}) => {
    connections += 1;
    sockets.add(ws);
    metrics.inc('connectionsOpened');
    ws.isAlive = true;
    ws.jurorId = context.jurorId ?? 'unknown';

    const send = (payload) => {
      if (ws.readyState !== ws.OPEN) return;
      if (ws.bufferedAmount > config.fanout.closeBytes) {
        metrics.inc('slowClientsClosed');
        ws.close(1013, 'slow_consumer');
        return;
      }
      ws.send(JSON.stringify(payload));
    };

    send({
      type: 'hello',
      instanceId: config.instanceId,
      jurorId: ws.jurorId,
      heartbeatMs: config.jury.heartbeatMs,
      serverTime: Date.now(),
    });

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

      if (message.type === 'ping') {
        send({ type: 'pong', t: Date.now() });
        return;
      }

      if (message.type !== 'acta.submit') {
        send({ type: 'error', code: 'UNSUPPORTED', message: `Unsupported message type: ${message.type}` });
        return;
      }

      const requestId = message.requestId ?? randomUUID();
      const acta = { ...(message.acta ?? {}) };
      if (!acta.idem) acta.idem = requestId;

      try {
        const result = await submitActa({ payload: acta, instanceId: config.instanceId, source: 'ws' });
        metrics.inc('wsMessagesOut');
        send({ type: 'ack', requestId, ...result });
      } catch (error) {
        logger.error('Jury submission failed', { requestId, ...serializeError(error) });
        metrics.inc('wsMessagesOut');
        send({
          type: 'ack',
          requestId,
          ok: false,
          status: 'error',
          retryable: true,
          retryAfterMs: 3000,
          instanceId: config.instanceId,
          message: 'Internal error while registering the acta. Retry with the same idempotency key.',
        });
      }
    });

    ws.on('close', () => {
      connections -= 1;
      sockets.delete(ws);
    });

    ws.on('error', (error) => {
      logger.debug('Jury socket error', { jurorId: ws.jurorId, message: error?.message });
    });
  });

  const heartbeat = setInterval(() => {
    for (const ws of sockets) {
      if (ws.isAlive === false) {
        ws.terminate();
        continue;
      }
      ws.isAlive = false;
      ws.ping();
    }
  }, config.jury.heartbeatMs);
  if (heartbeat.unref) heartbeat.unref();

  return {
    wss,
    close: () => {
      clearInterval(heartbeat);
      for (const ws of sockets) ws.close(1001, 'server_shutdown');
      wss.close();
    },
  };
};
