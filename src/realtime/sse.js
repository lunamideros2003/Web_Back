import { randomUUID } from 'node:crypto';
import express from 'express';
import { config } from '../config.js';
import { compareStreamIds, parseTopic } from '../domain/election.js';
import { actaRepository } from '../infra/supabaseRepository.js';
import { redisGateway } from '../infra/redis.js';
import { logger } from '../lib/logger.js';
import { metrics } from '../metrics.js';
import { historyService } from '../services/historyService.js';
import { readTopicState } from '../services/tallyService.js';
import { hub } from './hub.js';

const clients = new Set();

const format = (event, data, id) =>
  `${id ? `id: ${id}\n` : ''}event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;

const stateFor = async (topic) => {
  try {
    return await readTopicState(topic);
  } catch (error) {
    const cached = hub.getSnapshot(topic);
    if (cached) return { ...cached, stale: true, reason: 'redis_unavailable' };
    const persisted = await actaRepository.readLatestSnapshot(topic);
    if (persisted) return { ...persisted.payload, stale: true, reason: 'redis_unavailable' };
    logger.warn('No snapshot available for topic', { topic, message: error?.message });
    return null;
  }
};

const sortBySeq = (items) =>
  items.sort((left, right) => compareStreamIds(left.seq ?? '0-0', right.seq ?? '0-0'));

class SseClient {
  constructor({ topic, res, lastEventId }) {
    this.id = randomUUID();
    this.topic = topic.key;
    this.res = res;
    this.closed = false;
    this.lagging = false;
    this.replaying = true;
    this.pending = [];
    this.lastSentId = lastEventId || null;
    this.needsSnapshot = false;
    this.degraded = !redisGateway.isReady();
    this.heartbeat = null;
  }

  writeEvent(event, data, id) {
    if (this.closed) return false;
    const ok = this.res.write(format(event, data, id));
    this.#checkPressure();
    if (event === 'update') metrics.inc('sseEventsSent');
    return ok;
  }

  accept(seq) {
    if (!seq) return true;
    if (this.lastSentId && compareStreamIds(seq, this.lastSentId) <= 0) return false;
    this.lastSentId = seq;
    return true;
  }

  deliver(payload) {
    if (this.closed) return;
    if (this.replaying) {
      this.pending.push(payload);
      return;
    }
    if (!this.accept(payload.seq)) return;

    if (this.lagging) {
      if ((this.res.writableLength ?? 0) > config.fanout.resumeBytes) {
        metrics.inc('sseEventsSkipped');
        return;
      }
      this.lagging = false;
      this.needsSnapshot = true;
    }

    if (this.needsSnapshot) {
      this.needsSnapshot = false;
      void this.sendSnapshot('recovered');
      return;
    }

    this.writeEvent('update', payload, payload.seq);
  }

  async sendSnapshot(reason) {
    const state = await stateFor(this.topic);
    if (this.closed) return;
    if (!state) {
      this.writeEvent('degraded', { reason: 'no_state', retryAfterMs: config.sse.retryMs });
      return;
    }
    try {
      const latest = await historyService.latestId(this.topic);
      if (latest) this.lastSentId = latest;
    } catch {
      // Keep the previous cursor; the next update carries a newer one.
    }
    this.writeEvent('snapshot', { ...state, reason });
  }

  async start(lastEventId) {
    await hub.add(this.topic, this).catch(() => {});

    let resumed = false;
    if (lastEventId) {
      try {
        const replay = await historyService.readSince(this.topic, lastEventId);
        if (!replay.truncated) {
          for (const entry of replay.entries) {
            if (this.accept(entry.id)) this.writeEvent('update', entry.payload, entry.id);
          }
          resumed = true;
        }
      } catch (error) {
        logger.warn('History replay failed', { topic: this.topic, message: error?.message });
      }
    }

    if (!resumed) {
      await this.sendSnapshot(lastEventId ? 'history_truncated' : 'initial');
    }

    this.replaying = false;
    const pending = sortBySeq(this.pending);
    this.pending = [];
    for (const payload of pending) this.deliver(payload);

    this.writeEvent('ready', {
      instanceId: config.instanceId,
      topic: this.topic,
      resumed,
      retryMs: config.sse.retryMs,
      serverTime: Date.now(),
    });

    this.heartbeat = setInterval(() => this.#heartbeatTick(), config.sse.heartbeatMs);
  }

  #heartbeatTick() {
    if (this.closed) return;
    const buffered = this.res.writableLength ?? 0;

    if (buffered > config.fanout.closeBytes) {
      this.close('slow_consumer');
      return;
    }
    if (this.lagging && buffered <= config.fanout.resumeBytes) {
      this.lagging = false;
      this.needsSnapshot = true;
      metrics.inc('sseEventsSkipped', 0);
    }
    if (this.needsSnapshot) {
      this.needsSnapshot = false;
      void this.sendSnapshot('recovered');
      return;
    }
    if (!this.closed) this.res.write(': hb\n\n');
  }

  #checkPressure() {
    if (this.closed) return;
    const buffered = this.res.writableLength ?? 0;
    if (buffered > config.fanout.closeBytes) {
      this.close('slow_consumer');
      return;
    }
    if (buffered > config.fanout.slowBytes && !this.lagging) {
      this.lagging = true;
      metrics.inc('sseEventsSkipped');
    }
  }

  close(reason = 'closed') {
    if (this.closed) return;
    this.closed = true;
    if (this.heartbeat) clearInterval(this.heartbeat);
    clients.delete(this);
    hub.remove(this.topic, this).catch(() => {});
    if (reason === 'slow_consumer') {
      metrics.inc('slowClientsClosed');
      logger.warn('Slow SSE client disconnected for backpressure', {
        client: this.id,
        topic: this.topic,
        bufferedBytes: this.res.writableLength ?? 0,
      });
    }
    try {
      this.res.end();
    } catch {
      // Socket already gone.
    }
  }

  onRedisStatus(status) {
    if (this.closed) return;
    if (status === 'ready') {
      this.degraded = false;
      void this.sendSnapshot('redis_recovered');
      return;
    }
    if (!this.degraded) {
      this.degraded = true;
      this.writeEvent('degraded', {
        reason: 'redis_unavailable',
        message: 'Live updates paused. Showing the last consistent snapshot.',
        retryAfterMs: config.sse.retryMs,
      });
    }
  }
}

export const createSseRouter = () => {
  const router = express.Router();

  router.get('/events', async (req, res) => {
    const rawTopic = typeof req.query.topic === 'string' ? req.query.topic : 'dept';
    let topic;
    try {
      topic = parseTopic(rawTopic);
    } catch (error) {
      res.status(400).json({ error: error.message });
      return;
    }

    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders?.();
    res.socket?.setNoDelay(true);
    res.socket?.setTimeout(0);

    const lastEventId = String(
      req.headers['last-event-id'] ?? req.query.lastEventId ?? '',
    ).trim();

    const client = new SseClient({ topic, res, lastEventId });

    res.write(`retry: ${config.sse.retryMs}\n\n`);
    clients.add(client);
    metrics.inc('connectionsOpened');
    if (lastEventId) metrics.inc('reconnects');

    const cleanup = () => {
      if (client.closed) return;
      client.closed = true;
      if (client.heartbeat) clearInterval(client.heartbeat);
      clients.delete(client);
      hub.remove(client.topic, client).catch(() => {});
    };
    req.on('close', cleanup);
    res.on('close', cleanup);
    res.on('error', cleanup);

    try {
      await client.start(lastEventId);
    } catch (error) {
      logger.error('SSE start failed', { message: error?.message });
      client.close('error');
    }
  });

  return router;
};

redisGateway.on('status', (status) => {
  for (const client of clients) client.onRedisStatus(status);
});

export const sseClientCount = () => clients.size;
