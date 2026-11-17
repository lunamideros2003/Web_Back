import { config } from '../config.js';
import { diffTally, readTopicState } from './tallyService.js';
import { isPublicTopic, LEVELS } from '../domain/election.js';
import { keys } from '../infra/keys.js';
import { redis, redisGateway } from '../infra/redis.js';
import { actaRepository } from '../infra/supabaseRepository.js';
import { logger } from '../lib/logger.js';
import { metrics } from '../metrics.js';
import { getPublicClientCount } from './clusterView.js';
import { historyService } from './historyService.js';

/**
 * Aggregation window.
 *
 * Instead of emitting one message per acta per connected client (the naive
 * fan-out), a single elected instance reads the dirty topics every 2s and
 * publishes one state message per topic, with the minimal delta. At the
 * observed profile this is a reduction of several orders of magnitude.
 *
 * Leader election lives in Redis (SET NX PX plus an owner-checked renewal), so
 * running eight instances does not duplicate every message eight times.
 */
export class WindowPublisher {
  constructor() {
    this.timer = null;
    this.running = false;
    this.isLeader = false;
    this.publishedTopics = 0;
  }

  start() {
    metrics.setGauge('publisherLeader', () => (this.isLeader ? 1 : 0));
    this.timer = setInterval(() => {
      this.tick().catch((error) => logger.error('Window publisher tick failed', { message: error?.message }));
    }, config.window.publishMs);
    if (this.timer.unref) this.timer.unref();
    logger.info('Window publisher started', { windowMs: config.window.publishMs });
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  async tick() {
    if (this.running) return;
    this.running = true;
    try {
      const leader = await redisGateway.run(() =>
        redis.leader(keys.windowLeader(), config.instanceId, String(config.window.leaderTtlMs)),
      );
      this.isLeader = Number(leader) === 1;
      if (!this.isLeader) return;
      await this.#publishDirtyTopics();
    } finally {
      this.running = false;
    }
  }

  async #publishDirtyTopics() {
    const dirty = await redisGateway.run(() => redis.smembers(keys.dirtyTopics()));
    if (dirty.length === 0) return;

    const batch = dirty.slice(0, config.window.maxTopicsPerTick);
    for (const topic of batch) {
      if (!isPublicTopic(topic)) {
        await redisGateway.run(() => redis.srem(keys.dirtyTopics(), topic));
        continue;
      }
      // eslint-disable-next-line no-await-in-loop
      await this.#publishTopic(topic);
      // eslint-disable-next-line no-await-in-loop
      await redisGateway.run(() => redis.srem(keys.dirtyTopics(), topic));
      this.publishedTopics += 1;
    }

    if (dirty.length > batch.length) {
      setImmediate(() => {
        this.#publishDirtyTopics().catch(() => {});
      });
    }
  }

  async #publishTopic(topic) {
    const state = await readTopicState(topic);

    const pipeline = redis.pipeline();
    pipeline.get(keys.lastPublished(topic));
    pipeline.get(keys.windowActas(topic));
    pipeline.get(keys.windowFirst(topic));
    pipeline.incrby(keys.stats('messages:published'), 1);
    const replies = await redisGateway.run(() => pipeline.exec());

    let previous = null;
    try {
      previous = replies[0]?.[1] ? JSON.parse(replies[0][1]) : null;
    } catch {
      previous = null;
    }
    const actasInWindow = Number(replies[1]?.[1] ?? 0);
    const firstReception = Number(replies[2]?.[1] ?? 0);

    const payload = {
      type: 'update',
      topic: state.topic,
      level: state.level,
      id: state.id,
      tally: state.tally,
      totalVotes: state.totalVotes,
      reported: state.reported,
      rank: state.rank,
      changes: diffTally(previous?.tally, state.tally),
      actas: actasInWindow,
      windowMs: config.window.publishMs,
      serverTime: Date.now(),
    };

    if (config.demo.paddingBytes > 0) {
      // Fault injection for the backpressure demo; disabled (0) by default.
      payload.pad = 'x'.repeat(config.demo.paddingBytes);
    }

    const seq = await historyService.append(topic, payload);
    const message = JSON.stringify({ ...payload, seq });

    await redisGateway.run(() => {
      const cleanup = redis.pipeline();
      cleanup.set(keys.lastPublished(topic), JSON.stringify(payload), 'EX', 3600);
      cleanup.del(keys.windowActas(topic));
      cleanup.del(keys.windowFirst(topic));
      return cleanup.exec();
    });

    actaRepository.cacheSnapshot(topic, payload);

    await redisGateway.publish(keys.channel(topic), message);
    metrics.inc('messagesPublished');
    if (firstReception > 0) {
      metrics.observePublishLatency(Date.now() - firstReception);
    }

    if (state.level === LEVELS.DEPARTMENT && actasInWindow > 0) {
      // The naive baseline: every acta broadcast to every public client.
      const publicClients = await getPublicClientCount();
      const naive = actasInWindow * publicClients;
      metrics.addNaiveMessages(naive);
      if (naive > 0) {
        await redisGateway
          .run(() => redis.incrby(keys.stats('messages:naive'), naive))
          .catch(() => {});
      }
    }
  }
}

export const windowPublisher = new WindowPublisher();
