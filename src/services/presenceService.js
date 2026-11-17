import { config } from '../config.js';
import { keys } from '../infra/keys.js';
import { redis, redisGateway } from '../infra/redis.js';

const ttlMs = config.presence.ttlSeconds * 1000;

/**
 * Presence lives in Redis, not in a process, so the coordinator sees auditors
 * connected to any of the eight instances. TTL plus a heartbeat makes ghost
 * auditors disappear on their own (laptop closed, no clean disconnect).
 */
export const presenceService = {
  async heartbeat(auditorId, fields = {}) {
    const now = Date.now();
    const key = keys.presence(auditorId);
    const pipeline = redis.pipeline();
    pipeline.hset(key, {
      auditorId,
      instanceId: config.instanceId,
      lastSeen: String(now),
      ...fields,
    });
    pipeline.hsetnx(key, 'connectedAt', String(now));
    pipeline.expire(key, config.presence.ttlSeconds);
    pipeline.zadd(keys.presenceIndex(), now, auditorId);
    await redisGateway.run(() => pipeline.exec());
    return { auditorId, lastSeen: now, instanceId: config.instanceId };
  },

  async setCurrentCase(auditorId, actaId) {
    const now = Date.now();
    const pipeline = redis.pipeline();
    if (actaId) {
      pipeline.hset(keys.presence(auditorId), {
        currentActa: actaId,
        claimedAt: String(now),
        lastSeen: String(now),
      });
    } else {
      pipeline.hdel(keys.presence(auditorId), 'currentActa', 'claimedAt');
      pipeline.hset(keys.presence(auditorId), 'lastSeen', String(now));
    }
    pipeline.expire(keys.presence(auditorId), config.presence.ttlSeconds);
    pipeline.zadd(keys.presenceIndex(), now, auditorId);
    await redisGateway.run(() => pipeline.exec());
  },

  async remove(auditorId) {
    const pipeline = redis.pipeline();
    pipeline.del(keys.presence(auditorId));
    pipeline.zrem(keys.presenceIndex(), auditorId);
    await redisGateway.run(() => pipeline.exec());
  },

  async list() {
    const now = Date.now();
    const ids = await redisGateway.run(() =>
      redis.zrangebyscore(keys.presenceIndex(), now - ttlMs, '+inf', 'LIMIT', 0, 200),
    );
    if (ids.length === 0) return [];

    const pipeline = redis.pipeline();
    for (const id of ids) pipeline.hgetall(keys.presence(id));
    const replies = await redisGateway.run(() => pipeline.exec());

    return replies
      .map(([error, raw]) => {
        if (error || !raw || Object.keys(raw).length === 0) return null;
        const claimedAt = Number(raw.claimedAt ?? 0);
        return {
          auditorId: raw.auditorId,
          instanceId: raw.instanceId,
          currentActa: raw.currentActa ?? null,
          connectedAt: Number(raw.connectedAt ?? 0),
          lastSeen: Number(raw.lastSeen ?? 0),
          busyForMs: claimedAt > 0 ? now - claimedAt : 0,
          onlineForMs: now - Number(raw.connectedAt ?? now),
        };
      })
      .filter(Boolean);
  },

  /** Returns the ids that were considered ghosts and removed. */
  async sweep() {
    const now = Date.now();
    const stale = await redisGateway.run(() =>
      redis.zrangebyscore(keys.presenceIndex(), '-inf', now - ttlMs, 'LIMIT', 0, 100),
    );
    if (stale.length === 0) return [];

    const pipeline = redis.pipeline();
    for (const id of stale) {
      pipeline.del(keys.presence(id));
      pipeline.zrem(keys.presenceIndex(), id);
    }
    await redisGateway.run(() => pipeline.exec());
    return stale;
  },
};
