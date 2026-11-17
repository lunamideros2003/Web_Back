import { config } from '../config.js';
import { compareStreamIds, parseTopic } from '../domain/election.js';
import { keys } from '../infra/keys.js';
import { redis, redisGateway } from '../infra/redis.js';

const parseEntry = ([id, fields]) => {
  let payload = null;
  for (let index = 0; index < fields.length; index += 2) {
    if (fields[index] === 'p') {
      try {
        payload = JSON.parse(fields[index + 1]);
      } catch {
        payload = null;
      }
    }
  }
  return payload ? { id, payload } : null;
};

/**
 * Redis Streams are the continuity layer. Pub/Sub has no memory: what is
 * published while a client is away is lost forever. Each public topic has a
 * capped stream (~200 revisions, ~6 minutes at the 2s window) so a client can
 * reconnect with Last-Event-ID and receive exactly the missed updates.
 */
export const historyService = {
  async append(topic, payload) {
    const streamKey = keys.history(topic);
    const id = await redisGateway.run(() =>
      redis.xadd(
        streamKey,
        'MAXLEN',
        '~',
        String(config.history.maxEntries),
        '*',
        'p',
        JSON.stringify(payload),
      ),
    );
    await redisGateway.run(() => redis.expire(streamKey, config.history.ttlSeconds));
    return id;
  },

  async readSince(topic, lastEventId) {
    const streamKey = keys.history(topic);
    const pipeline = redis.pipeline();
    pipeline.xrange(streamKey, '-', '+', 'COUNT', 1);
    pipeline.xrevrange(streamKey, '+', '-', 'COUNT', 1);
    pipeline.xrange(streamKey, `(${lastEventId}`, '+', 'COUNT', config.history.replayMax + 1);
    const replies = await redisGateway.run(() => pipeline.exec());

    const oldest = replies[0]?.[1]?.[0]?.[0] ?? null;
    const newest = replies[1]?.[1]?.[0]?.[0] ?? null;
    const rawEntries = replies[2]?.[1] ?? [];

    if (!oldest || !newest) {
      return { entries: [], truncated: true, oldest: null, newest: null };
    }

    // The requested point fell out of the retention window: only a full
    // snapshot can guarantee the same state as a client that never left.
    if (compareStreamIds(lastEventId, oldest) < 0) {
      return { entries: [], truncated: true, oldest, newest };
    }

    if (rawEntries.length > config.history.replayMax) {
      return { entries: [], truncated: true, oldest, newest };
    }

    const entries = rawEntries.map(parseEntry).filter(Boolean);
    return { entries, truncated: false, oldest, newest };
  },

  async latestId(topic) {
    const newest = await redisGateway.run(() => redis.xrevrange(keys.history(topic), '+', '-', 'COUNT', 1));
    return newest?.[0]?.[0] ?? null;
  },

  async size(topic) {
    return redisGateway.run(() => redis.xlen(keys.history(parseTopic(topic).key)));
  },
};
