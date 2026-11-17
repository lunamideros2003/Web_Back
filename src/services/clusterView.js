import { keys } from '../infra/keys.js';
import { redis, redisGateway } from '../infra/redis.js';

const CACHE_MS = 2000;
let cache = { at: 0, instances: [] };

/**
 * Cross-instance view built only with Redis. No instance talks to another one:
 * each instance writes its own telemetry hash and reads the shared index.
 */
export const publishInstanceTelemetry = async (snapshot) => {
  const now = Date.now();
  const pipeline = redis.pipeline();
  pipeline.hset(keys.instanceTelemetry(), snapshot.instanceId, JSON.stringify(snapshot));
  pipeline.zadd(keys.instanceAlive(), now, snapshot.instanceId);
  pipeline.zremrangebyscore(keys.instanceAlive(), '-inf', now - 120_000);
  await redisGateway.run(() => pipeline.exec());
};

export const getClusterInstances = async ({ maxAgeMs = 20_000, cacheMs = CACHE_MS } = {}) => {
  if (cacheMs > 0 && Date.now() - cache.at < cacheMs) return cache.instances;

  const now = Date.now();
  const ids = await redisGateway.run(() =>
    redis.zrangebyscore(keys.instanceAlive(), now - maxAgeMs, '+inf'),
  );
  if (ids.length === 0) {
    cache = { at: Date.now(), instances: [] };
    return [];
  }

  const values = await redisGateway.run(() => redis.hmget(keys.instanceTelemetry(), ...ids));
  const instances = [];
  ids.forEach((id, index) => {
    try {
      if (values[index]) instances.push(JSON.parse(values[index]));
    } catch {
      // Ignore a partially written telemetry row; the next report replaces it.
    }
  });

  cache = { at: Date.now(), instances };
  return instances;
};

export const getPublicClientCount = async () => {
  try {
    const instances = await getClusterInstances();
    return instances.reduce((sum, item) => sum + (item?.connections?.sse ?? 0), 0);
  } catch {
    return 0;
  }
};

export const invalidateClusterCache = () => {
  cache = { at: 0, instances: [] };
};
