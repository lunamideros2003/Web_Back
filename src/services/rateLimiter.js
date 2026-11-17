import { config } from '../config.js';
import { keys } from '../infra/keys.js';
import { redis, redisGateway } from '../infra/redis.js';

/**
 * Fixed-window counter in Redis, one key per credential and minute. Exceeding
 * the limit returns a retry hint instead of silently dropping the request.
 */
export const enforceRateLimit = async ({
  scope,
  subject,
  limit,
  windowMs,
  failOpen = false,
}) => {
  const windowStart = Math.floor(Date.now() / windowMs) * windowMs;
  const key = keys.rateLimit(scope, subject, windowStart);

  try {
    const [allowed, retryAfterMs] = await redisGateway.run(() => redis.rateLimit(key, limit, windowMs));
    return {
      allowed: Number(allowed) === 1,
      retryAfterMs: Number(retryAfterMs),
      limit,
      windowMs,
    };
  } catch (error) {
    if (failOpen) {
      return { allowed: true, retryAfterMs: 0, limit, windowMs, degraded: true };
    }
    throw error;
  }
};

export const connectionLimiter = (ip) =>
  enforceRateLimit({
    scope: 'ws-connect',
    subject: ip,
    limit: config.rateLimit.connectionsPerMinute,
    windowMs: 60_000,
    failOpen: true,
  });
