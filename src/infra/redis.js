import { EventEmitter } from 'node:events';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import Redis from 'ioredis';
import { config } from '../config.js';
import { logger, serializeError } from '../lib/logger.js';
import { metrics } from '../metrics.js';

const here = dirname(fileURLToPath(import.meta.url));
const luaSource = (name) => readFileSync(join(here, 'lua', `${name}.lua`), 'utf8');

const SCRIPTS = {
  applyActa: { keys: 16, file: 'apply_acta' },
  acquireLock: { keys: 7, file: 'acquire_lock' },
  renewLock: { keys: 2, file: 'renew_lock' },
  releaseLock: { keys: 6, file: 'release_lock' },
  expireLock: { keys: 6, file: 'expire_lock' },
  rateLimit: { keys: 1, file: 'rate_limit' },
  leader: { keys: 1, file: 'leader' },
};

export class RedisUnavailableError extends Error {
  constructor(message = 'Redis is unavailable', options) {
    super(message, options);
    this.name = 'RedisUnavailableError';
    this.code = 'REDIS_UNAVAILABLE';
    this.statusCode = 503;
    this.retryable = true;
  }
}

/**
 * Owns every connection to Redis and centralises the failure policy.
 *
 * When the circuit opens, calls fail fast with RedisUnavailableError instead of
 * queueing forever. Callers decide the degraded behaviour: the jury never gets
 * a fake confirmation, the public stream falls back to its last snapshot.
 */
class RedisGateway extends EventEmitter {
  constructor() {
    super();
    this.status = 'connecting';
    this.failures = 0;
    this.openUntil = 0;
    this.lastError = null;
    this.channels = new Set();
    this.channelHandlers = new Map();
    this.lastStatusLog = '';

    this.client = this.#createClient('command');
    this.publisher = this.#createClient('publisher');
    this.subscriber = this.#createClient('subscriber');

    for (const [name, { keys, file }] of Object.entries(SCRIPTS)) {
      this.client.defineCommand(name, { numberOfKeys: keys, lua: luaSource(file) });
    }

    this.subscriber.on('message', (channel, message) => {
      const handler = this.channelHandlers.get(channel);
      if (!handler) return;
      try {
        handler(message, channel);
      } catch (error) {
        logger.error('Redis subscriber handler failed', { channel, ...serializeError(error) });
      }
    });

    this.subscriber.on('ready', () => {
      // ioredis restores subscriptions on reconnect; re-assert them anyway so a
      // subscription created while the link was down is not lost.
      const channels = [...this.channels];
      if (channels.length === 0) return;
      this.subscriber
        .subscribe(...channels)
        .then(() => logger.info('Re-subscribed to Redis channels', { channels: channels.length }))
        .catch((error) => logger.error('Re-subscribe failed', serializeError(error)));
    });

    this.client.on('ready', () => this.#setStatus('ready'));
    this.client.on('close', () => this.#setStatus('degraded'));
    this.client.on('end', () => this.#setStatus('down'));
    this.client.on('error', (error) => {
      this.lastError = error;
      metrics.redisStatus = 'degraded';
    });
  }

  #createClient(role) {
    // rediss:// already turns TLS on inside ioredis and keeps the correct SNI
    // hostname. Forcing tls.servername = undefined breaks Render Key Value,
    // whose certificate is only valid for the host in the connection string.
    const urlIsTls = config.redis.url.startsWith('rediss://');
    const client = new Redis(config.redis.url, {
      maxRetriesPerRequest: 1,
      enableOfflineQueue: false,
      connectTimeout: config.redis.connectTimeoutMs,
      retryStrategy: (attempt) => Math.min(attempt * 200, 3000),
      ...(config.redis.tls && !urlIsTls ? { tls: {} } : {}),
    });

    client.on('error', (error) => {
      if (client === this.client) this.lastError = error;
      logger.warn('Redis connection error', { role, ...serializeError(error) });
    });
    if (role !== 'command') client.on('ready', () => logger.debug('Redis client ready', { role }));
    return client;
  }

  #setStatus(status) {
    this.status = status;
    metrics.redisStatus = status;
    if (status !== this.lastStatusLog) {
      this.lastStatusLog = status;
      logger.info('Redis status changed', { status });
      this.emit('status', status);
    }
    if (status === 'ready') {
      this.failures = 0;
      this.openUntil = 0;
    }
  }

  isReady() {
    return this.status === 'ready';
  }

  isCircuitOpen() {
    return Date.now() < this.openUntil;
  }

  /** Wraps any Redis operation with the shared failure and circuit policy. */
  async run(operation) {
    if (this.isCircuitOpen()) {
      throw new RedisUnavailableError('Redis circuit is open');
    }
    try {
      const result = await operation();
      if (this.failures > 0) this.failures = 0;
      if (this.status !== 'ready' && this.client.status === 'ready') this.#setStatus('ready');
      return result;
    } catch (error) {
      this.failures += 1;
      this.lastError = error;
      metrics.inc('redisErrors');
      if (this.failures >= config.redis.failureThreshold) {
        this.openUntil = Date.now() + config.redis.circuitCooldownMs;
        if (this.status !== 'degraded') this.#setStatus('degraded');
        logger.error('Redis circuit opened', {
          failures: this.failures,
          cooldownMs: config.redis.circuitCooldownMs,
          ...serializeError(error),
        });
      }
      if (error instanceof RedisUnavailableError) throw error;
      throw new RedisUnavailableError('Redis operation failed', { cause: error });
    }
  }

  async publish(channel, payload) {
    return this.run(() => this.publisher.publish(channel, payload));
  }

  async subscribe(channel, handler) {
    this.channelHandlers.set(channel, handler);
    if (this.channels.has(channel)) return;
    this.channels.add(channel);
    await this.waitUntilReady(5000);
    await this.run(() => this.subscriber.subscribe(channel));
  }

  /** Resolves true once Redis is usable, or false after the timeout. */
  waitUntilReady(timeoutMs = 5000) {
    if (this.status === 'ready' || this.client.status === 'ready') return Promise.resolve(true);
    return new Promise((resolve) => {
      let timer;
      const handler = (status) => {
        if (status !== 'ready') return;
        cleanup();
        resolve(true);
      };
      const cleanup = () => {
        clearTimeout(timer);
        this.off('status', handler);
      };
      this.on('status', handler);
      timer = setTimeout(() => {
        cleanup();
        resolve(false);
      }, timeoutMs);
      if (timer.unref) timer.unref();
    });
  }

  async unsubscribe(channel) {
    if (!this.channels.has(channel)) return;
    this.channels.delete(channel);
    this.channelHandlers.delete(channel);
    await this.run(() => this.subscriber.unsubscribe(channel));
  }

  /** Used by the demo fault injection endpoint. */
  async simulateOutage(enabled) {
    if (enabled) {
      this.client.disconnect();
      this.publisher.disconnect();
      this.subscriber.disconnect();
      this.#setStatus('down');
      return;
    }
    await Promise.all([this.client.connect(), this.publisher.connect(), this.subscriber.connect()]);
    this.#setStatus('ready');
  }

  async close() {
    await Promise.allSettled([this.client.quit(), this.publisher.quit(), this.subscriber.quit()]);
  }

  async ping() {
    return this.run(() => this.client.ping());
  }
}

export const redisGateway = new RedisGateway();
export const redis = redisGateway.client;
