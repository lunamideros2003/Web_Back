import { config } from '../config.js';
import { keys } from '../infra/keys.js';
import { redisGateway } from '../infra/redis.js';
import { logger } from '../lib/logger.js';
import { metrics } from '../metrics.js';

const MAX_SNAPSHOTS = 2500;
const yieldToEventLoop = () => new Promise((resolve) => setImmediate(resolve));

/**
 * Fan-out engine.
 *
 * One Redis Pub/Sub channel per public topic. An instance subscribes only to
 * the topics that at least one of its clients is watching, so the cost is
 * proportional to the subscribers of that topic and not to the 50.000 sockets
 * of the instance.
 *
 * Delivery is batched and yields to the Event Loop between batches: a burst
 * never blocks jurors who are transmitting their actas.
 */
export class TopicHub {
  constructor() {
    this.topics = new Map();
    this.snapshots = new Map();
    this.deliveryBatches = 0;
  }

  #ensureEntry(topic) {
    let entry = this.topics.get(topic);
    if (!entry) {
      entry = {
        topic,
        clients: new Set(),
        refs: 0,
        running: false,
        pending: null,
        subscribed: false,
      };
      this.topics.set(topic, entry);
    }
    return entry;
  }

  async #subscribe(entry) {
    if (entry.subscribed) return;
    entry.subscribed = true;
    try {
      await redisGateway.subscribe(keys.channel(entry.topic), (message) => {
        this.#enqueue(entry.topic, message);
      });
    } catch (error) {
      entry.subscribed = false;
      logger.warn('Topic subscription failed; will retry on Redis ready', {
        topic: entry.topic,
        message: error?.message,
      });
    }
  }

  async add(topic, client) {
    const entry = this.#ensureEntry(topic);
    entry.clients.add(client);
    entry.refs += 1;
    await this.#subscribe(entry);
  }

  async remove(topic, client) {
    const entry = this.topics.get(topic);
    if (!entry) return;
    entry.clients.delete(client);

    if (entry.clients.size === 0) {
      this.topics.delete(topic);
      if (entry.subscribed) {
        try {
          await redisGateway.unsubscribe(keys.channel(topic));
        } catch {
          // The subscriber reconnects with its own channel set; nothing to do.
        }
      }
    }
  }

  /** Called after a Redis reconnect: re-arm every channel still in use. */
  async resubscribeAll() {
    for (const entry of this.topics.values()) {
      entry.subscribed = false;
      await this.#subscribe(entry);
    }
  }

  #enqueue(topic, message) {
    const entry = this.topics.get(topic);
    if (!entry || entry.clients.size === 0) return;

    entry.pending = message;
    if (entry.running) return;

    entry.running = true;
    this.#drain(entry)
      .catch((error) => logger.error('Fan-out failed', { topic, message: error?.message }))
      .finally(() => {
        entry.running = false;
        if (entry.pending) this.#enqueue(topic, entry.pending);
      });
  }

  async #drain(entry) {
    while (entry.pending) {
      const message = entry.pending;
      entry.pending = null;

      let payload;
      try {
        payload = JSON.parse(message);
      } catch {
        logger.warn('Invalid topic message dropped', { topic: entry.topic });
        continue;
      }

      if (payload.topic === 'dept') {
        this.snapshots.set('dept', payload);
      } else {
        this.snapshots.set(entry.topic, payload);
        if (this.snapshots.size > MAX_SNAPSHOTS) {
          const oldest = this.snapshots.keys().next().value;
          this.snapshots.delete(oldest);
        }
      }

      const clients = [...entry.clients];
      const batchSize = Math.max(50, config.fanout.batchSize);

      for (let offset = 0; offset < clients.length; offset += batchSize) {
        const batch = clients.slice(offset, offset + batchSize);
        for (const client of batch) {
          if (client.closed) {
            entry.clients.delete(client);
            continue;
          }
          try {
            client.deliver(payload);
          } catch (error) {
            logger.warn('Client delivery failed', { client: client.id, message: error?.message });
          }
        }
        this.deliveryBatches += 1;
        // Yield so timers, heartbeats and acta submissions keep being served.
        if (offset + batchSize < clients.length) await yieldToEventLoop();
      }
    }
  }

  getSnapshot(topic) {
    return this.snapshots.get(topic) ?? null;
  }

  stats() {
    let clients = 0;
    for (const entry of this.topics.values()) clients += entry.clients.size;
    return { topics: this.topics.size, clients, deliveryBatches: this.deliveryBatches };
  }

  registerGauges() {
    metrics.setGauge('sseConnections', () => this.stats().clients);
    metrics.setGauge('sseTopics', () => this.stats().topics);
  }
}

export const hub = new TopicHub();
