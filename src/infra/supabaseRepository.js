import { createClient } from '@supabase/supabase-js';
import { config } from '../config.js';
import { logger, serializeError } from '../lib/logger.js';
import { metrics } from '../metrics.js';

/**
 * Durable layer. Redis is the source of truth for the live election; Supabase
 * keeps the audit trail (every acta version) and periodic snapshots that the
 * public page can still read while Redis is unavailable.
 *
 * Writes are write-behind: the acta acknowledgement never waits for Postgres.
 * The queue is bounded, so a slow database can never grow memory without limit.
 */
class ActaRepository {
  constructor() {
    this.enabled = config.supabase.enabled;
    this.client = this.enabled
      ? createClient(config.supabase.url, config.supabase.serviceRoleKey, {
          auth: { persistSession: false, autoRefreshToken: false },
        })
      : null;
    this.queue = [];
    this.flushing = false;
    this.timer = null;
    this.cache = new Map();
    this.dropped = 0;
    this.persisted = 0;
  }

  start() {
    if (!this.enabled) {
      logger.warn('Supabase disabled: set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY to enable durable audit');
      return;
    }
    logger.info('Supabase write-behind enabled');
    this.#schedule(0);
  }

  get status() {
    return {
      enabled: this.enabled,
      queued: this.queue.length,
      persisted: this.persisted,
      dropped: this.dropped,
    };
  }

  enqueueActa(record) {
    if (!this.enabled) return;
    if (this.queue.length >= config.supabase.writeQueueLimit) {
      this.queue.shift();
      this.dropped += 1;
      metrics.inc('supabaseDropped');
      return;
    }
    this.queue.push(record);
    this.#schedule(250);
  }

  #schedule(delayMs) {
    if (!this.enabled || this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this.#flush().catch((error) => logger.error('Supabase flush failed', serializeError(error)));
    }, delayMs);
    if (this.timer.unref) this.timer.unref();
  }

  async #flush() {
    if (!this.enabled || this.flushing || this.queue.length === 0) return;
    this.flushing = true;
    const batch = this.queue.splice(0, 50);

    try {
      const rows = batch.map((item) => ({
        acta_id: item.acta.acta,
        version: item.acta.version,
        mesa: item.acta.mesa,
        puesto: item.acta.puesto,
        municipio: item.acta.municipio,
        juror_id: item.acta.jurado,
        votes: item.acta.votos,
        sufragantes: item.acta.sufragantes,
        total_votes: item.totalVotes,
        is_doubtful: item.doubtful,
        photo_hash: item.acta.hash_foto,
        idempotency_key: item.acta.idem,
        instance_id: item.instanceId,
        submitted_at: new Date().toISOString(),
      }));

      const { error } = await this.client
        .from('actas')
        .upsert(rows, { onConflict: 'acta_id,version', ignoreDuplicates: true });

      if (error) throw error;
      this.persisted += rows.length;
    } catch (error) {
      // Put the batch back at the front and retry later with backoff.
      this.queue.unshift(...batch.slice(0, config.supabase.writeQueueLimit));
      const overflow = this.queue.length - config.supabase.writeQueueLimit;
      if (overflow > 0) {
        this.queue.splice(0, overflow);
        this.dropped += overflow;
        metrics.inc('supabaseDropped', overflow);
      }
      logger.warn('Supabase batch failed; will retry', serializeError(error));
      this.flushing = false;
      this.#schedule(3000);
      return;
    }

    this.flushing = false;
    if (this.queue.length > 0) this.#schedule(50);
  }

  /** Cached snapshot: served to SSE clients when Redis is unavailable. */
  cacheSnapshot(topic, payload) {
    this.cache.set(topic, { payload, at: Date.now() });
  }

  getCachedSnapshot(topic) {
    return this.cache.get(topic) ?? null;
  }

  async saveSnapshot(topic, payload, seq) {
    this.cacheSnapshot(topic, payload);
    if (!this.enabled) return;
    try {
      const { error } = await this.client.from('tally_snapshots').upsert(
        {
          topic,
          payload,
          seq,
          updated_at: new Date().toISOString(),
        },
        { onConflict: 'topic' },
      );
      if (error) throw error;
    } catch (error) {
      logger.warn('Supabase snapshot failed', serializeError(error));
    }
  }

  async readLatestSnapshot(topic) {
    if (!this.enabled) return this.getCachedSnapshot(topic);
    try {
      const { data, error } = await this.client
        .from('tally_snapshots')
        .select('payload, seq, updated_at')
        .eq('topic', topic)
        .maybeSingle();
      if (error) throw error;
      if (!data) return this.getCachedSnapshot(topic);
      const entry = { payload: data.payload, seq: data.seq, at: Date.parse(data.updated_at) };
      this.cache.set(topic, entry);
      return entry;
    } catch (error) {
      logger.warn('Supabase snapshot read failed', serializeError(error));
      return this.getCachedSnapshot(topic);
    }
  }
}

export const actaRepository = new ActaRepository();
