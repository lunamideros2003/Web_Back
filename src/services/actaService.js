import { config } from '../config.js';
import { CANDIDATES, VOTE_CODES } from '../domain/election.js';
import { casePriority, doubtReason, validateActa } from '../domain/validation.js';
import { actaScriptKeys, keys } from '../infra/keys.js';
import { redis, redisGateway, RedisUnavailableError } from '../infra/redis.js';
import { actaRepository } from '../infra/supabaseRepository.js';
import { logger } from '../lib/logger.js';
import { metrics } from '../metrics.js';
import { enforceRateLimit } from './rateLimiter.js';

const topicsFor = (acta) => [
  'dept',
  `mun:${acta.municipio}`,
  `puesto:${acta.puesto}`,
];

const markWindow = async (topics, receivedAt) => {
  const pipeline = redis.pipeline();
  for (const topic of topics) {
    pipeline.set(keys.windowFirst(topic), String(receivedAt), 'PX', 15_000, 'NX');
    pipeline.incr(keys.windowActas(topic));
    pipeline.pexpire(keys.windowActas(topic), 15_000);
  }
  await redisGateway.run(() => pipeline.exec());
};

const publishQueueSignal = async (actaId) => {
  try {
    await redisGateway.publish(
      keys.verificationChannel(),
      JSON.stringify({ type: 'queue.updated', actaId, at: Date.now() }),
    );
  } catch {
    // The verification channel is best effort; the queue itself lives in Redis.
  }
};

const ack = (base) => ({ ...base, instanceId: config.instanceId });

/**
 * Single entry point for acta submission, shared by the jury WebSocket, the
 * HTTP fallback and the replay tooling. It never reports success unless Redis
 * committed the acta.
 */
export const submitActa = async ({ payload, instanceId = config.instanceId, source = 'http' }) => {
  const receivedAt = Date.now();
  const validation = validateActa(payload);

  if (!validation.ok) {
    metrics.inc('actasRejected');
    return ack({
      ok: false,
      status: 'rejected',
      retryable: false,
      errors: validation.errors,
      message: 'The acta is structurally invalid and was not recorded.',
    });
  }

  const acta = validation.acta;
  metrics.inc('actasReceived');

  try {
    const rate = await enforceRateLimit({
      scope: 'acta',
      subject: acta.jurado,
      limit: config.jury.actasPerMinute,
      windowMs: 60_000,
    });

    if (!rate.allowed) {
      metrics.inc('actasRejected');
      return ack({
        ok: false,
        status: 'rate_limited',
        retryable: true,
        retryAfterMs: rate.retryAfterMs,
        message: `Rate limit exceeded (${rate.limit} actas/minute). Retry later.`,
      });
    }

    const topics = topicsFor(acta);
    const suspicious = validation.doubtful;
    const reason = suspicious ? doubtReason(acta, validation.totalVotes) : '';
    const scriptKeys = actaScriptKeys({
      actaId: acta.acta,
      mesa: acta.mesa,
      puesto: acta.puesto,
      municipio: acta.municipio,
      idem: acta.idem,
    });

    const raw = await redisGateway.run(() =>
      redis.applyActa(
        ...scriptKeys,
        acta.acta,
        String(acta.version),
        String(acta.mesa),
        String(acta.puesto),
        String(acta.municipio),
        JSON.stringify(acta.votos),
        String(config.jury.idempotencyTtlSeconds),
        JSON.stringify(VOTE_CODES),
        JSON.stringify(CANDIDATES),
        JSON.stringify(topics),
        String(receivedAt),
        suspicious ? '1' : '0',
        String(casePriority(acta, validation.totalVotes)),
        acta.jurado,
        acta.hash_foto ?? '',
        reason,
        String(acta.sufragantes),
        String(validation.totalVotes),
      ),
    );

    const result = JSON.parse(raw);
    const durationMs = Date.now() - receivedAt;

    if (result.status === 'conflict') {
      metrics.inc('actasRejected');
      return ack({
        ok: false,
        status: 'conflict',
        retryable: false,
        actaId: acta.acta,
        message: 'This idempotency key already belongs to a different acta.',
      });
    }

    if (result.status === 'duplicate') {
      metrics.inc('actasDuplicate');
      return ack({
        ok: true,
        status: 'duplicate',
        applied: false,
        actaId: acta.acta,
        version: acta.version,
        registeredVersion: result.version,
        doubtful: suspicious,
        totalVotes: validation.totalVotes,
        sufragantes: acta.sufragantes,
        latencyMs: durationMs,
        message: `Acta already registered at version ${result.version}. Votes were not counted again.`,
      });
    }

    if (result.status === 'stale') {
      metrics.inc('actasStale');
      return ack({
        ok: true,
        status: 'superseded',
        applied: false,
        actaId: acta.acta,
        version: acta.version,
        registeredVersion: result.version,
        doubtful: suspicious,
        totalVotes: validation.totalVotes,
        sufragantes: acta.sufragantes,
        latencyMs: durationMs,
        message: `Version ${acta.version} was superseded by version ${result.version}; totals did not move backwards.`,
      });
    }

    metrics.inc('actasAccepted');
    if (result.corrected) metrics.inc('actasCorrected');
    if (suspicious) metrics.inc('actasDoubtful');

    await markWindow(topics, receivedAt).catch(() => {});

    actaRepository.enqueueActa({
      acta,
      totalVotes: validation.totalVotes,
      doubtful: suspicious,
      instanceId,
    });

    if (suspicious || result.corrected) {
      publishQueueSignal(acta.acta);
    }

    logger.debug('Acta applied', {
      actaId: acta.acta,
      version: acta.version,
      corrected: result.corrected,
      doubtful: suspicious,
      source,
      durationMs,
    });

    return ack({
      ok: true,
      status: 'accepted',
      applied: true,
      actaId: acta.acta,
      version: acta.version,
      registeredVersion: acta.version,
      reported: result.reported,
      corrected: result.corrected,
      doubtful: suspicious,
      totalVotes: validation.totalVotes,
      sufragantes: acta.sufragantes,
      reason: suspicious ? reason : undefined,
      queuedForVerification: suspicious,
      latencyMs: durationMs,
      message: suspicious
        ? 'Acta recorded, but the accounting rule does not match; a human auditor will review it.'
        : 'Acta recorded with idempotency guarantee.',
    });
  } catch (error) {
    if (error instanceof RedisUnavailableError) {
      metrics.inc('actasRejected');
      return ack({
        ok: false,
        status: 'unavailable',
        retryable: true,
        retryAfterMs: 5000,
        actaId: acta.acta,
        message: 'The registry is temporarily unavailable. The acta was NOT stored; retry with the same idempotency key.',
      });
    }
    throw error;
  }
};

export const getActa = async (actaId) => {
  const raw = await redisGateway.run(() => redis.hgetall(keys.acta(actaId)));
  if (!raw || Object.keys(raw).length === 0) return null;
  return {
    acta: actaId,
    version: Number(raw.version),
    mesa: Number(raw.mesa),
    puesto: Number(raw.puesto),
    municipio: Number(raw.municipio),
    sufragantes: Number(raw.sufragantes ?? 0),
    votes: Object.fromEntries(VOTE_CODES.map((code) => [code, Number(raw[code] ?? 0)])),
    updatedAt: Number(raw.updated_at ?? 0),
  };
};
