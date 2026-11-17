import { randomUUID } from 'node:crypto';
import { config } from '../config.js';
import { keys } from '../infra/keys.js';
import { redis, redisGateway } from '../infra/redis.js';
import { logger } from '../lib/logger.js';
import { presenceService } from './presenceService.js';

const QUEUE_SCORE_BASE = 1_000_000_000_000;

const parseVotes = (raw) => {
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
};

const toCase = (actaId, raw, lockTtlMs = -2) => {
  if (!raw || Object.keys(raw).length === 0) return null;
  return {
    actaId,
    version: Number(raw.version ?? 0),
    mesa: Number(raw.mesa ?? 0),
    puesto: Number(raw.puesto ?? 0),
    municipio: Number(raw.municipio ?? 0),
    sufragantes: Number(raw.sufragantes ?? 0),
    totalVotes: Number(raw.totalVotes ?? 0),
    reason: raw.reason ?? '',
    priority: Number(raw.priority ?? 3),
    status: raw.status ?? 'pending',
    votes: parseVotes(raw.votes),
    owner: raw.owner ?? null,
    claimedAt: Number(raw.claimedAt ?? 0),
    updatedAt: Number(raw.updatedAt ?? 0),
    lockExpiresInMs: lockTtlMs > 0 ? lockTtlMs : 0,
  };
};

export const publishVerification = async (event) => {
  try {
    await redisGateway.publish(
      keys.verificationChannel(),
      JSON.stringify({ ...event, instanceId: config.instanceId, at: Date.now() }),
    );
  } catch (error) {
    logger.warn('Verification event not published', { type: event?.type, message: error?.message });
  }
};

export const getQueue = async ({ limit = 120 } = {}) => {
  const pipeline = redis.pipeline();
  pipeline.zrange(keys.queue(), 0, limit - 1, 'WITHSCORES');
  pipeline.zrange(keys.activeCases(), 0, limit - 1);
  pipeline.zcard(keys.queue());
  pipeline.zcard(keys.activeCases());
  const replies = await redisGateway.run(() => pipeline.exec());

  const pendingFlat = replies[0]?.[1] ?? [];
  const pending = [];
  for (let index = 0; index < pendingFlat.length; index += 2) {
    pending.push({ actaId: pendingFlat[index], score: Number(pendingFlat[index + 1]) });
  }
  const active = replies[1]?.[1] ?? [];
  const ids = [...new Set([...pending.map((item) => item.actaId), ...active])];

  if (ids.length === 0) {
    return {
      cases: [],
      counts: { pending: Number(replies[2]?.[1] ?? 0), active: Number(replies[3]?.[1] ?? 0) },
    };
  }

  const detail = redis.pipeline();
  for (const id of ids) {
    detail.hgetall(keys.case(id));
    detail.pttl(keys.lock(id));
  }
  const details = await redisGateway.run(() => detail.exec());

  const caseMap = new Map();
  ids.forEach((id, index) => {
    const raw = details[index * 2]?.[1];
    const ttl = details[index * 2 + 1]?.[1] ?? -2;
    const view = toCase(id, raw, ttl);
    if (view) caseMap.set(id, view);
  });

  const cases = [
    ...pending.map((item) => caseMap.get(item.actaId)).filter(Boolean),
    ...active.map((id) => caseMap.get(id)).filter(Boolean),
  ];

  return {
    cases,
    counts: { pending: Number(replies[2]?.[1] ?? 0), active: Number(replies[3]?.[1] ?? 0) },
  };
};

export const getCoordinatorSnapshot = async () => {
  const [{ cases, counts }, roster] = await Promise.all([getQueue(), presenceService.list()]);
  return {
    cases,
    roster,
    counts: {
      ...counts,
      open: counts.pending + counts.active,
      auditorsOnline: roster.length,
    },
  };
};

export const claimCase = async ({ auditorId, actaId }) => {
  const token = `${auditorId}|${randomUUID()}`;
  const now = Date.now();

  const result = await redisGateway.run(() =>
    redis.acquireLock(
      keys.queue(),
      keys.lock(actaId),
      keys.case(actaId),
      keys.auditorLocks(auditorId),
      keys.lockExpiryIndex(),
      keys.casesIndex(),
      keys.activeCases(),
      actaId,
      token,
      String(config.locks.leaseMs),
      String(now),
      auditorId,
    ),
  );

  if (Number(result) === 0) {
    const owner = await redisGateway.run(() => redis.hget(keys.case(actaId), 'owner'));
    return { ok: false, reason: 'already_claimed', owner: owner ?? null };
  }
  if (Number(result) === -1) {
    return { ok: false, reason: 'unknown_case' };
  }

  await presenceService.setCurrentCase(auditorId, actaId);
  await publishVerification({ type: 'case.claimed', actaId, auditorId, claimedAt: now });

  return { ok: true, token, leaseMs: config.locks.leaseMs, claimedAt: now };
};

/**
 * Renews every lock held by the auditor. Clients call this from their
 * heartbeat, so an auditor who is actively working never loses the case, while
 * a crashed one stops renewing and the lease expires.
 */
export const renewAuditorLocks = async (auditorId) => {
  const held = await redisGateway.run(() => redis.hgetall(keys.auditorLocks(auditorId)));
  const renewed = [];
  const lost = [];
  const now = Date.now();

  for (const [actaId, token] of Object.entries(held ?? {})) {
    // eslint-disable-next-line no-await-in-loop
    const result = await redisGateway.run(() =>
      redis.renewLock(
        keys.lock(actaId),
        keys.lockExpiryIndex(),
        token,
        String(config.locks.leaseMs),
        String(now),
        actaId,
      ),
    );
    if (Number(result) === 1) {
      renewed.push(actaId);
    } else {
      lost.push(actaId);
      // eslint-disable-next-line no-await-in-loop
      await redisGateway.run(() => redis.hdel(keys.auditorLocks(auditorId), actaId));
      // eslint-disable-next-line no-await-in-loop
      await publishVerification({ type: 'case.lost', actaId, auditorId });
    }
  }

  return { renewed, lost, leaseMs: config.locks.leaseMs };
};

export const releaseCase = async ({
  auditorId,
  actaId,
  token,
  requeue = true,
  resolution = '',
}) => {
  if (!token) return { ok: false, reason: 'missing_token' };

  const raw = await redisGateway.run(() => redis.hgetall(keys.case(actaId)));
  const priority = Number(raw?.priority ?? 3);
  const createdAt = Number(raw?.createdAt ?? raw?.updatedAt ?? Date.now());
  const score = priority * QUEUE_SCORE_BASE + createdAt;
  const now = Date.now();

  const result = await redisGateway.run(() =>
    redis.releaseLock(
      keys.lock(actaId),
      keys.auditorLocks(auditorId),
      keys.queue(),
      keys.case(actaId),
      keys.lockExpiryIndex(),
      keys.activeCases(),
      token,
      actaId,
      requeue ? '1' : '0',
      String(score),
      String(now),
      resolution.slice(0, 500),
    ),
  );

  const status = Number(result);
  if (status === 0) {
    // The lease expired and the case belongs to someone else now. The stale
    // owner cannot release or complete it.
    return { ok: false, reason: 'not_owner' };
  }

  await presenceService.setCurrentCase(auditorId, null);

  if (status === 1) {
    await publishVerification({ type: 'case.requeued', actaId, auditorId, reason: 'released_by_auditor' });
    return { ok: true, requeued: true };
  }

  await redisGateway.run(() => redis.hincrby(keys.stats(`auditor:${auditorId}`), 'resolved', 1));
  await publishVerification({ type: 'case.resolved', actaId, auditorId, resolution });
  return { ok: true, requeued: false };
};

export const releaseAllForAuditor = async (auditorId, reason = 'disconnected') => {
  const held = await redisGateway.run(() => redis.hgetall(keys.auditorLocks(auditorId)));
  const released = [];
  for (const [actaId, token] of Object.entries(held ?? {})) {
    // eslint-disable-next-line no-await-in-loop
    const result = await releaseCase({ auditorId, actaId, token, requeue: true, resolution: reason });
    if (result.ok) released.push(actaId);
  }
  if (released.length > 0) {
    await publishVerification({ type: 'auditor.released', auditorId, actaIds: released, reason });
  }
  return released;
};

/** Returns leases that expired without a clean release back to the queue. */
export const sweepExpiredLocks = async () => {
  const now = Date.now();
  const expired = await redisGateway.run(() =>
    redis.zrangebyscore(keys.lockExpiryIndex(), '-inf', now, 'LIMIT', 0, 100),
  );
  if (expired.length === 0) return [];

  const requeued = [];
  for (const actaId of expired) {
    const raw = await redisGateway.run(() => redis.hgetall(keys.case(actaId)));
    const owner = raw?.owner ?? '';
    const priority = Number(raw?.priority ?? 3);
    const createdAt = Number(raw?.createdAt ?? raw?.updatedAt ?? now);
    const score = priority * QUEUE_SCORE_BASE + createdAt;

    // eslint-disable-next-line no-await-in-loop
    const result = await redisGateway.run(() =>
      redis.expireLock(
        keys.lock(actaId),
        keys.queue(),
        keys.case(actaId),
        keys.lockExpiryIndex(),
        keys.activeCases(),
        keys.auditorLocks(owner || 'none'),
        actaId,
        String(score),
        String(now),
      ),
    );

    if (Number(result) === 1) {
      requeued.push(actaId);
      // eslint-disable-next-line no-await-in-loop
      await publishVerification({ type: 'case.expired', actaId, previousOwner: owner || null });
      if (owner) {
        // eslint-disable-next-line no-await-in-loop
        await presenceService.setCurrentCase(owner, null);
      }
    }
  }
  return requeued;
};

export const getAuditorStats = async (auditorId) => {
  const raw = await redisGateway.run(() => redis.hgetall(keys.stats(`auditor:${auditorId}`)));
  return { resolved: Number(raw?.resolved ?? 0) };
};
