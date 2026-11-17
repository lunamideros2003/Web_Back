import { randomUUID } from 'node:crypto';
import cors from 'cors';
import express from 'express';
import { config } from '../config.js';
import { buildDataset } from '../domain/dataset.js';
import { CANDIDATES, VOTE_CODES, parseTopic } from '../domain/election.js';
import { generateActa } from '../domain/generator.js';
import { keys } from '../infra/keys.js';
import { redis, redisGateway } from '../infra/redis.js';
import { actaRepository } from '../infra/supabaseRepository.js';
import { logger } from '../lib/logger.js';
import { metrics } from '../metrics.js';
import { getActa, submitActa } from '../services/actaService.js';
import { getClusterInstances } from '../services/clusterView.js';
import { historyService } from '../services/historyService.js';
import { readTopicState } from '../services/tallyService.js';
import {
  claimCase,
  getAuditorStats,
  getCoordinatorSnapshot,
  releaseCase,
} from '../services/verificationService.js';
import { createSseRouter } from '../realtime/sse.js';
import { getLocalSnapshot } from '../jobs/telemetry.js';

const dataset = buildDataset(config.election);

const httpStatusFor = (status) => {
  switch (status) {
    case 'accepted':
    case 'duplicate':
    case 'superseded':
      return 200;
    case 'conflict':
      return 409;
    case 'rejected':
      return 422;
    case 'rate_limited':
      return 429;
    case 'unavailable':
      return 503;
    default:
      return 500;
  }
};

const percentile = (values, p) => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return Math.round(sorted[index] * 100) / 100;
};

const readInpSamples = async () => {
  try {
    const raw = await redisGateway.run(() => redis.lrange(keys.stats('inp'), 0, 199));
    return raw.map(Number).filter((value) => Number.isFinite(value));
  } catch {
    return [];
  }
};

const requireAdmin = (req, res, next) => {
  if (!config.demo.enabled) {
    res.status(404).json({ error: 'Demo endpoints are disabled' });
    return;
  }
  if (config.tokens.admin && req.get('x-admin-token') !== config.tokens.admin) {
    res.status(401).json({ error: 'Missing or invalid x-admin-token' });
    return;
  }
  next();
};

/**
 * Operational metrics are public in the demo (the ops dashboard is one of the
 * deliverables). In a real election set METRICS_PUBLIC=false and read them with
 * the admin token.
 */
const requireMetrics = (req, res, next) => {
  if (config.tokens.metricsPublic) {
    next();
    return;
  }
  if (!config.tokens.admin || req.get('x-admin-token') !== config.tokens.admin) {
    res.status(401).json({ error: 'Missing or invalid x-admin-token' });
    return;
  }
  next();
};

export const createApp = () => {
  const app = express();
  app.set('trust proxy', true);
  app.disable('x-powered-by');
  app.use(
    cors({
      origin: config.corsOrigins.includes('*') ? '*' : config.corsOrigins,
      methods: ['GET', 'POST', 'OPTIONS'],
      allowedHeaders: ['Content-Type', 'x-admin-token', 'Last-Event-ID'],
      exposedHeaders: ['Retry-After'],
    }),
  );
  app.use(express.json({ limit: '256kb' }));

  app.get('/', (req, res) => {
    res.json({
      service: 'preconteo-realtime',
      instanceId: config.instanceId,
      docs: '/api/config',
      health: '/health',
      publicStream: '/events?topic=dept',
      jurySocket: '/ws/jury',
      verificationSocket: '/ws/verification',
    });
  });

  app.get('/health', (req, res) => {
    res.json({
      status: 'ok',
      instanceId: config.instanceId,
      uptimeSeconds: Math.round(process.uptime()),
      redis: redisGateway.status,
      connections: metrics.snapshot().connections,
      timestamp: new Date().toISOString(),
    });
  });

  app.get('/ready', (req, res) => {
    const ready = redisGateway.isReady();
    res.status(ready ? 200 : 503).json({ ready, instanceId: config.instanceId, redis: redisGateway.status });
  });

  app.get('/api/config', (req, res) => {
    res.json({
      instanceId: config.instanceId,
      environment: config.env,
      election: dataset.totals,
      candidates: CANDIDATES,
      voteCodes: VOTE_CODES,
      windowMs: config.window.publishMs,
      sseRetryMs: config.sse.retryMs,
      verification: {
        leaseMs: config.locks.leaseMs,
        heartbeatMs: config.presence.heartbeatMs,
      },
      jury: { heartbeatMs: config.jury.heartbeatMs },
      features: {
        supabase: config.supabase.enabled,
        demoEndpoints: config.demo.enabled,
      },
    });
  });

  app.get('/api/topics', (req, res) => {
    res.json({
      department: { topic: 'dept', mesas: dataset.totals.mesas, puestos: dataset.totals.puestos },
      municipalities: Array.from({ length: dataset.totals.municipios }, (_, index) => {
        const id = index + 1;
        return {
          id,
          label: `Municipality ${id}`,
          topic: `mun:${id}`,
          mesas: dataset.mesasPerMunicipio[index],
          puestos: dataset.puestosPerMunicipio[index],
        };
      }),
      puestos: { from: 1, to: dataset.totals.puestos },
    });
  });

  app.get('/api/results', async (req, res) => {
    const rawTopic = typeof req.query.topic === 'string' ? req.query.topic : 'dept';
    let topic;
    try {
      topic = parseTopic(rawTopic);
    } catch (error) {
      res.status(400).json({ error: error.message });
      return;
    }

    try {
      const [state, seq] = await Promise.all([
        readTopicState(topic),
        historyService.latestId(topic.key),
      ]);
      res.json({ ...state, seq, stale: false, instanceId: config.instanceId });
    } catch (error) {
      const cached = actaRepository.getCachedSnapshot(topic.key);
      if (cached) {
        res.json({
          ...cached.payload,
          seq: cached.seq ?? null,
          stale: true,
          reason: 'redis_unavailable',
          instanceId: config.instanceId,
        });
        return;
      }
      res.status(503).json({
        error: 'Results are temporarily unavailable',
        reason: 'redis_unavailable',
        instanceId: config.instanceId,
        detail: error?.message,
      });
    }
  });

  app.post('/api/actas', async (req, res) => {
    const result = await submitActa({
      payload: req.body,
      instanceId: config.instanceId,
      source: 'http',
    });
    if (result.retryAfterMs) {
      res.set('Retry-After', String(Math.ceil(result.retryAfterMs / 1000)));
    }
    res.status(httpStatusFor(result.status)).json(result);
  });

  app.get('/api/actas/:actaId', async (req, res) => {
    try {
      const acta = await getActa(req.params.actaId);
      if (!acta) {
        res.status(404).json({ error: 'Acta not found' });
        return;
      }
      res.json(acta);
    } catch (error) {
      res.status(503).json({ error: 'Registry unavailable', detail: error?.message });
    }
  });

  app.get(['/api/verification/state', '/api/verification/queue'], async (req, res) => {
    try {
      const snapshot = await getCoordinatorSnapshot();
      const auditorId = typeof req.query.auditorId === 'string' ? req.query.auditorId : null;
      const stats = auditorId ? await getAuditorStats(auditorId.toUpperCase()) : null;
      res.json({
        ...snapshot,
        stats,
        leaseMs: config.locks.leaseMs,
        heartbeatMs: config.presence.heartbeatMs,
        instanceId: config.instanceId,
        stale: false,
      });
    } catch (error) {
      res.status(503).json({
        error: 'Verification service temporarily unavailable',
        reason: 'redis_unavailable',
        instanceId: config.instanceId,
        detail: error?.message,
      });
    }
  });

  app.post('/api/verification/claim', async (req, res) => {
    const { auditorId, actaId } = req.body ?? {};
    if (!auditorId || !actaId) {
      res.status(400).json({ error: 'auditorId and actaId are required' });
      return;
    }
    try {
      const result = await claimCase({ auditorId: String(auditorId).toUpperCase(), actaId });
      res.status(result.ok ? 200 : 409).json(result);
    } catch (error) {
      res.status(503).json({ error: 'Verification service unavailable', detail: error?.message });
    }
  });

  app.post('/api/verification/release', async (req, res) => {
    const { auditorId, actaId, token, resolution, requeue } = req.body ?? {};
    if (!auditorId || !actaId || !token) {
      res.status(400).json({ error: 'auditorId, actaId and token are required' });
      return;
    }
    try {
      const result = await releaseCase({
        auditorId: String(auditorId).toUpperCase(),
        actaId,
        token,
        requeue: requeue !== false,
        resolution: resolution ?? 'http_release',
      });
      res.status(result.ok ? 200 : 409).json(result);
    } catch (error) {
      res.status(503).json({ error: 'Verification service unavailable', detail: error?.message });
    }
  });

  app.get('/metrics', requireMetrics, (req, res) => {
    res.json({
      ...metrics.snapshot(),
      redis: { status: redisGateway.status, circuitOpen: redisGateway.isCircuitOpen() },
      supabase: actaRepository.status,
    });
  });

  app.get('/metrics/prometheus', requireMetrics, (req, res) => {
    res.type('text/plain; version=0.0.4').send(metrics.toPrometheus());
  });

  app.get('/api/ops/metrics', requireMetrics, async (req, res) => {
    let instances;
    let source = 'redis';
    try {
      instances = await getClusterInstances({ cacheMs: 0 });
    } catch {
      instances = [];
      source = 'local';
    }
    if (instances.length === 0) {
      instances = [getLocalSnapshot()];
      if (source === 'redis') source = 'local';
    }

    let global = { published: 0, naive: 0 };
    try {
      const raw = await redisGateway.run(() =>
        redis.mget(keys.stats('messages:published'), keys.stats('messages:naive')),
      );
      global = { published: Number(raw?.[0] ?? 0), naive: Number(raw?.[1] ?? 0) };
    } catch {
      // Fall back to summing the instance snapshots below.
      global = {
        published: instances.reduce((sum, item) => sum + (item.totals?.messagesPublished ?? 0), 0),
        naive: instances.reduce((sum, item) => sum + (item.totals?.naiveMessages ?? 0), 0),
      };
    }

    const inpSamples = await readInpSamples();
    const sum = (selector) => instances.reduce((total, item) => total + (selector(item) ?? 0), 0);

    // Two different comparisons, both honest:
    //  * client deliveries: what each viewer actually received, versus the
    //    naive one-message-per-acta-per-viewer fan-out;
    //  * backplane traffic: topic messages published to Redis, versus eight
    //    instances publishing every acta to every other instance.
    const deliveredEvents = sum((item) => item.totals?.sseEventsSent);
    const actasAccepted = sum((item) => item.totals?.actasAccepted);
    const naiveBackplane = actasAccepted * 8;

    res.json({
      source,
      generatedAt: new Date().toISOString(),
      instances,
      global: {
        ...global,
        deliveredEvents,
        naiveDeliveries: global.naive,
        deliveryReductionPct:
          global.naive > 0 ? Math.round((1 - deliveredEvents / global.naive) * 10000) / 100 : 0,
        backplane: {
          published: global.published,
          naive: naiveBackplane,
          reductionPct:
            naiveBackplane > 0
              ? Math.round((1 - global.published / naiveBackplane) * 10000) / 100
              : 0,
        },
      },
      aggregate: {
        instances: instances.length,
        sseConnections: sum((item) => item.connections?.sse),
        juryConnections: sum((item) => item.connections?.jury),
        verificationConnections: sum((item) => item.connections?.verification),
        totalConnections: sum((item) => item.connections?.total),
        actasPerMinute: sum((item) => item.rates?.actasPerMinute),
        messagesPerSecond: sum((item) => item.rates?.messagesPerSecond),
        actasAccepted,
        actasDoubtful: sum((item) => item.totals?.actasDoubtful),
        deliveredEvents,
        heapUsedMb: Math.round(sum((item) => item.memory?.heapUsedMb) * 10) / 10,
        eventLoopP99Ms: Math.max(0, ...instances.map((item) => item.eventLoop?.p99Ms ?? 0)),
        eventLoopMaxMs: Math.max(0, ...instances.map((item) => item.eventLoop?.maxMs ?? 0)),
        degradedInstances: instances.filter((item) => item.redisStatus !== 'ready').length,
      },
      inp: {
        samples: inpSamples.length,
        p75: percentile(inpSamples, 75),
        p95: percentile(inpSamples, 95),
        worst: inpSamples.length ? Math.max(...inpSamples) : 0,
      },
      supabase: actaRepository.status,
    });
  });

  app.post('/api/telemetry/inp', async (req, res) => {
    const value = Number(req.body?.value);
    if (!Number.isFinite(value) || value < 0 || value > 60_000) {
      res.status(400).json({ error: 'value must be an INP measurement in milliseconds' });
      return;
    }
    try {
      const payload = JSON.stringify({
        value,
        rating: req.body?.rating ?? null,
        page: String(req.body?.page ?? 'public').slice(0, 40),
        at: Date.now(),
      });
      await redisGateway.run(() => {
        const pipeline = redis.pipeline();
        pipeline.lpush(keys.stats('inp'), payload);
        pipeline.ltrim(keys.stats('inp'), 0, 199);
        pipeline.expire(keys.stats('inp'), 86_400);
        return pipeline.exec();
      });
      res.status(202).json({ accepted: true });
    } catch {
      res.status(202).json({ accepted: false, reason: 'redis_unavailable' });
    }
  });

  app.post('/api/demo/registry-outage', requireAdmin, async (req, res) => {
    const enabled = req.body?.enabled !== false;
    await redisGateway.simulateOutage(enabled);
    logger.warn('Demo: registry outage toggled', { enabled });
    res.json({ ok: true, redis: redisGateway.status, instanceId: config.instanceId });
  });

  app.post('/api/demo/acta', requireAdmin, async (req, res) => {
    const requested =
      Number.isInteger(req.body?.mesa) && req.body.mesa > 0 && req.body.mesa <= dataset.totals.mesas
        ? req.body.mesa
        : 1 + Math.floor(Math.random() * dataset.totals.mesas);
    const puesto = dataset.puestoOfMesa.get(requested) ?? 1;
    const municipio = dataset.municipalityOfMesa.get(requested) ?? 1;
    const acta = generateActa({
      mesa: requested,
      puesto,
      municipio,
      version: Number.isInteger(req.body?.version) ? req.body.version : 1,
      doubtful: req.body?.doubtful === true,
    });
    const result = await submitActa({ payload: acta, source: 'demo' });
    res.status(httpStatusFor(result.status)).json({ acta, result });
  });

  app.post('/api/demo/reset', requireAdmin, async (req, res) => {
    let cursor = '0';
    let deleted = 0;
    try {
      do {
        const [next, batch] = await redisGateway.run(() =>
          redis.scan(cursor, 'MATCH', `${config.keyPrefix}:*`, 'COUNT', 500),
        );
        cursor = next;
        if (batch.length > 0) {
          // eslint-disable-next-line no-await-in-loop
          await redisGateway.run(() => redis.del(...batch));
          deleted += batch.length;
        }
      } while (cursor !== '0');
      res.json({ ok: true, deletedKeys: deleted });
    } catch (error) {
      res.status(503).json({ error: 'Redis unavailable', detail: error?.message });
    }
  });

  app.use(createSseRouter());

  app.use((req, res) => {
    res.status(404).json({ error: 'Not found', path: req.path });
  });

  // eslint-disable-next-line no-unused-vars
  app.use((error, req, res, next) => {
    logger.error('HTTP request failed', {
      path: req.path,
      method: req.method,
      message: error?.message,
      requestId: randomUUID(),
    });
    if (res.headersSent) return;
    const status = error?.statusCode ?? (error?.type === 'entity.parse.failed' ? 400 : 500);
    res.status(status).json({ error: error?.message ?? 'Internal error' });
  });

  return app;
};
