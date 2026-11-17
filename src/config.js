import 'dotenv/config';

const asInt = (name, fallback) => {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number.parseInt(raw, 10);
  if (Number.isNaN(value)) throw new Error(`Environment variable ${name} must be an integer`);
  return value;
};

const asBool = (name, fallback) => {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(raw.toLowerCase());
};

const asList = (name, fallback = []) => {
  const raw = process.env[name];
  if (!raw) return fallback;
  return raw.split(',').map((item) => item.trim()).filter(Boolean);
};

const isProduction = process.env.NODE_ENV === 'production';

export const config = {
  env: process.env.NODE_ENV ?? 'development',
  isProduction,
  instanceId:
    process.env.INSTANCE_ID ??
    process.env.RENDER_INSTANCE_ID ??
    `${process.env.HOSTNAME ?? 'node'}-${process.pid}`,
  port: asInt('PORT', 3000),
  host: '0.0.0.0',
  corsOrigins: asList('CORS_ORIGIN', ['*']),
  keyPrefix: process.env.KEY_PREFIX ?? 'preconteo',
  tokens: {
    jury: process.env.JURY_TOKEN ?? 'demo-jury-token',
    auditor: process.env.AUDITOR_TOKEN ?? 'demo-auditor-token',
    admin: process.env.ADMIN_TOKEN ?? '',
    // Demo deployments expose read-only operational metrics without a token.
    metricsPublic: asBool('METRICS_PUBLIC', true),
  },
  redis: {
    url: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379',
    tls: asBool('REDIS_TLS', process.env.REDIS_URL?.startsWith('rediss://') ?? false),
    connectTimeoutMs: asInt('REDIS_CONNECT_TIMEOUT_MS', 5000),
    commandTimeoutMs: asInt('REDIS_COMMAND_TIMEOUT_MS', 2000),
    failureThreshold: asInt('REDIS_FAILURE_THRESHOLD', 3),
    circuitCooldownMs: asInt('REDIS_CIRCUIT_COOLDOWN_MS', 4000),
  },
  supabase: {
    url: process.env.SUPABASE_URL ?? '',
    serviceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY ?? '',
    enabled: Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY),
    writeQueueLimit: asInt('SUPABASE_WRITE_QUEUE_LIMIT', 2000),
    snapshotIntervalMs: asInt('SUPABASE_SNAPSHOT_INTERVAL_MS', 30_000),
  },
  election: {
    mesas: asInt('ELECTION_MESAS', 6400),
    puestos: asInt('ELECTION_PUESTOS', 1842),
    municipios: asInt('ELECTION_MUNICIPIOS', 11),
  },
  window: {
    // One aggregated broadcast every 2s instead of one per acta per client.
    publishMs: asInt('PUBLISH_WINDOW_MS', 2000),
    maxTopicsPerTick: asInt('PUBLISH_MAX_TOPICS', 2000),
    leaderTtlMs: asInt('PUBLISH_LEADER_TTL_MS', 3000),
  },
  history: {
    // ~200 revisions x 2s = ~6.6 minutes of replay per topic.
    maxEntries: asInt('HISTORY_MAX_ENTRIES', 200),
    ttlSeconds: asInt('HISTORY_TTL_SECONDS', 7200),
    replayMax: asInt('HISTORY_REPLAY_MAX', 400),
  },
  fanout: {
    batchSize: asInt('FANOUT_BATCH_SIZE', 250),
    slowBytes: asInt('FANOUT_SLOW_BYTES', 256 * 1024),
    closeBytes: asInt('FANOUT_CLOSE_BYTES', 2 * 1024 * 1024),
    resumeBytes: asInt('FANOUT_RESUME_BYTES', 64 * 1024),
  },
  jury: {
    actasPerMinute: asInt('JURY_ACTAS_PER_MINUTE', 240),
    idempotencyTtlSeconds: asInt('IDEMPOTENCY_TTL_SECONDS', 86_400),
    heartbeatMs: asInt('WS_HEARTBEAT_MS', 30_000),
  },
  locks: {
    leaseMs: asInt('LOCK_LEASE_MS', 60_000),
    renewMs: asInt('LOCK_RENEW_MS', 20_000),
  },
  presence: {
    ttlSeconds: asInt('PRESENCE_TTL_SECONDS', 45),
    heartbeatMs: asInt('PRESENCE_HEARTBEAT_MS', 15_000),
    sweepMs: asInt('PRESENCE_SWEEP_MS', 5000),
  },
  metrics: {
    reportMs: asInt('METRICS_REPORT_MS', 5000),
    ttlSeconds: asInt('METRICS_TTL_SECONDS', 30),
  },
  sse: {
    heartbeatMs: asInt('SSE_HEARTBEAT_MS', 15_000),
    retryMs: asInt('SSE_RETRY_MS', 3000),
  },
  rateLimit: {
    connectionsPerMinute: asInt('CONNECTION_RATE_LIMIT_PER_MINUTE', 240),
  },
  demo: {
    // Enables fault-injection endpoints used during the presentation.
    enabled: asBool('ENABLE_DEMO_ENDPOINTS', true),
    // Inflates published payloads so the SSE backpressure policy can be
    // demonstrated in seconds instead of waiting for a real slow link.
    paddingBytes: asInt('SSE_DEMO_PADDING_BYTES', 0),
  },
};

export const isDemo = config.demo.enabled;
