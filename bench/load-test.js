/* eslint-disable no-console */
import { mkdirSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildDataset } from '../src/domain/dataset.js';
import { generateActa } from '../src/domain/generator.js';
import { config } from '../src/config.js';

/**
 * Reproducible load test for RT-8.
 *
 * Example (20.000 public connections while the peak profile runs):
 *   node bench/load-test.js --sse 20000 --duration 300 --actas-per-minute 300 --profile peak
 *
 * To reproduce the real two-hour window at 1x speed:
 *   node bench/load-test.js --sse 20000 --duration 7200 --actas-per-minute 90 --profile peak
 */

const arg = (name, fallback) => {
  const index = process.argv.indexOf(`--${name}`);
  if (index === -1) return fallback;
  const raw = process.argv[index + 1];
  const number = Number(raw);
  return Number.isFinite(number) ? number : raw;
};
const TARGET = String(arg('target', process.env.BASE_URL ?? 'http://127.0.0.1:8080')).replace(/\/$/, '');
const targetsIndex = process.argv.indexOf('--targets');
const TARGETS =
  targetsIndex === -1
    ? [TARGET]
    : String(process.argv[targetsIndex + 1])
        .split(',')
        .map((value) => value.trim().replace(/\/$/, ''))
        .filter(Boolean);
const CONNECTIONS = Number(arg('sse', 20000));
const SOURCE_IPS = String(arg('source-ips', '')).split(',').map((value) => value.trim()).filter(Boolean);
const DURATION = Number(arg('duration', 120));
const ACTAS_PER_MINUTE = Number(arg('actas-per-minute', 300));
const PROFILE = String(arg('profile', 'peak'));
const RAMP_PER_SECOND = Number(arg('ramp-per-second', 2000));
const SAMPLE_MS = Number(arg('sample-ms', 5000));
const SEED = Number(arg('seed', 42));

const dataset = buildDataset(config.election);
const agent = new http.Agent({ keepAlive: false, maxSockets: Infinity });
// Telemetry is read out-of-band on a single reused socket: when the generator
// is close to the local file-descriptor limit, a brand new socket per sample
// would fail even though the server is healthy.
const metricsAgent = new http.Agent({ keepAlive: true, maxSockets: 2 });

const getJson = (url) =>
  new Promise((resolve, reject) => {
    const request = http.get(url, { agent: metricsAgent }, (response) => {
      let raw = '';
      response.setEncoding('utf8');
      response.on('data', (chunk) => {
        raw += chunk;
      });
      response.on('end', () => {
        if (response.statusCode !== 200) {
          reject(new Error(`HTTP ${response.statusCode}`));
          return;
        }
        try {
          resolve(JSON.parse(raw));
        } catch (error) {
          reject(error);
        }
      });
    });
    request.setTimeout(15_000, () => request.destroy(new Error('metrics request timed out')));
    request.on('error', reject);
  });

let randomState = SEED;
const random = () => {
  randomState = (randomState * 1103515245 + 12345) % 2147483648;
  return randomState / 2147483648;
};
const randomInt = (min, max) => Math.floor(random() * (max - min + 1)) + min;
const pick = (items) => items[Math.floor(random() * items.length)];

const connects = [];
const results = {
  startedAt: new Date().toISOString(),
  target: TARGET,
  targets: TARGETS,
  requestedConnections: CONNECTIONS,
  durationSeconds: DURATION,
  profile: PROFILE,
  clients: { opened: 0, failed: 0, closedEarly: 0, eventsReceived: 0, errors: {}, perTarget: {} },
  actas: { submitted: 0, accepted: 0, duplicate: 0, superseded: 0, rejected: 0, doubtful: 0, failed: 0 },
  latencyMs: [],
  samples: [],
};

const topics = [];
for (let index = 0; index < 24; index += 1) {
  topics.push(`mun:${randomInt(1, dataset.totals.municipios)}`);
}
topics.push('dept');

const connectOne = (index) =>
  new Promise((resolve) => {
    const topic = topics[index % topics.length];
    const target = TARGETS[index % TARGETS.length];
    const localAddress = SOURCE_IPS.length > 0 ? SOURCE_IPS[index % SOURCE_IPS.length] : undefined;
    const request = http.get(`${target}/events?topic=${encodeURIComponent(topic)}`, { agent, localAddress }, (response) => {
      if (response.statusCode !== 200) {
        response.resume();
        results.clients.errors[`HTTP_${response.statusCode}`] =
          (results.clients.errors[`HTTP_${response.statusCode}`] ?? 0) + 1;
        resolve({ ok: false, status: response.statusCode, target });
        return;
      }
      const client = { ok: true, request, response, closed: false, events: 0, topic, target };
      response.on('data', (chunk) => {
        const text = chunk.toString('latin1');
        let count = 0;
        let cursor = text.indexOf('event: update');
        while (cursor !== -1) {
          count += 1;
          cursor = text.indexOf('event: update', cursor + 1);
        }
        client.events += count;
        results.clients.eventsReceived += count;
      });
      response.on('error', () => {
        client.closed = true;
      });
      response.on('close', () => {
        client.closed = true;
      });
      resolve(client);
    });
    request.setTimeout(0);
    request.on('error', (error) => {
      const code = error.code ?? 'UNKNOWN';
      results.clients.errors[code] = (results.clients.errors[code] ?? 0) + 1;
      resolve({ ok: false, code, target });
    });
  });

const openConnections = async () => {
  console.log(`[load] opening ${CONNECTIONS} SSE connections against ${TARGETS.join(', ')}`);
  const rampStarted = Date.now();
  for (let offset = 0; offset < CONNECTIONS; offset += RAMP_PER_SECOND) {
    const size = Math.min(RAMP_PER_SECOND, CONNECTIONS - offset);
    const batch = await Promise.all(
      Array.from({ length: size }, (_, index) => connectOne(offset + index)),
    );
    for (const client of batch) {
      if (client.ok) {
        connects.push(client);
        results.clients.opened += 1;
        results.clients.perTarget[client.target] = (results.clients.perTarget[client.target] ?? 0) + 1;
      } else {
        results.clients.failed += 1;
      }
    }
    process.stdout.write(`\r[load] connections: ${results.clients.opened}/${CONNECTIONS}`);
  }
  results.clients.rampSeconds = Math.round((Date.now() - rampStarted) / 1000);
  process.stdout.write('\n');
};

let mesaCursor = 1 + Math.floor(random() * dataset.totals.mesas);
const versionByMesa = new Map();

const nextMesa = () => {
  const mesa = mesaCursor;
  mesaCursor = (mesaCursor % dataset.totals.mesas) + 1;
  const version = (versionByMesa.get(mesa) ?? 0) + 1;
  versionByMesa.set(mesa, version);
  return { mesa, version };
};

const submitActa = async () => {
  let mesa;
  let version;
  let idem = `${Date.now()}-${randomInt(1, 1e9)}`;
  let doubtful = false;

  if (recentActas.length > 0 && random() < 0.08) {
    // 8% of the transmissions are retries of an acta already sent.
    const previous = pick(recentActas);
    mesa = previous.mesa;
    version = previous.version;
    idem = previous.idem;
    doubtful = previous.doubtful;
  } else if (recentActas.length > 0 && random() < 0.02) {
    const previous = pick(recentActas);
    mesa = previous.mesa;
    version = previous.version + 1;
    versionByMesa.set(mesa, version);
    idem = `${previous.idem}-c${version}`;
    doubtful = false;
  } else {
    ({ mesa, version } = nextMesa());
    doubtful = random() < 0.075;
  }

  if (recentActas.length >= 200) recentActas.shift();
  recentActas.push({ mesa, version, idem, doubtful });

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const acta = generateActa({
      mesa,
      puesto: dataset.puestoOfMesa.get(mesa),
      municipio: dataset.municipalityOfMesa.get(mesa),
      version,
      doubtful,
      jurorId: `JUR-${randomInt(1000, 6400)}`,
      at: Math.floor(Date.now() / 1000),
    });
    acta.idem = idem;

    const started = performance.now();
    try {
      const response = await fetch(`${TARGETS[results.actas.submitted % TARGETS.length]}/api/actas`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(acta),
      });
      const body = await response.json().catch(() => null);
      const elapsed = performance.now() - started;
      results.latencyMs.push(elapsed);
      results.actas.submitted += 1;

      if (response.status === 429 || response.status === 503) {
        results.actas.failed += 1;
        return;
      }

      if (body?.status === 'superseded' && attempt < 2) {
        // A previous run already counted this mesa. Resubmit as the next
        // version so the benchmark keeps measuring real corrections instead
        // of draining the remaining time into no-ops.
        results.actas.superseded += 1;
        version = Number(body.registeredVersion ?? version) + 1;
        versionByMesa.set(mesa, version);
        idem = `${idem}-r${version}`;
        continue;
      }

      if (body?.status === 'accepted') results.actas.accepted += 1;
      else if (body?.status === 'duplicate') results.actas.duplicate += 1;
      else if (body?.status === 'superseded') results.actas.superseded += 1;
      else results.actas.rejected += 1;
      if (body?.doubtful) results.actas.doubtful += 1;
      return;
    } catch {
      results.actas.failed += 1;
      return;
    }
  }
};

const recentActas = [];

const dayProfileRate = (elapsedSeconds, totalSeconds) => {
  if (PROFILE !== 'peak') return ACTAS_PER_MINUTE;
  const progress = elapsedSeconds / totalSeconds;
  // 55% of the actas arrive in the first 40% of the window.
  if (progress < 0.4) return (ACTAS_PER_MINUTE * 1.55) / 0.4;
  return (ACTAS_PER_MINUTE * 0.45) / 0.6;
};

const sampleServer = async () => {
  try {
    const body = await getJson(`${TARGET}/api/ops/metrics`);
    results.samples.push({
      at: new Date().toISOString(),
      elapsedSeconds: Math.round((Date.now() - startedAt) / 1000),
      aggregate: body.aggregate,
      global: body.global,
      inp: body.inp,
      perInstance: (body.instances ?? []).map((item) => ({
        instanceId: item.instanceId,
        sse: item.connections?.sse ?? 0,
        p99: item.eventLoop?.p99Ms ?? 0,
        max: item.eventLoop?.maxMs ?? 0,
        heapMb: item.memory?.heapUsedMb ?? 0,
        perConnectionKb: item.memory?.perConnectionKb ?? 0,
        messagesPerSecond: item.rates?.messagesPerSecond ?? 0,
        redisStatus: item.redisStatus,
      })),
    });
  } catch (error) {
    results.samples.push({ at: new Date().toISOString(), elapsedSeconds: Math.round((Date.now() - startedAt) / 1000), error: error.message });
  }
};

const percentile = (values, p) => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return Math.round(sorted[index] * 100) / 100;
};

const bar = (value, max, width = 40) => '#'.repeat(Math.max(0, Math.round((value / Math.max(max, 1)) * width)));

let startedAt = Date.now();

const run = async () => {
  const health = await fetch(`${TARGET}/health`).then((response) => response.json());
  console.log(`[load] backend instance seen by the balancer: ${health.instanceId}`);

  // Warm the telemetry socket before the audience ramp so sampling never
  // competes for an ephemeral port.
  await getJson(`${TARGET}/api/ops/metrics`).catch(() => {});

  await openConnections();
  // The measurement window starts once the audience is connected, so the
  // ramp-up is reported separately instead of polluting the latency sample.
  startedAt = Date.now();
  await sampleServer();

  let submittedTotal = 0;
  const sampling = setInterval(sampleServer, SAMPLE_MS);
  const ticker = setInterval(async () => {
    const elapsed = (Date.now() - startedAt) / 1000;
    const perMinute = dayProfileRate(elapsed, DURATION);
    const target = Math.max(1, Math.round(perMinute / 60));
    const batch = [];
    for (let index = 0; index < target; index += 1) batch.push(submitActa());
    await Promise.all(batch);
    submittedTotal += target;
  }, 1000);

  const progress = setInterval(() => {
    const elapsed = Math.round((Date.now() - startedAt) / 1000);
    process.stdout.write(
      `\r[load] ${elapsed}s/${DURATION}s  actas=${results.actas.submitted}  events=${results.clients.eventsReceived}  open=${results.clients.opened}   `,
    );
  }, 1000);

  await new Promise((resolve) => setTimeout(resolve, DURATION * 1000));

  clearInterval(ticker);
  clearInterval(progress);
  clearInterval(sampling);
  await sampleServer();

  for (const client of connects) {
    if (client.closed) results.clients.closedEarly += 1;
  }
  for (const client of connects) client.request.destroy();

  results.latency = {
    p50: percentile(results.latencyMs, 50),
    p95: percentile(results.latencyMs, 95),
    p99: percentile(results.latencyMs, 99),
  };
  results.finishedAt = new Date().toISOString();
  results.elapsedSeconds = Math.round((Date.now() - startedAt) / 1000);

  const last = results.samples[results.samples.length - 1];
  const maxLoop = Math.max(0, ...results.samples.map((sample) => sample.aggregate?.eventLoopP99Ms ?? 0));

  console.log('\n=== Load test summary ===');
  console.log(`targets                ${TARGETS.join(', ')}`);
  console.log(`duration               ${results.elapsedSeconds}s (${PROFILE} profile, ${results.clients.rampSeconds}s ramp)`);
  console.log(`public connections     ${results.clients.opened} opened, ${results.clients.failed} refused`);
  if (Object.keys(results.clients.perTarget).length > 1) {
    for (const [target, count] of Object.entries(results.clients.perTarget)) {
      console.log(`  ${target.padEnd(30)} ${count} sockets`);
    }
  }
  if (Object.keys(results.clients.errors).length > 0) {
    console.log(`refused by error       ${JSON.stringify(results.clients.errors)}`);
  }
  console.log(`events received        ${results.clients.eventsReceived}`);
  console.log(`actas submitted        ${results.actas.submitted} (accepted ${results.actas.accepted}, duplicate ${results.actas.duplicate}, superseded ${results.actas.superseded}, rejected ${results.actas.rejected})`);
  console.log(`acta latency           p50 ${results.latency.p50}ms  p95 ${results.latency.p95}ms  p99 ${results.latency.p99}ms`);
  console.log(`instances reporting    ${last?.aggregate?.instances ?? 'n/a'}`);
  console.log(`event loop p99 (max)   ${maxLoop}ms`);
  console.log(`topic messages         ${last?.global?.published ?? 'n/a'} published to Redis`);
  console.log(`naive backplane        ${last?.global?.backplane?.naive ?? 'n/a'} (actas x 8 instances)`);
  console.log(`backplane reduction    ${last?.global?.backplane?.reductionPct ?? 'n/a'}%`);
  console.log(`delivered events       ${last?.aggregate?.deliveredEvents ?? 'n/a'} (clients saw these)`);
  console.log(`naive per-acta fanout  ${last?.global?.naiveDeliveries ?? 'n/a'} deliveries`);
  console.log(`delivery reduction     ${last?.global?.deliveryReductionPct ?? 'n/a'}%`);
  console.log(`heap used              ${last?.aggregate?.heapUsedMb ?? 'n/a'}MB`);

  console.log('\nEvent loop p99 per sample (ms)');
  const samples = results.samples.filter((sample) => sample.aggregate);
  const peak = Math.max(1, ...samples.map((sample) => sample.aggregate.eventLoopP99Ms ?? 0));
  for (const sample of samples) {
    const value = sample.aggregate.eventLoopP99Ms ?? 0;
    console.log(`${String(sample.elapsedSeconds).padStart(4)}s ${String(value).padStart(7)} ${bar(value, peak)}`);
  }

  const here = dirname(fileURLToPath(import.meta.url));
  const outDir = join(here, 'results');
  mkdirSync(outDir, { recursive: true });
  const file = join(outDir, `load-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
  writeFileSync(file, JSON.stringify(results, null, 2));
  console.log(`\n[load] raw results written to ${file}`);

  process.exit(0);
};

run().catch((error) => {
  console.error('[load] failed', error);
  process.exit(1);
});
