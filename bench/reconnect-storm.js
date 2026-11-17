/* eslint-disable no-console */
import { mkdirSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Reconnection avalanche experiment (RF-10).
 *
 * It opens N public connections, kills them at the same instant (simulating an
 * instance death) and measures reconnections per second with:
 *
 *   naive  -> every client retries immediately
 *   jitter -> exponential backoff with full jitter
 *
 * Run with the backend behind the local balancer:
 *   node scripts/cluster.js
 *   node bench/reconnect-storm.js --clients 2000 --mode both
 */

const arg = (name, fallback) => {
  const index = process.argv.indexOf(`--${name}`);
  if (index === -1) return fallback;
  const value = Number(process.argv[index + 1]);
  return Number.isFinite(value) ? value : fallback;
};

const TARGET = String(process.env.BASE_URL ?? 'http://127.0.0.1:8080').replace(/\/$/, '');
const CLIENTS = arg('clients', 2000);
const modeIndex = process.argv.indexOf('--mode');
const MODE = modeIndex === -1 ? 'both' : String(process.argv[modeIndex + 1]);
const RAMP_PER_SECOND = arg('ramp', 1000);
const OBSERVE_SECONDS = arg('observe', 12);
const TOPIC = 'dept';
// First retry after a failure is spread over 3s instead of hammering at once.
const JITTER_BASE_MS = arg('jitter-base-ms', 3000);
const JITTER_CAP_MS = arg('jitter-cap-ms', 30_000);

const agent = new http.Agent({ keepAlive: false, maxSockets: Infinity });

const connect = () =>
  new Promise((resolve) => {
    const request = http.get(`${TARGET}/events?topic=${TOPIC}`, { agent }, (response) => {
      if (response.statusCode !== 200) {
        response.resume();
        resolve({ ok: false, status: response.statusCode, request });
        return;
      }
      response.on('data', () => {});
      response.on('error', () => {});
      resolve({ ok: true, request, response });
    });
    request.setTimeout(0);
    request.on('error', () => resolve({ ok: false, request }));
  });

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const bar = (value, max, width = 44) => '#'.repeat(Math.max(0, Math.round((value / Math.max(1, max)) * width)));

const runPhase = async (mode) => {
  console.log(`\n[storm] mode=${mode}: opening ${CLIENTS} connections`);
  const connections = [];
  for (let offset = 0; offset < CLIENTS; offset += RAMP_PER_SECOND) {
    const size = Math.min(RAMP_PER_SECOND, CLIENTS - offset);
    // eslint-disable-next-line no-await-in-loop
    const batch = await Promise.all(Array.from({ length: size }, connect));
    connections.push(...batch.filter((item) => item.ok));
  }
  console.log(`[storm] ${connections.length}/${CLIENTS} connected; cutting them now`);
  await wait(500);

  for (const connection of connections) {
    connection.request.destroy();
  }

  const startedAt = Date.now();
  const perSecond = new Array(OBSERVE_SECONDS).fill(0);
  const per100ms = new Array(Math.min(50, OBSERVE_SECONDS * 10)).fill(0);
  let successes = 0;
  let failures = 0;
  let attempts = 0;

  const retry = (attempt) => {
    if (Date.now() - startedAt > OBSERVE_SECONDS * 1000) return;
    attempts += 1;

    let delay = 0;
    if (mode === 'jitter') {
      const ceiling = Math.min(JITTER_CAP_MS, JITTER_BASE_MS * 2 ** Math.min(attempt, 4));
      delay = Math.random() * ceiling;
    } else if (attempt > 0) {
      delay = 200;
    }

    setTimeout(() => {
      connect().then((result) => {
        if (result.ok) {
          successes += 1;
          const elapsed = Date.now() - startedAt;
          const second = Math.min(OBSERVE_SECONDS - 1, Math.floor(elapsed / 1000));
          const bucket = Math.min(per100ms.length - 1, Math.floor(elapsed / 100));
          perSecond[second] += 1;
          per100ms[bucket] += 1;
          result.request.destroy();
        } else {
          failures += 1;
          retry(attempt + 1);
        }
      });
    }, delay);
  };

  // Naive: everyone hammers the balancer in the same tick.
  // Jitter: retries spread over the backoff window.
  for (let index = 0; index < connections.length; index += 1) {
    retry(0);
  }

  await wait(OBSERVE_SECONDS * 1000);
  await wait(500);

  const cumulative = perSecond.reduce((sum, value) => sum + value, 0);
  let settledSecond = perSecond.length;
  let running = 0;
  for (let index = 0; index < perSecond.length; index += 1) {
    running += perSecond[index];
    if (running >= connections.length * 0.95) {
      settledSecond = index + 1;
      break;
    }
  }

  return {
    mode,
    clients: connections.length,
    successes,
    failures,
    attempts,
    perSecond,
    per100ms,
    peakPerSecond: Math.max(...perSecond),
    peakPer100ms: Math.max(...per100ms),
    settledSecond,
    appliedRate: cumulative / Math.max(1, OBSERVE_SECONDS),
  };
};

const run = async () => {
  const health = await fetch(`${TARGET}/health`).then((response) => response.json());
  console.log(`[storm] target ${TARGET} (instance ${health.instanceId})`);

  const modes = MODE === 'both' ? ['naive', 'jitter'] : [MODE];
  const results = [];
  for (const mode of modes) {
    // eslint-disable-next-line no-await-in-loop
    results.push(await runPhase(mode));
    // eslint-disable-next-line no-await-in-loop
    await wait(1000);
  }

  console.log('\n=== Reconnections per 100ms (avalanche shape) ===');
  const window = Math.min(30, Math.max(...results.map((result) => result.per100ms.length)));
  for (let bucket = 0; bucket < window; bucket += 1) {
    const row = results.map((result) => String(result.per100ms[bucket] ?? 0).padStart(5)).join('  ');
    console.log(`${String(bucket * 100).padStart(5)}ms ${row}`);
  }

  console.log('\n=== Reconnections per second ===');
  for (let second = 0; second < OBSERVE_SECONDS; second += 1) {
    const row = results.map((result) => String(result.perSecond[second] ?? 0).padStart(5)).join('  ');
    console.log(`${String(second).padStart(3)}s ${row}`);
  }
  console.log(`\n${' '.repeat(5)}  ${results.map((result) => result.mode.padStart(5)).join('  ')}`);

  for (const result of results) {
    console.log(
      `\n${result.mode}: peak ${result.peakPerSecond}/s (${result.peakPer100ms}/100ms), ` +
        `95% reconnected after ${result.settledSecond}s, failures ${result.failures}/${result.attempts} attempts`,
    );
  }

  if (results.length === 2) {
    const [naive, jitter] = results;
    const improvement =
      naive.peakPer100ms > 0
        ? Math.round((1 - jitter.peakPer100ms / naive.peakPer100ms) * 100)
        : 0;
    console.log(`\nInstantaneous peak reduction with full jitter: ${improvement}%`);
  }

  const here = dirname(fileURLToPath(import.meta.url));
  const outDir = join(here, 'results');
  mkdirSync(outDir, { recursive: true });
  const file = join(outDir, `reconnect-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
  writeFileSync(file, JSON.stringify({ target: TARGET, clients: CLIENTS, results }, null, 2));
  console.log(`[storm] raw results written to ${file}`);
};

run().catch((error) => {
  console.error('[storm] failed', error);
  process.exit(1);
});
