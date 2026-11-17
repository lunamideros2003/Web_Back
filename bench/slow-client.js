/* eslint-disable no-console */
import http from 'node:http';
import { buildDataset } from '../src/domain/dataset.js';
import { generateActa } from '../src/domain/generator.js';

/**
 * Backpressure demonstration (RF-9).
 *
 * Run the backend with small thresholds so the policy is observable in seconds:
 *
 *   PUBLISH_WINDOW_MS=250 FANOUT_SLOW_BYTES=4096 FANOUT_CLOSE_BYTES=16384 \
 *   PORT=3000 node src/index.js
 *
 * Then:
 *   node bench/slow-client.js --clients 20 --seconds 20
 */

const arg = (name, fallback) => {
  const index = process.argv.indexOf(`--${name}`);
  if (index === -1) return fallback;
  const value = Number(process.argv[index + 1]);
  return Number.isFinite(value) ? value : fallback;
};

const TARGET = String(process.env.BASE_URL ?? 'http://127.0.0.1:3000').replace(/\/$/, '');
const CLIENTS = arg('clients', 20);
const SECONDS = arg('seconds', 20);
const ACTA_MS = arg('acta-ms', 150);
const dataset = buildDataset({ mesas: 6400, puestos: 1842, municipios: 11 });

const slowClients = [];

const connectSlow = () =>
  new Promise((resolve) => {
    const request = http.get(`${TARGET}/events?topic=dept`, { agent: false }, (response) => {
      if (response.statusCode !== 200) {
        response.resume();
        resolve(null);
        return;
      }
      // Never read from the socket: the TCP window closes and the server buffer
      // starts to grow. This is the deliberately slow consumer.
      response.pause();
      let closed = false;
      response.on('close', () => {
        closed = true;
      });
      response.on('error', () => {});
      resolve({ request, response, get closed() { return closed; } });
    });
    request.setTimeout(0);
    request.on('error', () => resolve(null));
  });

let mesaCursor = 5000;
const submitActa = async () => {
  mesaCursor = mesaCursor >= dataset.totals.mesas ? 1 : mesaCursor + 1;
  const acta = generateActa({
    mesa: mesaCursor,
    puesto: dataset.puestoOfMesa.get(mesaCursor),
    municipio: dataset.municipalityOfMesa.get(mesaCursor),
    jurorId: 'JUR-SLOW-DEMO',
  });
  await fetch(`${TARGET}/api/actas`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(acta),
  }).catch(() => {});
};

const metrics = async () => {
  try {
    const body = await fetch(`${TARGET}/metrics`).then((response) => response.json());
    return body;
  } catch {
    return null;
  }
};

const run = async () => {
  console.log(`[slow] opening ${CLIENTS} non-reading SSE clients against ${TARGET}`);
  for (let index = 0; index < CLIENTS; index += 1) {
    // eslint-disable-next-line no-await-in-loop
    const client = await connectSlow();
    if (client) slowClients.push(client);
  }
  console.log(`[slow] ${slowClients.length} connected; generating updates`);

  const started = Date.now();
  const timer = setInterval(() => {
    void submitActa();
  }, ACTA_MS);

  while (Date.now() - started < SECONDS * 1000) {
    // eslint-disable-next-line no-await-in-loop
    await new Promise((resolve) => setTimeout(resolve, 2000));
    // eslint-disable-next-line no-await-in-loop
    const snapshot = await metrics();
    const closed = slowClients.filter((client) => client.closed).length;
    const buffered = slowClients.map((client) => client.response.socket?.writableLength ?? 0);
    console.log(
      `[slow] t=${Math.round((Date.now() - started) / 1000)}s ` +
        `skipped=${snapshot?.totals?.sseEventsSkipped ?? '?'} ` +
        `closedByServer=${snapshot?.totals?.slowClientsClosed ?? '?'} ` +
        `clientClosed=${closed}/${slowClients.length} ` +
        `heap=${snapshot?.memory?.heapUsedMb ?? '?'}MB serverBuffered=${buffered.length ? Math.max(...buffered) : 0}B`,
    );
  }

  clearInterval(timer);
  for (const client of slowClients) client.request.destroy();

  const final = await metrics();
  console.log('\n=== Backpressure summary ===');
  console.log(`events skipped while lagging : ${final?.totals?.sseEventsSkipped ?? 'n/a'}`);
  console.log(`clients closed by the server : ${final?.totals?.slowClientsClosed ?? 'n/a'}`);
  console.log(`heap after the test          : ${final?.memory?.heapUsedMb ?? 'n/a'} MB`);
  console.log('Policy: stop sending deltas, mark the client, send a snapshot when it catches up,');
  console.log('        close it with a continuity hint if the buffer crosses the hard limit.');
};

run().catch((error) => {
  console.error('[slow] failed', error);
  process.exit(1);
});
