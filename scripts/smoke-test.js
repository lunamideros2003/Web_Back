/* eslint-disable no-console */
import http from 'node:http';
import WebSocket from 'ws';
import { buildDataset } from '../src/domain/dataset.js';
import { config } from '../src/config.js';

const BASE = process.env.BASE_URL ?? `http://127.0.0.1:${config.port}`;
const WS_BASE = BASE.replace(/^http/, 'ws');
const dataset = buildDataset(config.election);

let passed = 0;
let failed = 0;

const check = (name, condition, detail) => {
  if (condition) {
    passed += 1;
    console.log(`PASS  ${name}`);
  } else {
    failed += 1;
    console.log(`FAIL  ${name}${detail ? ` :: ${detail}` : ''}`);
  }
};

const json = async (path, options) => {
  const response = await fetch(`${BASE}${path}`, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...(options?.headers ?? {}) },
  });
  const body = await response.json().catch(() => null);
  return { status: response.status, body };
};

const actaFor = (mesa, overrides = {}) => {
  const puesto = dataset.puestoOfMesa.get(mesa);
  const municipio = dataset.municipalityOfMesa.get(mesa);
  return {
    acta: `E14-${String(puesto).padStart(3, '0')}-${String(mesa).padStart(4, '0')}-03`,
    mesa,
    puesto,
    municipio,
    version: 1,
    jurado: 'JUR-SMOKE-1',
    ts_cierre: Math.floor(Date.now() / 1000),
    votos: { C1: 100, C2: 50, C3: 80, C4: 0, C5: 0, C6: 0, BLANCO: 10, NULO: 5, NO_MARCADO: 5 },
    sufragantes: 250,
    hash_foto: 'deadbeefdeadbeef',
    idem: `idem-${mesa}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    ...overrides,
  };
};

const openSse = (topic, lastEventId) =>
  new Promise((resolve, reject) => {
    const url = new URL(`${BASE}/events`);
    url.searchParams.set('topic', topic);
    if (lastEventId) url.searchParams.set('lastEventId', lastEventId);

    const events = [];
    let buffer = '';
    let closed = false;

    const req = http.get(url, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`SSE returned ${res.statusCode}`));
        return;
      }
      const client = {
        events,
        get closed() {
          return closed;
        },
        close() {
          closed = true;
          req.destroy();
        },
        waitFor(predicate, timeoutMs = 8000) {
          return new Promise((resolveWait, rejectWait) => {
            const started = Date.now();
            const timer = setInterval(() => {
              const found = events.find(predicate);
              if (found) {
                clearInterval(timer);
                resolveWait(found);
              } else if (Date.now() - started > timeoutMs) {
                clearInterval(timer);
                rejectWait(new Error(`SSE timeout after ${timeoutMs}ms`));
              }
            }, 25);
            if (timer.unref) timer.unref();
          });
        },
      };

      res.on('data', (chunk) => {
        buffer += chunk.toString('utf8');
        let index = buffer.indexOf('\n\n');
        while (index !== -1) {
          const raw = buffer.slice(0, index);
          buffer = buffer.slice(index + 2);
          let event = 'message';
          let data = null;
          let id = null;
          for (const line of raw.split('\n')) {
            if (line.startsWith('event:')) event = line.slice(6).trim();
            else if (line.startsWith('data:')) data = line.slice(5).trim();
            else if (line.startsWith('id:')) id = line.slice(3).trim();
          }
          if (data) {
            try {
              events.push({ event, id, data: JSON.parse(data) });
            } catch {
              events.push({ event, id, data });
            }
          }
          index = buffer.indexOf('\n\n');
        }
      });

      resolve(client);
    });
    req.on('error', (error) => {
      if (!closed) reject(error);
    });
  });

const openSocket = (path) =>
  new Promise((resolve, reject) => {
    const ws = new WebSocket(`${WS_BASE}${path}`);
    const messages = [];
    const waiters = [];

    const api = {
      messages,
      send(payload) {
        ws.send(JSON.stringify(payload));
      },
      close() {
        ws.close();
      },
      waitFor(predicate, timeoutMs = 8000) {
        return new Promise((resolveWait, rejectWait) => {
          const found = messages.find(predicate);
          if (found) {
            resolveWait(found);
            return;
          }
          const timer = setTimeout(() => {
            const index = waiters.indexOf(entry);
            if (index >= 0) waiters.splice(index, 1);
            rejectWait(new Error(`WS timeout after ${timeoutMs}ms`));
          }, timeoutMs);
          const entry = { predicate, resolve: resolveWait, timer };
          waiters.push(entry);
        });
      },
    };

    ws.on('message', (raw) => {
      let message;
      try {
        message = JSON.parse(raw.toString());
      } catch {
        return;
      }
      messages.push(message);
      for (const waiter of [...waiters]) {
        if (waiter.predicate(message)) {
          clearTimeout(waiter.timer);
          waiters.splice(waiters.indexOf(waiter), 1);
          waiter.resolve(message);
        }
      }
    });
    ws.on('open', () => resolve(api));
    ws.on('error', reject);
  });

const sumVotes = (tally) => Object.values(tally ?? {}).reduce((sum, value) => sum + Number(value), 0);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const run = async () => {
  const health = await json('/health');
  check('health responds', health.status === 200 && health.body?.status === 'ok', JSON.stringify(health.body));
  const instanceId = health.body?.instanceId;

  const mesa = 100;
  const acta = actaFor(mesa);
  const topic = `puesto:${acta.puesto}`;

  const first = await json('/api/actas', { method: 'POST', body: JSON.stringify(acta) });
  check('acta accepted', first.status === 200 && first.body?.status === 'accepted', JSON.stringify(first.body));
  check('ack carries instance id', Boolean(first.body?.instanceId));

  const afterFirst = await json(`/api/results?topic=${topic}`);
  check('puesto tally applied', Number(afterFirst.body?.tally?.C1) === 100, JSON.stringify(afterFirst.body?.tally));
  check('puesto reported count is 1', Number(afterFirst.body?.reported?.mesas) === 1);

  const duplicate = await json('/api/actas', { method: 'POST', body: JSON.stringify(acta) });
  check('same idem is a duplicate', duplicate.body?.status === 'duplicate' && duplicate.body?.applied === false, JSON.stringify(duplicate.body));

  const sameVersion = await json('/api/actas', {
    method: 'POST',
    body: JSON.stringify({ ...acta, idem: `${acta.idem}-other` }),
  });
  check('same version with new idem is superseded', sameVersion.body?.status === 'superseded', JSON.stringify(sameVersion.body));

  const duplicateTally = await json(`/api/results?topic=${topic}`);
  check('duplicate did not double count', Number(duplicateTally.body?.tally?.C1) === 100, JSON.stringify(duplicateTally.body?.tally));

  const corrected = await json('/api/actas', {
    method: 'POST',
    body: JSON.stringify({
      ...acta,
      version: 2,
      votos: { C1: 90, C2: 60, C3: 80, C4: 0, C5: 0, C6: 0, BLANCO: 10, NULO: 5, NO_MARCADO: 5 },
      idem: `${acta.idem}-v2`,
    }),
  });
  check('correction accepted', corrected.body?.status === 'accepted' && corrected.body?.corrected === true, JSON.stringify(corrected.body));

  const afterCorrection = await json(`/api/results?topic=${topic}`);
  check('correction reverted previous version', Number(afterCorrection.body?.tally?.C1) === 90, JSON.stringify(afterCorrection.body?.tally));
  check('correction applied new value', Number(afterCorrection.body?.tally?.C2) === 60, JSON.stringify(afterCorrection.body?.tally));
  check('reported count unchanged by correction', Number(afterCorrection.body?.reported?.mesas) === 1);

  await json('/api/actas', {
    method: 'POST',
    body: JSON.stringify({
      ...acta,
      version: 3,
      votos: { C1: 95, C2: 55, C3: 80, C4: 0, C5: 0, C6: 0, BLANCO: 10, NULO: 5, NO_MARCADO: 5 },
      idem: `${acta.idem}-v3`,
    }),
  });
  const late = await json('/api/actas', {
    method: 'POST',
    body: JSON.stringify({
      ...acta,
      version: 2,
      votos: { C1: 90, C2: 60, C3: 80, C4: 0, C5: 0, C6: 0, BLANCO: 10, NULO: 5, NO_MARCADO: 5 },
      idem: `${acta.idem}-late-v2`,
    }),
  });
  check('late version is superseded', late.body?.status === 'superseded', JSON.stringify(late.body));

  const afterLate = await json(`/api/results?topic=${topic}`);
  check('late version did not move totals', Number(afterLate.body?.tally?.C1) === 95, JSON.stringify(afterLate.body?.tally));

  const doubtful = actaFor(200, {
    jurado: 'JUR-SMOKE-2',
    sufragantes: 260,
    idem: `doubtful-${Date.now()}`,
  });
  const doubtfulAck = await json('/api/actas', { method: 'POST', body: JSON.stringify(doubtful) });
  check('doubtful acta accepted and flagged', doubtfulAck.body?.doubtful === true && doubtfulAck.body?.queuedForVerification === true, JSON.stringify(doubtfulAck.body));

  const state = await json('/api/verification/state');
  const queued = state.body?.cases?.find((item) => item.actaId === doubtful.acta && item.status === 'pending');
  check('doubtful acta reached the verification queue', Boolean(queued), JSON.stringify(state.body?.counts));

  const claim1 = await json('/api/verification/claim', {
    method: 'POST',
    body: JSON.stringify({ auditorId: 'AUD-1', actaId: doubtful.acta }),
  });
  check('auditor claims the case', claim1.body?.ok === true && Boolean(claim1.body?.token), JSON.stringify(claim1.body));

  const claim2 = await json('/api/verification/claim', {
    method: 'POST',
    body: JSON.stringify({ auditorId: 'AUD-2', actaId: doubtful.acta }),
  });
  check('second auditor is rejected', claim2.body?.ok === false && claim2.body?.reason === 'already_claimed', JSON.stringify(claim2.body));

  const staleRelease = await json('/api/verification/release', {
    method: 'POST',
    body: JSON.stringify({ auditorId: 'AUD-1', actaId: doubtful.acta, token: 'stale-token', requeue: true }),
  });
  check('release with a stale token is refused', staleRelease.status === 409 && staleRelease.body?.reason === 'not_owner', JSON.stringify(staleRelease.body));

  const release = await json('/api/verification/release', {
    method: 'POST',
    body: JSON.stringify({ auditorId: 'AUD-1', actaId: doubtful.acta, token: claim1.body.token, requeue: true }),
  });
  check('owner releases the case back to the queue', release.body?.ok === true && release.body?.requeued === true, JSON.stringify(release.body));

  const claim3 = await json('/api/verification/claim', {
    method: 'POST',
    body: JSON.stringify({ auditorId: 'AUD-2', actaId: doubtful.acta }),
  });
  check('released case can be claimed again', claim3.body?.ok === true, JSON.stringify(claim3.body));

  const complete = await json('/api/verification/release', {
    method: 'POST',
    body: JSON.stringify({
      auditorId: 'AUD-2',
      actaId: doubtful.acta,
      token: claim3.body.token,
      requeue: false,
      resolution: 'reviewed_by_smoke_test',
    }),
  });
  check('owner completes the case', complete.body?.ok === true && complete.body?.requeued === false, JSON.stringify(complete.body));

  // ----- SSE: snapshot, live update and replay after a disconnect -----
  const publicMesa = 6400;
  const publicActa = actaFor(publicMesa, {
    jurado: 'JUR-SMOKE-3',
    votos: { C1: 10, C2: 10, C3: 10, C4: 0, C5: 0, C6: 0, BLANCO: 0, NULO: 0, NO_MARCADO: 0 },
    sufragantes: 30,
    idem: `sse-${Date.now()}`,
  });
  const publicTopic = `mun:${publicActa.municipio}`;

  const sse = await openSse(publicTopic);
  const snapshot = await sse.waitFor((event) => event.event === 'snapshot');
  check('SSE delivers an initial snapshot', Boolean(snapshot?.data?.tally), snapshot?.event);

  await json('/api/actas', { method: 'POST', body: JSON.stringify(publicActa) });
  const update = await sse.waitFor(
    (event) => event.event === 'update' && sumVotes(event.data.tally) >= 30,
    10_000,
  );
  check('SSE receives an aggregated update', Boolean(update?.data?.seq), JSON.stringify(update?.data?.changes));

  const lastId = update.id;
  const beforeDisconnect = await json(`/api/results?topic=${publicTopic}`);
  sse.close();

  await sleep(300);
  const missed = actaFor(6399, {
    jurado: 'JUR-SMOKE-4',
    votos: { C1: 7, C2: 7, C3: 7, C4: 0, C5: 0, C6: 0, BLANCO: 0, NULO: 0, NO_MARCADO: 0 },
    sufragantes: 21,
    idem: `sse-missed-${Date.now()}`,
  });
  await json('/api/actas', { method: 'POST', body: JSON.stringify(missed) });
  await sleep(3000);

  const resumed = await openSse(publicTopic, lastId);
  const ready = await resumed.waitFor((event) => event.event === 'ready', 10_000);
  check('SSE resumes from Last-Event-ID', ready?.data?.resumed === true, JSON.stringify(ready?.data));
  resumed.close();

  const afterReconnect = await json(`/api/results?topic=${publicTopic}`);
  check(
    'resumed client sees the same accumulated state',
    sumVotes(afterReconnect.body?.tally) > sumVotes(beforeDisconnect.body?.tally),
    `${sumVotes(beforeDisconnect.body?.tally)} -> ${sumVotes(afterReconnect.body?.tally)}`,
  );

  // ----- Jury WebSocket -----
  const jury = await openSocket(`/ws/jury?token=${config.tokens.jury}&jurorId=JUR-WS-1`);
  const hello = await jury.waitFor((message) => message.type === 'hello');
  check('jury socket gets an instance hello', Boolean(hello?.instanceId));

  const wsActa = actaFor(300, { jurado: 'JUR-WS-1', idem: `ws-${Date.now()}` });
  jury.send({ type: 'acta.submit', requestId: 'req-1', acta: wsActa });
  const ack = await jury.waitFor((message) => message.type === 'ack' && message.requestId === 'req-1');
  check('jury socket receives an explicit ack', ack?.status === 'accepted' && ack?.applied === true, JSON.stringify(ack));

  jury.send({ type: 'acta.submit', requestId: 'req-2', acta: wsActa });
  const duplicateAck = await jury.waitFor((message) => message.type === 'ack' && message.requestId === 'req-2');
  check('jury socket duplicate ack is explicit', duplicateAck?.status === 'duplicate', JSON.stringify(duplicateAck));
  jury.close();

  // ----- Verification WebSocket -----
  const verifierA = await openSocket(
    `/ws/verification?token=${config.tokens.auditor}&auditorId=AUD-WS-1`,
  );
  const initA = await verifierA.waitFor((message) => message.type === 'init');
  check('verification console gets queue and roster', Array.isArray(initA?.cases) && Array.isArray(initA?.roster), JSON.stringify(initA?.counts));

  const verifierB = await openSocket(
    `/ws/verification?token=${config.tokens.auditor}&auditorId=AUD-WS-2`,
  );
  await verifierB.waitFor((message) => message.type === 'init');

  const caseActa = actaFor(400, {
    jurado: 'JUR-SMOKE-5',
    sufragantes: 300,
    idem: `ws-case-${Date.now()}`,
  });
  await json('/api/actas', { method: 'POST', body: JSON.stringify(caseActa) });

  verifierA.send({ type: 'case.claim', actaId: caseActa.acta });
  const claimAck = await verifierA.waitFor((message) => message.type === 'claim.result');
  check('auditor claims over WebSocket', claimAck?.ok === true, JSON.stringify(claimAck));

  verifierB.send({ type: 'case.claim', actaId: caseActa.acta });
  const claimDenied = await verifierB.waitFor(
    (message) => message.type === 'claim.result' && message.actaId === caseActa.acta,
  );
  check('other instance refuses the same case', claimDenied?.ok === false, JSON.stringify(claimDenied));

  verifierA.send({ type: 'case.complete', actaId: caseActa.acta, token: claimAck.token, resolution: 'checked' });
  const completeAck = await verifierA.waitFor((message) => message.type === 'complete.result');
  check('auditor completes over WebSocket', completeAck?.ok === true, JSON.stringify(completeAck));

  verifierA.close();
  verifierB.close();

  console.log(`\n${passed} passed, ${failed} failed (instance ${instanceId})`);
  if (failed > 0) process.exitCode = 1;
};

run().catch((error) => {
  console.error('SMOKE TEST ERROR', error);
  process.exitCode = 1;
});
