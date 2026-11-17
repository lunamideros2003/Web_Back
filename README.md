# Preconteo realtime backend

Realtime election preconteo (informational count) for **6,400 polling tables**, **120 verification auditors** and up to **400,000 public viewers**, running as **eight stateless Node.js instances behind a load balancer** with **Redis as the only coordination layer**.

Everything is in English, as requested: code, comments, logs, docs and UI.

---

## 1. What this service does

| Audience | Transport | Guarantee |
| --- | --- | --- |
| Jurors / data entry (6,400, flaky mobile) | **WebSocket** `/ws/jury` | An acta is counted **once**, the juror always gets an explicit verdict (`accepted`, `duplicate`, `superseded`, `rejected`, `rate_limited`, `unavailable`) |
| Verification consoles (120, all day) | **WebSocket** `/ws/verification` | The same acta is **never** assigned to two auditors; a crashed auditor releases the case automatically |
| Public (up to 400,000) | **Server-Sent Events** `/events?topic=…` | The number never moves backwards silently; a reconnecting client ends up **exactly** where a client that never left would be |

HTTP endpoints exist as well (acta fallback, reads, verification REST, ops), so **three transports coexist**: WebSocket, SSE and plain HTTP.

The dispatch of one acta is a single atomic Redis Lua script that updates **four hierarchical levels** (mesa → puesto → municipio → department), the reported-table counters and the candidate rankings, plus the idempotency key and the verification queue.

---

## 2. Requirements

- Node.js **20 or newer** (tested on Node 26).
- Redis **7+** (tested on Redis 8.10 on Windows). No Docker required.
- A Supabase project (optional, only for the durable audit trail).

---

## 3. Run locally

```bash
cd Backend
cp .env.example .env          # Windows: copy .env.example .env
npm install
npm run seed                  # writes the 6,400/1,842/11 reference dataset to Redis
npm start                     # http://localhost:3000
```

Redis defaults to `redis://127.0.0.1:6379`. Anything can be overridden with `.env`.

Quick checks:

```bash
curl http://localhost:3000/health
curl http://localhost:3000/api/results?topic=dept
curl -N http://localhost:3000/events?topic=dept        # keep it open: SSE stream
```

### Eight instances + load balancer (no sticky sessions)

```bash
npm run dev:cluster                 # 8 children on :3101…:3108, round-robin proxy on :8080
BASE_URL=http://127.0.0.1:8080 npm run smoke
```

Every request/upgrade is balanced independently, so two sockets from the same browser land on different instances. Nothing breaks, because all shared state is in Redis.

---

## 4. Endpoints

### Public read side

| Method | Path | Notes |
| --- | --- | --- |
| `GET` | `/events?topic=dept\|mun:<id>\|puesto:<id>` | SSE. `Last-Event-ID` header or `?lastEventId=` resumes exactly |
| `GET` | `/api/results?topic=…` | Current aggregated state + last stream id |
| `GET` | `/api/topics` | Topic catalogue with the tables per municipality |
| `GET` | `/api/config` | Window size, heartbeat, tokens policy, features |
| `POST` | `/api/telemetry/inp` | INP sample reported by the public page (RT-9) |

### Jury write side

| Method | Path | Notes |
| --- | --- | --- |
| `WS` | `/ws/jury?token=<JURY_TOKEN>&jurorId=<id>` | `{"type":"acta.submit","requestId":"…","acta":{…}}` → `{"type":"ack",…}` |
| `POST` | `/api/actas` | HTTP fallback with the same semantics |
| `GET` | `/api/actas/:actaId` | Stored version and votes |

### Verification

| Method | Path | Notes |
| --- | --- | --- |
| `WS` | `/ws/verification?token=<AUDITOR_TOKEN>&auditorId=<id>` | `queue.refresh`, `case.claim`, `case.renew`, `case.complete`, `case.release`, `heartbeat` |
| `GET` | `/api/verification/state` | Queue, active cases, roster, counts |
| `POST` | `/api/verification/claim` | REST fallback |
| `POST` | `/api/verification/release` | `requeue: true\|false` |

### Operations

| Method | Path | Notes |
| --- | --- | --- |
| `GET` | `/health`, `/ready` | Liveness / readiness (readiness fails when Redis is down) |
| `GET` | `/metrics` | Local instance snapshot (JSON) |
| `GET` | `/metrics/prometheus` | Same data in Prometheus text format |
| `GET` | `/api/ops/metrics` | **Cluster** view aggregated from Redis + INP percentiles |
| `POST` | `/api/demo/acta` | Generates and submits a random acta (demo) |
| `POST` | `/api/demo/registry-outage` | `{"enabled":true}` disconnects Redis to demo degraded mode |
| `POST` | `/api/demo/reset` | Wipes the `preconteo:*` namespace |

When `ADMIN_TOKEN` is set, the `/api/demo/*` endpoints require `x-admin-token`.
When `METRICS_PUBLIC=false`, `/metrics`, `/metrics/prometheus` and `/api/ops/metrics` require `x-admin-token`.

---

## 5. Configuration

Everything is documented in [`.env.example`](./.env.example). The values that matter most:

| Variable | Default | Meaning |
| --- | --- | --- |
| `REDIS_URL` | `redis://127.0.0.1:6379` | The backplane. Injected by the Render Blueprint |
| `SUPABASE_URL` / `SUPABASE_SERVICE_ROLE_KEY` | empty | Enables the durable audit; empty = audit disabled, everything still works |
| `CORS_ORIGIN` | `*` | Comma separated list; set the Vercel domain in production |
| `JURY_TOKEN` / `AUDITOR_TOKEN` / `ADMIN_TOKEN` | demo values | Shared secrets for the sockets / admin endpoints |
| `KEY_PREFIX` | `preconteo` | Redis namespace |
| `PUBLISH_WINDOW_MS` | `2000` | Aggregation window: one message per topic per window |
| `ELECTION_MESAS` / `_PUESTOS` / `_MUNICIPIOS` | `6400` / `1842` / `11` | Reference dataset |
| `JURY_ACTAS_PER_MINUTE` | `240` | Per-juror rate limit |
| `LOCK_LEASE_MS` | `60000` | Verification lease; renewed by heartbeats |

---

## 6. Scripts

| Command | What it does |
| --- | --- |
| `npm run seed` | Rebuilds the reference dataset in Redis (6,400 tables, 1,842 stations, 11 municipalities) |
| `npm run dev` | Single instance with `--watch` |
| `npm run dev:cluster` | 8 instances + round-robin proxy on :8080 |
| `npm run smoke` | 33 end-to-end assertions against one instance or the balancer |
| `npm test` | 42 unit tests (validation, dataset, topics, stream ids, deltas, metrics, keys) |
| `npm run bench:load` | 20,000-connection SSE generator with the election day profile |
| `npm run bench:reconnect` | Reconnection avalanche: naive vs exponential backoff with full jitter |
| `npm run bench:slow` | Deliberately slow consumers to demonstrate the backpressure policy |

### Load test

```bash
# always reseed first: the generator corrects actas it has already sent
npm run seed

# through the balancer (double socket cost: client + proxy)
BASE_URL=http://127.0.0.1:8080 npm run bench:load -- --sse 3000 --duration 30 --actas-per-minute 300

# straight to the eight instances, 20,000 connections
npm run bench:load -- \
  --targets http://127.0.0.1:3101,http://127.0.0.1:3102,http://127.0.0.1:3103,http://127.0.0.1:3104,http://127.0.0.1:3105,http://127.0.0.1:3106,http://127.0.0.1:3107,http://127.0.0.1:3108 \
  --target http://127.0.0.1:3101 \
  --sse 20000 --duration 120 --actas-per-minute 300 --profile peak
```

On Linux, raise the client limits before a 20k run:

```bash
ulimit -n 1000000
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
```

### Reconnection storm

```bash
BASE_URL=http://127.0.0.1:8080 npm run bench:reconnect -- --clients 600 --observe 10 --mode both
```

### Backpressure

```bash
PUBLISH_WINDOW_MS=100 SSE_DEMO_PADDING_BYTES=6000 \
FANOUT_SLOW_BYTES=8192 FANOUT_CLOSE_BYTES=16384 \
PORT=3000 node src/index.js

npm run bench:slow -- --clients 20 --seconds 16 --acta-ms 30
```

Measured on this machine: **900 events skipped** while the clients lagged with the heap bounded at ~30 MB, and **20/20 clients closed** once the buffer crossed the hard limit. See [docs/REPORT.md](./docs/REPORT.md).

---

## 7. Project layout

```
Backend
├── src
│   ├── config.js              every knob, parsed and validated
│   ├── index.js               bootstrap and graceful shutdown
│   ├── domain                 election model, validation, dataset, generator, tally math
│   ├── infra
│   │   ├── keys.js            the complete Redis key layout
│   │   ├── redis.js           one gateway: 3 connections, circuit breaker, scripts
│   │   ├── lua                atomic acta, locks, leader election, rate limit
│   │   └── supabaseRepository write-behind audit + snapshot fallback
│   ├── services               acta, tally, history, verification, presence, window publisher
│   ├── realtime               SSE, jury gateway, verification gateway, topic hub
│   ├── jobs                   telemetry and maintenance sweeps
│   └── http/app.js            every HTTP route
├── scripts                    seed, cluster, smoke test
├── bench                      load test, reconnect storm, slow client
├── supabase/schema.sql        tables, indexes, RLS policies
├── test                       42 unit tests
└── docs/REPORT.md             the written answer: requirements, Redis roles, measurements
```

---

## 8. Deployment

- **Render**: Blueprint in [`../render.yaml`](../render.yaml) — one managed Key Value service + `numInstances: 8`. On the first boot, if Redis has no reference totals, one instance writes the 6,400 / 1,842 / 11 dataset. It does not wipe actas that are already stored. `npm run seed` is the local reset.
- **Vercel**: frontend in [`../Fronend`](../Fronend). Set the project root to `Fronend` and `VITE_API_URL` to the Render URL.
- **Supabase**: run [`supabase/schema.sql`](./supabase/schema.sql) once in the SQL editor.

The exact values to type in each panel are in [docs/REPORT.md § 12](./docs/REPORT.md#12-deployment-checklist).
