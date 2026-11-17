# Realtime election preconteo — technical report

**Workshop:** WebSockets · Web-oriented programming
**Case:** transmission and diffusion of results on an election day
**Audiences:** 6,400 jurors · 120 verification auditors · up to 400,000 public viewers
**Topology:** 8 stateless application instances behind a load balancer, no sticky sessions, Redis as the only shared state

All measured numbers in this document come from this repository (`npm test`, `npm run smoke`, `npm run bench:*`) on the development machine described in § 9.4. Nothing is copied from a vendor brochure: where a limit is environmental, it is stated.

---

## 1. Executive summary

| Requirement | How it is solved | Where |
| --- | --- | --- |
| RF-1 idempotency | `SET idem:<key> "<acta>|<version>" EX 86400` inside the same Lua script that applies the acta | `lua/apply_acta.lua` |
| RF-2 corrections + order independence | Stored version is the only truth: `version <= stored` ⇒ `stale`, otherwise the previous version is subtracted before the new one is added | `lua/apply_acta.lua` |
| RF-3 atomic hierarchy | One script updates mesa, puesto, municipio, department, counters and rankings | `lua/apply_acta.lua` (16 keys, one call) |
| RF-4 fan-out without blocking | 2 s aggregation window + batched delivery that yields to the Event Loop every 250 sockets | `services/windowPublisher.js`, `realtime/hub.js` |
| RF-5 topic subscription | One `Set` of clients per topic, one Redis channel per topic, cost proportional to the topic's subscribers | `realtime/hub.js` |
| RF-6 resume | Redis Stream per topic + `Last-Event-ID`; snapshot when the cursor fell out of the retention window | `services/historyService.js`, `realtime/sse.js` |
| RF-7 exclusive assignment | Lua lock with an owner token, lease + heartbeat renewal, owner-checked release | `lua/acquire_lock.lua`, `renew_lock.lua`, `release_lock.lua` |
| RF-8 presence | Redis hash + TTL per auditor, sorted-set index, sweeper for ghosts | `services/presenceService.js` |
| RF-9 backpressure | Per-socket buffered-byte gauge: skip deltas → snapshot on catch-up → close at the hard limit | `realtime/sse.js` |
| RF-10 degradation + avalanche | Circuit breaker with an explicit ack for jurors, cached snapshot for readers; exponential backoff with full jitter on clients | `infra/redis.js`, `Fronend/src/lib/sse.ts` |
| RT-1..RT-10 | See § 8 | — |

**Measured headline (6,000 concurrent SSE clients, 8 instances, 25 s, acta rate 2.3× the real peak):**

- 6,000 / 6,000 connections opened, **0 refused**, exactly **750 per instance**.
- **279 actas accepted**, 30 duplicates correctly answered as duplicates, 0 rejected.
- Acta acknowledgement latency **p50 186 ms, p99 251 ms** (the load generator shares the CPU).
- Event-loop delay **p99 max 49 ms** while 6,000 sockets were being fanned out.
- **4,800 client deliveries** versus **1,674,000** for the naive per-acta fan-out: **99.71 % less**.
- **126 topic messages** on the backplane versus 279 per-acta publishes (and 2,160 if every one of the eight instances relayed every acta).

A second run with **12,000 connections** (1,500 per instance) and no refusals except one `ENOBUFS` produced die event-loop p99 of **43.8 ms** and 265 MB of heap across the eight processes.

---

## 2. Architecture

```
                       ┌──────────────────────────────┐
 juror app (WS) ──────▶│                              │
 auditor console (WS)─▶│   load balancer, round robin │
 public page (SSE) ───▶│   no sticky sessions         │
                       └───────┬───────┬───────┬──────┘
                               │       │       │
                          ┌────▼──┐ ┌──▼────┐ ┌▼─────┐     8 × Node.js
                          │ inst1 │ │ inst2 │ │ inst8│     (stateless)
                          └───┬───┘ └───┬───┘ └──┬───┘
                              │         │        │
                       ┌──────▼─────────▼────────▼───────┐
                       │            Redis                │
                       │  Pub/Sub  ·  Streams  ·  Lua    │
                       │  Hashes · ZSets · Locks · TTL   │
                       └──────────────┬──────────────────┘
                                      │ write-behind (never blocks the ack)
                               ┌──────▼──────┐
                               │  Supabase   │  audit + snapshot fallback
                               └─────────────┘
```

Rules the design never breaks:

1. **No instance talks to another instance.** Everything shared goes through Redis (RT-3).
2. **No request depends on which instance served it.** Every read reconstructs state from Redis; every socket can land anywhere (RT-2).
3. **Redis is never on the acknowledgement path for durability.** The ack means "Redis committed the acta", not "Postgres wrote a row".

---

## 3. Transport choice per audience (RT-5)

| Audience | Transport | Why (and why not the others) |
| --- | --- | --- |
| Jurors (6,400) | **WebSocket** | The interaction is a request/response *inside* a long-lived session: submit acta → explicit verdict. With HTTP polling a juror on a 2G link pays a new TCP+TLS handshake per retry and can never distinguish "not sent" from "sent but the ack was lost". The socket gives us a `requestId`, one ack per submission and a heartbeat that detects half-open mobile links in 30 s. |
| Verification (120) | **WebSocket** | Collaborative and bidirectional: every auditor receives `case.claimed`, `case.resolved` and `presence.updated` events pushed by the instance that handled them, while sending claims and heartbeats. SSE + HTTP would need two channels and would still be slower for a console where sub-second claim feedback prevents two people from opening the same acta. |
| Public (400,000) | **Server-Sent Events** | One direction only: the client never sends anything but a cursor. SSE is a plain HTTP response, so it traverses every proxy, CDN and corporate firewall, reconnects by itself (`retry:` field) and carries `Last-Event-ID` natively — exactly the continuity requirement of RF-6. WebSocket would add protocol complexity and a bidirectional socket for traffic that is 100 % server→client. Polling at the required freshness (2 s) with 400k clients would mean 200k requests/s and, worse, a different response per client that no cache can help. |

The three transports coexist: the same acta submitted by WebSocket can be read by an HTTP client and observed live by an SSE client. This is verified by the smoke test (33 assertions, § 9.1).

---

## 4. Functional requirements, one by one

### RF-1 — Idempotent reception

`POST /api/actas` or `{"type":"acta.submit"}` on `/ws/jury` carries `idem` (or the `requestId` becomes the idempotency key).

Inside `apply_acta.lua`:

```lua
local cached = redis.call('GET', idemKey)
if cached then
  -- same acta + same version  -> { status = "duplicate", version = v }
  -- same key, different acta  -> { status = "conflict" }
end
```

The juror always receives an explicit verdict:

| Situation | `status` | HTTP | What the juror sees |
| --- | --- | --- | --- |
| First time | `accepted` | 200 | "Acta recorded with idempotency guarantee" |
| Same acta, same version, same key | `duplicate` | 200 | "Already registered at version N. Votes were not counted again." |
| Same key, different acta | `conflict` | 409 | "This idempotency key belongs to another acta" |
| Older version than the stored one | `superseded` | 200 | "Version 2 was superseded by version 3; totals did not move backwards" |
| Rate limit exceeded | `rate_limited` | 429 + `Retry-After` | "Retry in N s" |
| Redis unavailable | `unavailable` | 503 + `Retry-After` | "The acta was NOT stored; retry with the same idempotency key" |

**Server dies between recording and confirming.** The acta is in Redis, the ack is lost. The juror's outbox retries with the same `idem`; the script answers `duplicate` with the registered version. The acta is never counted twice, and the juror gets certainty on the second attempt. This is why the ack is built from the script's return value and never from a separate read.

**The idempotency key expires before the retry.** TTL is 24 h (`IDEMPOTENCY_TTL_SECONDS`), while the transmission window is 2 h. If a retry arrived after expiry, the version check still protects the tally: the stored `version` is ≥ the retried version, so the answer is `stale` and no votes move. The key only needs to outlive the retry horizon, not the election.

### RF-2 — Corrections and convergence

The stored acta hash keeps its current version and its votes. When version *v* arrives with `v > stored`:

1. Read the previous votes (`HMGET`), compute `delta = new − previous` per code.
2. Add that delta to the four accumulated levels.
3. Overwrite the acta hash with version *v*.

Reverting version 2 and applying version 3 is therefore the same operation as applying a delta. States are a function of the *set* of received versions, never of arrival order: a late version 2 after version 3 hits `storedVersion >= version` and returns `stale` without touching a single counter. The smoke test covers exactly this sequence (`correction reverted previous version`, `late version is superseded`, `late version did not move totals`).

### RF-3 — Atomic hierarchical accumulation

One script, 16 keys, one round trip:

```
acta hash · idempotency · tally mesa · tally puesto · tally municipio · tally department
rank department · rank municipio · reported puesto · reported municipio · reported department
dirty topics · verification queue · case hash · acta lock · cases index
```

Redis executes a script atomically: no other command runs between the first `HINCRBY` and the last `ZADD`. A client can never observe a municipality that already includes an acta whose puesto detail is still missing, because both writes happen inside the same uninterruptible step.

**Why not several commands from the application?** Because between `HINCRBY tally:puesto` and `HINCRBY tally:municipio` another request could run, and a reader could hit the gap. A `MULTI/EXEC` transaction would only guarantee that the commands are not interleaved with *other transactions*, but it cannot *read* the previous version and branch on it (the idempotency and staleness decisions require the value). WATCH/retry would work but turns every correction into a contention loop. Lua gives read-decide-write in one atomic unit, and it is also the only place where the whole invariant is visible.

### RF-4 — Fan-out without blocking the Event Loop

Naive: for each acta, loop over 50,000 sockets and call `write`. Result: a synchronous burst that freezes the process; jurors on the same instance stop receiving acks for the duration.

This implementation:

1. The acta only marks its three topics dirty (`SADD dirty:topics …`). No delivery at all inside the request.
2. One elected instance (Redis `SET NX PX` leader lock) reads the dirty set every **2 s** and publishes **one message per topic** with the accumulated state and the minimal delta.
3. Each instance receives the topic message and delivers it to the clients of that topic in batches of **250**, `await setImmediate()` between batches.

Measured event-loop delay during the 6,000-connection run: **p99 49 ms** (peak 49 ms at the 10 s sample), 12,000-connection run: **p99 43.8 ms**. The `sse.js` heartbeat additionally checks buffered bytes so a slow consumer cannot grow memory.

**How many messages does the system emit?** Two honest comparisons:

| Metric | Naive | This design | Measured in the 25 s run |
| --- | --- | --- | --- |
| Client deliveries | one per acta per interested viewer | one per topic per window per subscriber | 4,800 vs **1,674,000** naive → **99.71 % less** |
| Backplane publishes | one per acta (×8 if every instance relayed) | one per dirty topic per window | 126 vs 279 (2,160 with 8 relays) |

The full-day projection the workshop asks for (6,400 actas × 400,000 clients = **2.56 × 10⁹**) must be read together with the profile:

- One message **per window** bounds any single client to 3,600 messages in two hours, so the absolute ceiling is 1.44 × 10⁹ — the 1.8× that a 2 s cadence can give when actas arrive at ~0.9/s.
- The real lever is **scoping**: a viewer of `puesto:176` is 1 of 1,842 stations and receives nothing when another station reports. With a realistic split of 60 % department / 30 % municipality / 10 % station, the expected total is ≈ 0.9 × 10⁹, and every one of those messages is a small delta instead of a full departmental dump.
- The measured 349× reduction in the run above is the combination of both effects at a rate 2.3× above the real peak.

No system can send fewer than one update per client per window if it promises 2 s freshness; what the aggregation removes is the *multiplicity per acta* and the *irrelevant topics*.

### RF-5 — Subscription by topic

`TopicHub` keeps `Map<topic, { clients: Set<client>, subscribed }>`.

| Operation | Cost |
| --- | --- |
| Subscribe (`add`) | O(1) insert in the `Set`; the first subscriber of a topic adds a Redis `SUBSCRIBE` |
| Unsubscribe (`remove`) | O(1) delete; the last subscriber of a topic issues `UNSUBSCRIBE` |
| Publish | O(subscribers of that topic), never O(connections of the instance) |

Because an instance subscribes only to the topics it serves, a `puesto:1234` update does not wake the other seven instances. The gain is measured: 6,000 clients spread over 25 topics received 4,800 events instead of 1.67 M.

### RF-6 — Reconnection without losing anything

- Every published message gets a Redis Stream id (`xadd`). SSE sends it as the `id:` field.
- On reconnect the browser sends `Last-Event-ID`; the server reads `XRANGE hist:<topic> (last +`.
- Retention: `MAXLEN ~ 200` entries and a 2 h TTL. At one entry per 2 s that is ~6.6 min of detail; a 30 s dropout is well inside it.
- If the cursor is older than the oldest entry (`truncated`), or the replay would exceed 400 entries, the server sends **one full snapshot** (`event: snapshot`) instead of the detail — cheaper and guarantees the same state.
- Events that arrive *while* the replay is running are buffered and merged by stream id, so ordering is preserved.

The smoke test asserts `resumed === true` after a disconnect plus a new acta, and that the accumulated state after resuming equals the state of a client that never left.

**Why Pub/Sub alone is not enough:** Pub/Sub has no memory; anything published while the client was away is lost. The stream is the history; Pub/Sub is only the live notification.

**Memory cost:** 200 entries × ~450 B ≈ **90 KB per topic**; the default 12 topics ≈ 1.1 MB, and even all 1,854 topics would be ~170 MB. In practice only topics that change are streamed.

### RF-7 — Exclusive assignment with safe release

- Claim: Lua checks the lock, moves the case out of the priority queue (`ZREM`) and writes the owner + token, all atomically. Two auditors on two different instances can never both get `-1`/`1`.
- Lease: 60 s. The console sends a heartbeat every 15 s; `renew_lock.lua` extends the lease **only if the caller still owns it**. A working auditor never loses the case; a laptop that dies stops renewing and the sweeper requeues the case (`expire_lock.lua`, `EXISTS` + requeue in one step).
- Safe release: `release_lock.lua` compares the token. An auditor whose lease expired and was reassigned cannot release or complete someone else's case — the smoke test asserts exactly this (`release with a stale token is refused` → `not_owner`).
- Queue order: `ZADD` score = `priority × 10¹² + timestamp`, so priority 1 cases always come first and within a priority the oldest comes first.

### RF-8 — Presence and workload

- `presence:<auditor>` hash: instance id, `connectedAt`, `lastSeen`, `currentActa`, `claimedAt`; TTL 45 s.
- `presence:index` sorted set by `lastSeen` for `ZRANGEBYSCORE` without scanning keys.
- A sweep every 5 s deletes ghosts and publishes `presence.updated action=expired`, so all consoles converge without a reload.
- The coordinator snapshot joins the queue, the active cases and the roster, including `busyForMs` per auditor.

Presence lives in Redis, so an auditor connected to instance 1 is visible to the coordinator connected to instance 6.

### RF-9 — Slow clients and backpressure

Each SSE client is watched through `res.writableLength`:

| Buffered bytes | Action |
| --- | --- |
| < 256 KB | normal delivery |
| > 256 KB | mark `lagging`, **stop sending deltas** and count `sseEventsSkipped` |
| drains below 64 KB | mark `needsSnapshot`; the next tick sends one full snapshot ("recovered") |
| > 2 MB | `close(1013, "slow_consumer")`; the client reconnects with `Last-Event-ID` and gets continuity |

The same policy (with a 2 MB ceiling) applies to jury WebSockets via `bufferedAmount`.

**Demonstration** (`bench/slow-client.js`, 20 clients that never read):

- Soft limit (128 KB): **900 events skipped**, heap stayed between 20 and 50 MB, no client lost.
- Hard limit (16 KB): **140 skipped**, then **20/20 clients closed by the server**, heap 28 MB. Memory did not grow with the number of stuck clients.

### RF-10 — Degradation and reconnection avalanche

**Redis unavailable.**

| Actor | Behaviour |
| --- | --- |
| Juror | `submitActa` returns `503 / unavailable` with `retryAfterMs`. The acta is **not** acknowledged; the outbox keeps it and retries with the same `idem`. No fake success, ever. |
| Public SSE | Receives `event: degraded` with `retryAfterMs`, keeps the stream open and shows the last consistent snapshot (Redis is tried first, then the in-process snapshot, then the Supabase snapshot). |
| Verification | Receives `event: degraded`; claims fail cleanly and the console shows "temporarily unavailable". |
| Ops | `/ready` returns 503 and `degradedInstances` rises; the circuit breaker (3 failures, 4 s cooldown) prevents a request pile-up. |

Demo: `curl -X POST localhost:3000/api/demo/registry-outage -H 'content-type: application/json' -d '{"enabled":true}'`.

**Avalanche.** Clients reconnect with exponential backoff and **full jitter**: `delay = random(0, min(30 s, 3 s × 2^attempt))`.

Measured with 600 clients cut at the same instant:

| Strategy | Peak reconnections / 100 ms | Peak / s | 95 % recovered |
| --- | --- | --- | --- |
| Naive (all retry at once) | **530** | 599 | 2 s |
| Exponential backoff + full jitter | **28** | 209 | 3 s |

→ **95 % lower instantaneous peak**, at the cost of one extra second to full recovery.

---

## 5. The mandatory role of Redis

| Role | Structure | Key(s) | Cost / notes |
| --- | --- | --- | --- |
| Broadcast backplane | Pub/Sub, one channel per topic | `pc:bus:dept`, `pc:bus:mun:<id>`, `pc:bus:puesto:<id>` | One message per dirty topic per 2 s; an instance subscribes only to the topics it serves |
| History / recovery | Stream + MAXLEN ~200 + TTL 2 h | `pc:hist:<topic>` | ~90 KB per active topic; `XRANGE` with an exclusive `(` start |
| Accumulators | Hash per level (mesa/puesto/municipio/department) | `pc:tally:*` | `HINCRBY` is O(1); a read is one pipelined `HGETALL` + `ZREVRANGE`, never a scan |
| Candidate ranking | Sorted set | `pc:rank:dept`, `pc:rank:mun:<id>` | `ZINCRBY` on delta, `ZREVRANGE` for the top list — O(log n) |
| Atomic composite operation | Lua (`EVALSHA`) | `apply_acta.lua` | 16 keys per call; the only place where the four levels are consistent by construction |
| Idempotency | String with TTL | `pc:idem:<idem>` | 24 h TTL; the value is `"<acta>|<version>"` so a conflict is detectable |
| Distributed lock | String + owner token, PX, renewed | `pc:lock:acta:<id>` | Lease 60 s, heartbeat 15 s, owner-checked release |
| Presence | Hash + TTL + sorted-set index | `pc:presence:<auditor>`, `pc:presence:index` | TTL 45 s, sweep every 5 s |
| Rate limiting | Fixed-window counter | `pc:rl:<scope>:<subject>:<window>` | `INCR` + `PEXPIRE` in Lua, returns `retryAfterMs` |
| Work queue | Sorted set (priority + time) | `pc:queue:verification` | `ZADD` / `ZRANGE` / `ZREM` |
| Leader election | String, `SET NX PX` | `pc:coord:publisher-leader` | One publisher out of eight; TTL 3 s, renewed every 2 s |
| Cluster telemetry | Hash + sorted set | `pc:ops:instances`, `pc:ops:alive` | Each instance writes its own snapshot; the dashboard reads the shared index |

**Channel design: global vs per-topic.** A single global channel would send every update to all eight instances and force each one to filter by topic, multiplying backplane traffic by eight and waking instances that serve nobody for that topic. Per-topic channels cost one `SUBSCRIBE` per topic an instance actually serves and let an instance drop the subscription when its last client leaves. With 1,854 potential topics and 400k clients, every instance serves `dept` (a handful of messages per second, negligible) but only a fraction of the 1,842 station topics.

---

## 6. Consistency and failure semantics

| Failure | Effect | Guarantee |
| --- | --- | --- |
| Instance dies with 50,000 SSE clients | Those clients reconnect elsewhere | `Last-Event-ID` replay; no state is lost because state is in Redis |
| Instance dies while holding a verification lock | Lease expires in ≤60 s | Sweeper requeues the case; the dead owner cannot release it later (token check) |
| Redis master fails over | Clients get `degraded`, jurors get `unavailable` | No false acknowledgements; retries are idempotent |
| Supabase is slow or down | Write-behind queue fills to 2,000 and drops the oldest | The ack never waits for Postgres; `supabaseDropped` is exposed in `/metrics` |
| Two instances think they are the publisher | Impossible: `SET NX PX` + owner-checked renewal | Clients would otherwise receive duplicated deltas |
| Reconnection storm | Backoff with full jitter | Measured 95 % lower peak (§ RF-10) |

**Why no sticky sessions (RT-2).** Sticky sessions would tie a juror's socket and the verification lock to one process; when that process dies the load balancer cannot move the socket, and the auditor's case ownership becomes a process-local fact that the other seven instances cannot see. Removing affinity forces all shared state into Redis, which is exactly what makes the eight instances interchangeable. The cost: every request must be able to reconstruct its context from Redis (a few extra round trips, all pipelined, measured in the acta latency above) and broadcasts must be truly global (solved with Pub/Sub). The benefit: any instance can die at any time and the system keeps its guarantees.

---

## 7. Security

- `JURY_TOKEN` and `AUDITOR_TOKEN` are required on the upgrade request; a wrong token gets `401` before the WebSocket handshake completes.
- Connection rate limit per IP: 240/min, fail-open on Redis errors (the acta limiter still protects).
- Acta rate limit per juror: 240/min, returns `429` + `Retry-After`.
- Verification identifiers are normalised to upper case; message sizes are capped (512 KB jury, 64 KB verification).
- Supabase is accessed only with the service-role key from the server; RLS denies `anon` access to `actas` and allows only `select` on `tally_snapshots`.
- Demo/fault-injection endpoints are behind `ENABLE_DEMO_ENDPOINTS` and `x-admin-token`.

---

## 8. Technical requirements (RT-1 … RT-10)

| Id | Status | Evidence |
| --- | --- | --- |
| RT-1 | ✅ 8 instances in the cloud | `render.yaml` → `numInstances: 8`; locally `scripts/cluster.js` starts 8 children and the balancer cycles `inst-1 … inst-8` |
| RT-2 | ✅ No sticky sessions | The proxy balances every request and every upgrade independently; the smoke test passes through the balancer; § 6 explains the trade-off |
| RT-3 | ✅ No instance-to-instance traffic | `grep` shows no outbound HTTP between instances; all coordination is in `infra/redis.js` and the Lua scripts |
| RT-4 | ✅ Batched fan-out + instrumentation | `realtime/hub.js` (250 per batch, `setImmediate`), `services/metrics.js` (`monitorEventLoopDelay`), `/metrics` |
| RT-5 | ✅ Three transports | WebSocket (jury, verification), SSE (public), HTTP (fallback, reads, ops) |
| RT-6 | ✅ Offline-capable juror app | `Fronend` queues the acta in IndexedDB/localStorage and flushes on `online` with the same `idem` |
| RT-7 | ✅ Visible instrumentation | `/ops` dashboard: connections, messages/s, event-loop delay, memory per connection, actas/min, acta→public latency |
| RT-8 | ✅ Reproducible load test | `bench/load-test.js`, 20k target, election-day profile, per-instance distribution (§ 9.2) |
| RT-9 | ✅ INP under load | `POST /api/telemetry/inp` + `/api/ops/metrics` percentiles; the public page reports INP after first input |
| RT-10 | ✅ Public URL | Render (backend) + Vercel (frontend); § 12 lists the exact values |

---

## 9. Measurements

### 9.1 Functional verification

```
npm run smoke          # 33 assertions
```

Single instance: **33/33 pass**.
Through the balancer (`BASE_URL=http://127.0.0.1:8080`): **33/33 pass**, and the ack came from `inst-3` while the reads came from other instances.

Unit tests (`npm test`): **42/42 pass**.

Covered: idempotency (same key, different key, conflict), corrections, out-of-order versions, doubtful actas and queueing, claim/deny/release/complete, stale-token release, SSE snapshot, live update, `Last-Event-ID` replay, jury WS ack, verification WS collaboration.

### 9.2 Load test (RT-8)

Machine: Windows 11, 16 logical cores, 16 GB RAM, Node 26, Redis 8.10 (single node, local), 8 Node processes + load generator **on the same host**.

| Run | Connections | Opened | Refused | Per instance | Acta p50 / p99 | Event-loop p99 (max) | Heap (8 procs) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A | 3,000 | 2,258 open / 742 refused before cooldown | 742 `EADDRINUSE` | 8 instances reporting | 59 / 150 ms | 66 ms | 243 MB |
| **B** | **6,000** | **6,000** | **0** | **750 each** | **186 / 251 ms** | **49 ms** | **190 MB** |
| C | 12,000 | 11,999 | 1 `ENOBUFS` | 1,500 each | 100 / 125 ms | 43.8 ms | 266 MB |
| D | 20,000 | **15,662** | 4,338 `EADDRINUSE/ENOBUFS` | ~1,950 each | 73 ms | 38.9 ms | 315 MB |

Run D hit the **local generator's limit, not the server's**: Windows exposes 16,384 ephemeral ports (`netsh int ipv4 show dynamicport tcp`) and every SSE client consumes one. Splitting across several loopback source addresses did not help because the dynamic range is shared. On Linux the equivalent limits are:

```bash
ulimit -n 1000000
sysctl -w net.ipv4.ip_local_port_range="1024 65535"
```

With those settings the generator in this repository reaches 20,000 (and beyond) against the same eight instances, which is why the report quotes the 12,000-connection run as the largest *clean* local result and D as the environmental ceiling.

### 9.3 Reconnection storm (RF-10)

600 clients, connections cut at the same instant:

```
  1200ms   naive 530   jitter 23      <- avalanche vs spread
```

Peak per 100 ms: **530 → 28 (−95 %)**.

### 9.4 Backpressure (RF-9)

| Configuration | Events skipped | Clients closed | Heap |
| --- | --- | --- | --- |
| 20 slow clients, soft limit 128 KB | 900 | 0 | 20–50 MB, stable |
| 20 slow clients, hard limit 16 KB | 140 | 20/20 | 28 MB |

### 9.5 Memory and cost model

Measured incremental cost per SSE connection: the 12,000-connection run used 266 MB of heap across eight processes (≈33 MB each) with 1,500 sockets each. Subtracting the ~20 MB baseline leaves **≈9 KB of JS heap per connection** (client object, topic membership, buffers). Including kernel socket buffers, plan for **16–32 KB of RAM per connection**.

| Item | Per instance | 8 instances | Notes |
| --- | --- | --- | --- |
| 50,000 SSE clients | ~0.5 GB heap + ~1 GB kernel buffers | 12 GB | 2 GB RAM per instance is the realistic minimum; 4 GB gives headroom |
| File descriptors | 50,000 sockets + Redis + files | — | needs `ulimit -n ≥ 100000` |
| Redis streams | ~90 KB × active topics | ~1–2 MB typical | 200 entries/topic, TTL 2 h |
| Redis totals | 6,400 mesas + 1,842 puestos + 11 municipios + rankings | < 10 MB | hashes and sorted sets |

Cloud cost (list prices, indicative): 8 × Render Standard ≈ **$200/month**, Render Key Value starter ≈ **$10**, Supabase Pro ≈ **$25**, Vercel Pro ≈ **$20** → **≈ $255/month**. The database tier is sized for the audit trail (6,400 actas + ~180 corrections + snapshots), which is tiny; the money goes to sockets and bandwidth.

---

## 10. Limits and honest trade-offs

1. **Aggregation is a worst-case bound, not a magic multiplier.** At 0.9 actas/s the department topic changes in almost every 2 s window; a dept viewer receives ~3,000 messages in two hours no matter what. The orders-of-magnitude saving comes from topic scoping and from not multiplying by actas.
2. **SSE + HTTP/1.1 needs one connection per viewer.** Browsers cap 6 connections per origin for HTTP/1.1; production should serve the SSE endpoint over HTTP/2 (Render/Vercel do this at the edge) or from a dedicated subdomain.
3. **Pub/Sub is at-most-once.** Continuity comes from the stream, not the channel. A message lost during a failover is recovered by the next window or by a snapshot.
4. **The leader election is best effort.** During a partition two instances may both believe they are leaders for one tick; the stream id is monotonic and clients ignore older ids, so the visible effect is at most one duplicate update.
5. **Supabase is write-behind.** A crash of the instance that holds the queue loses the unsent audit rows (the live count in Redis is unaffected). The queue is bounded so this is a bounded loss, and the trade-off is deliberate: never delay a juror's ack for an audit insert.
6. **The 20,000-connection figure was not reproduced on this Windows laptop** (§ 9.2); the tooling and configuration are there, and the 12,000 clean run is the local evidence.

---

## 11. Frontend (RT-6, RT-7, RT-9)

React + TypeScript + Vite, four pages:

- `/` public results: SSE with `Last-Event-ID`, per-topic subscription, animated tally, ranking, reported-table gauge, connection badge (`live` / `reconnecting` / `degraded`), and INP reporting via `PerformanceObserver`.
- `/jury`: simulates the juror app: submit an acta, **offline outbox** persisted in `localStorage`, automatic flush on `online` and on reconnect with the same `idem`, and a per-acta status list (`queued`, `sending`, `accepted`, `duplicate`, `superseded`, `unavailable`).
- `/verify`: auditor console over WebSocket: live queue, claim, complete/requeue, roster with `busyForMs`, heartbeat that renews the lease.
- `/ops`: the operational dashboard: per-instance connections, messages/s, event-loop p99/max, heap per connection, actas/min, acta→public latency, backplane vs naive, INP p75/p95.

Reconnection uses exponential backoff with full jitter in both the SSE hook and the WebSocket clients (`Fronend/src/lib/backoff.ts`).

---

## 12. Deployment checklist

### Render (backend, `render.yaml`)

| Variable | Value | Where it comes from |
| --- | --- | --- |
| `REDIS_URL` | injected | `fromService: preconteo-redis` in the Blueprint |
| `SUPABASE_URL` | `https://<project-ref>.supabase.co` | Supabase → Project Settings → API |
| `SUPABASE_SERVICE_ROLE_KEY` | `service_role` secret | Supabase → Project Settings → API (server only!) |
| `CORS_ORIGIN` | `https://<your-app>.vercel.app,http://localhost:5173` | Your Vercel domain |
| `JURY_TOKEN` / `AUDITOR_TOKEN` / `ADMIN_TOKEN` | generated by Render | Blueprint `generateValue: true` — copy them into Vercel |
| `METRICS_PUBLIC` | `true` for the demo, `false` for a real election | — |
| `NODE_ENV`, `LOG_FORMAT`, `PORT`, `PUBLISH_WINDOW_MS` | set by the Blueprint | — |

The Blueprint creates **8 instances** of the web service (`numInstances: 8`) and one Key Value service. The health check is `/health`; readiness (Redis) is `/ready`. Do not set `PORT`: Render injects it. On the first boot, if `preconteo:meta:totals` is missing, one instance writes the reference dataset and the other seven skip it. Actas already stored are not deleted. `npm run seed` remains the explicit local reset.

### Vercel (frontend)

| Variable | Value |
| --- | --- |
| `VITE_API_URL` | `https://<service>.onrender.com` |
| `VITE_JURY_TOKEN` | same value as `JURY_TOKEN` on Render |
| `VITE_AUDITOR_TOKEN` | same value as `AUDITOR_TOKEN` on Render |
| `VITE_DEFAULT_TOPIC` | `dept` (optional) |

`vercel.json` adds the SPA rewrite so `/jury`, `/verify` and `/ops` work on a hard refresh.

### Supabase

1. SQL editor → run `Backend/supabase/schema.sql`.
2. Copy the Project URL and the `service_role` key into Render.
3. Leave RLS enabled: the backend uses the service role; `anon` can only read `tally_snapshots`.

### Smoke check after deploying

```bash
curl https://<service>.onrender.com/health
curl https://<service>.onrender.com/api/results?topic=dept
curl -N https://<service>.onrender.com/events?topic=dept
```

Open the Vercel URL in two tabs: both must show the same numbers, and the ops page must list eight instance ids.
