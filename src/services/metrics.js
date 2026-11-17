import { monitorEventLoopDelay } from 'node:perf_hooks';

/**
 * Sliding-window counter built from 1s buckets. Rates stay correct even if the
 * dashboard samples at irregular intervals.
 */
export class RateCounter {
  constructor(total = 0, windowSeconds = 120) {
    this.buckets = new Float64Array(windowSeconds);
    this.windowSeconds = windowSeconds;
    this.currentSecond = Math.floor(Date.now() / 1000);
    this.total = total;
  }

  #advance() {
    const now = Math.floor(Date.now() / 1000);
    let elapsed = now - this.currentSecond;
    if (elapsed <= 0) return;
    if (elapsed > this.windowSeconds) elapsed = this.windowSeconds;
    for (let i = 1; i <= elapsed; i += 1) {
      this.buckets[(this.currentSecond + i) % this.windowSeconds] = 0;
    }
    this.currentSecond = now;
  }

  inc(amount = 1) {
    this.#advance();
    this.total += amount;
    this.buckets[this.currentSecond % this.windowSeconds] += amount;
  }

  perSecond(windowSeconds = 10) {
    this.#advance();
    const span = Math.max(1, Math.min(windowSeconds, this.windowSeconds));
    let sum = 0;
    for (let i = 0; i < span; i += 1) {
      sum += this.buckets[(this.currentSecond - i + this.windowSeconds) % this.windowSeconds];
    }
    return Math.round((sum / span) * 100) / 100;
  }

  perMinute(windowSeconds = 60) {
    return Math.round(this.perSecond(windowSeconds) * 60 * 100) / 100;
  }
}

/** Fixed-size ring buffer used to compute percentiles without unbounded memory. */
export class Percentiles {
  constructor(size = 1024) {
    this.size = size;
    this.values = new Float64Array(size);
    this.index = 0;
    this.count = 0;
  }

  observe(value) {
    this.values[this.index] = value;
    this.index = (this.index + 1) % this.size;
    this.count = Math.min(this.count + 1, this.size);
  }

  #sorted() {
    return Array.from(this.values.subarray(0, this.count)).sort((a, b) => a - b);
  }

  percentile(p) {
    if (this.count === 0) return 0;
    const sorted = this.#sorted();
    const position = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
    return Math.round(sorted[position] * 1000) / 1000;
  }

  summary() {
    if (this.count === 0) return { count: 0, meanMs: 0, p50Ms: 0, p95Ms: 0, p99Ms: 0, maxMs: 0 };
    const sorted = this.#sorted();
    const sum = sorted.reduce((acc, value) => acc + value, 0);
    return {
      count: this.count,
      meanMs: Math.round((sum / sorted.length) * 1000) / 1000,
      p50Ms: this.percentile(50),
      p95Ms: this.percentile(95),
      p99Ms: this.percentile(99),
      maxMs: Math.round(sorted[sorted.length - 1] * 1000) / 1000,
    };
  }
}

const round = (value, decimals = 2) => {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
};

/**
 * In-process instrumentation for one instance. A snapshot is published to
 * Redis so the operations dashboard can aggregate the eight instances.
 */
export class Metrics {
  constructor({ instanceId }) {
    this.instanceId = instanceId;
    this.startedAt = Date.now();
    this.counters = {
      actasReceived: new RateCounter(),
      actasAccepted: new RateCounter(),
      actasDuplicate: new RateCounter(),
      actasStale: new RateCounter(),
      actasRejected: new RateCounter(),
      actasDoubtful: new RateCounter(),
      actasCorrected: new RateCounter(),
      messagesPublished: new RateCounter(),
      sseEventsSent: new RateCounter(),
      sseEventsSkipped: new RateCounter(),
      wsMessagesIn: new RateCounter(),
      wsMessagesOut: new RateCounter(),
      connectionsOpened: new RateCounter(),
      redisErrors: new RateCounter(),
      supabaseDropped: new RateCounter(),
      slowClientsClosed: new RateCounter(),
      reconnects: new RateCounter(),
    };
    this.gauges = new Map();
    this.publishLatency = new Percentiles(2048);
    this.actaLatency = new Percentiles(2048);
    this.eventLoop = monitorEventLoopDelay({ resolution: 10 });
    this.eventLoop.enable();
    this.peakEventLoopMs = 0;
    this.redisStatus = 'connecting';
    this.naiveMessages = 0;
    this.lastLoop = { meanMs: 0, p50Ms: 0, p99Ms: 0, maxMs: 0 };
  }

  inc(counter, amount = 1) {
    const target = this.counters[counter];
    if (target) target.inc(amount);
  }

  setGauge(name, value) {
    this.gauges.set(name, typeof value === 'function' ? value : () => value);
  }

  observePublishLatency(ms) {
    this.publishLatency.observe(ms);
  }

  observeActaLatency(ms) {
    this.actaLatency.observe(ms);
  }

  addNaiveMessages(count) {
    this.naiveMessages += count;
  }

  /**
   * Reading the histogram resets it, so the dashboard shows the delay of the
   * last sampling interval (the interesting number during the peak) while the
   * cumulative peak is kept separately.
   */
  readEventLoop() {
    const mean = this.eventLoop.mean / 1e6;
    const max = this.eventLoop.max / 1e6;
    const p50 = this.eventLoop.percentile(50) / 1e6;
    const p99 = this.eventLoop.percentile(99) / 1e6;
    this.eventLoop.reset();
    if (Number.isFinite(max) && max > this.peakEventLoopMs) this.peakEventLoopMs = round(max, 3);
    const snapshot = {
      meanMs: round(Number.isFinite(mean) ? mean : 0, 3),
      p50Ms: round(Number.isFinite(p50) ? p50 : 0, 3),
      p99Ms: round(Number.isFinite(p99) ? p99 : 0, 3),
      maxMs: round(Number.isFinite(max) ? max : 0, 3),
      peakMs: this.peakEventLoopMs,
    };
    this.lastLoop = snapshot;
    return snapshot;
  }

  snapshot() {
    const gauges = {};
    for (const [name, read] of this.gauges.entries()) {
      try {
        gauges[name] = read();
      } catch {
        gauges[name] = null;
      }
    }

    const memory = process.memoryUsage();
    const totalConnections = gauges.totalConnections ?? 0;
    const published = this.counters.messagesPublished.total;
    const naive = this.naiveMessages;

    return {
      instanceId: this.instanceId,
      uptimeSeconds: Math.round((Date.now() - this.startedAt) / 1000),
      redisStatus: this.redisStatus,
      gauges,
      connections: {
        sse: gauges.sseConnections ?? 0,
        jury: gauges.juryConnections ?? 0,
        verification: gauges.verificationConnections ?? 0,
        total: totalConnections,
      },
      eventLoop: this.readEventLoop(),
      rates: {
        actasPerMinute: this.counters.actasAccepted.perMinute(),
        messagesPerSecond: this.counters.messagesPublished.perSecond(),
        sseEventsPerSecond: this.counters.sseEventsSent.perSecond(),
        connectionsPerSecond: this.counters.connectionsOpened.perSecond(),
        reconnectsPerSecond: this.counters.reconnects.perSecond(),
        qualifiedActasPerMinute: this.counters.actasReceived.perMinute(),
      },
      totals: {
        actasReceived: this.counters.actasReceived.total,
        actasAccepted: this.counters.actasAccepted.total,
        actasDuplicate: this.counters.actasDuplicate.total,
        actasStale: this.counters.actasStale.total,
        actasRejected: this.counters.actasRejected.total,
        actasDoubtful: this.counters.actasDoubtful.total,
        actasCorrected: this.counters.actasCorrected.total,
        messagesPublished: published,
        naiveMessages: naive,
        messageReductionPct: naive > 0 ? round((1 - published / naive) * 100, 4) : 0,
        sseEventsSent: this.counters.sseEventsSent.total,
        sseEventsSkipped: this.counters.sseEventsSkipped.total,
        slowClientsClosed: this.counters.slowClientsClosed.total,
        supabaseDropped: this.counters.supabaseDropped.total,
        redisErrors: this.counters.redisErrors.total,
      },
      latency: {
        publish: this.publishLatency.summary(),
        actaToPublic: this.actaLatency.summary(),
      },
      memory: {
        rssMb: round(memory.rss / 1024 / 1024, 1),
        heapUsedMb: round(memory.heapUsed / 1024 / 1024, 1),
        perConnectionKb: totalConnections > 0 ? round((memory.heapUsed / 1024 / totalConnections), 1) : 0,
      },
      timestamp: new Date().toISOString(),
    };
  }

  toPrometheus(snapshot = this.snapshot()) {
    const lines = [];
    const metric = (name, help, type, value, labels = '') => {
      lines.push(`# HELP ${name} ${help}`);
      lines.push(`# TYPE ${name} ${type}`);
      lines.push(`${name}${labels} ${value}`);
    };

    metric('preconteo_connections', 'Open realtime connections', 'gauge', snapshot.connections.total, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_sse_connections', 'Open SSE connections', 'gauge', snapshot.connections.sse, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_jury_connections', 'Open jury WebSocket connections', 'gauge', snapshot.connections.jury, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_event_loop_p99_ms', 'Event loop delay p99 in milliseconds', 'gauge', snapshot.eventLoop.p99Ms, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_event_loop_max_ms', 'Event loop delay max in milliseconds', 'gauge', snapshot.eventLoop.maxMs, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_messages_published_total', 'Aggregated messages published', 'counter', snapshot.totals.messagesPublished, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_naive_messages_total', 'Messages a per-acta per-client fan-out would emit', 'counter', snapshot.totals.naiveMessages, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_actas_accepted_total', 'Actas accepted', 'counter', snapshot.totals.actasAccepted, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_actas_doubtful_total', 'Actas flagged for verification', 'counter', snapshot.totals.actasDoubtful, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_heap_used_mb', 'Heap used in megabytes', 'gauge', snapshot.memory.heapUsedMb, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_publish_latency_p95_ms', 'Acta reception to public publish p95', 'gauge', snapshot.latency.actaToPublic.p95Ms, `{instance="${snapshot.instanceId}"}`);
    metric('preconteo_redis_up', 'Redis reachable from this instance', 'gauge', snapshot.redisStatus === 'ready' ? 1 : 0, `{instance="${snapshot.instanceId}"}`);
    return `${lines.join('\n')}\n`;
  }
}
