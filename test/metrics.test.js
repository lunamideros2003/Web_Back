import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Percentiles, RateCounter } from '../src/services/metrics.js';

describe('Percentiles', () => {
  it('reports zero before any observation', () => {
    const summary = new Percentiles(8).summary();
    assert.equal(summary.count, 0);
    assert.equal(summary.p99Ms, 0);
  });

  it('computes p50, p95 and p99 on a known distribution', () => {
    const ring = new Percentiles(1000);
    for (let value = 1; value <= 100; value += 1) ring.observe(value);
    const summary = ring.summary();
    assert.equal(summary.count, 100);
    assert.equal(summary.p50Ms, 50);
    assert.equal(summary.p95Ms, 95);
    assert.equal(summary.p99Ms, 99);
    assert.equal(summary.maxMs, 100);
  });

  it('keeps a fixed memory footprint (ring buffer)', () => {
    const ring = new Percentiles(10);
    for (let value = 1; value <= 1000; value += 1) ring.observe(value);
    assert.equal(ring.count, 10);
    assert.equal(ring.summary().maxMs, 1000);
  });
});

describe('RateCounter', () => {
  it('accumulates a running total', () => {
    const counter = new RateCounter();
    counter.inc();
    counter.inc(4);
    assert.equal(counter.total, 5);
  });

  it('reports a per second rate from the current bucket', () => {
    const counter = new RateCounter();
    counter.inc(60);
    assert.ok(counter.perSecond(1) >= 60);
    assert.ok(counter.perMinute(1) >= 60);
  });
});
