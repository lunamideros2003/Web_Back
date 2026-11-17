import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { CANDIDATES, SPECIAL_VOTES } from '../src/domain/election.js';
import { generateActa } from '../src/domain/generator.js';
import { casePriority, doubtReason, validateActa } from '../src/domain/validation.js';

const baseActa = (overrides = {}) => ({
  acta: 'E14-001-0100-03',
  mesa: 100,
  puesto: 1,
  municipio: 1,
  version: 1,
  jurado: 'JUR-1001',
  ts_cierre: 1_700_000_000,
  votos: Object.fromEntries([...CANDIDATES, ...SPECIAL_VOTES].map((code) => [code, 10])),
  sufragantes: 90,
  hash_foto: 'deadbeef',
  idem: 'idem-00000001',
  ...overrides,
});

describe('validateActa', () => {
  it('accepts a structurally valid acta that satisfies the accounting rule', () => {
    const result = validateActa(baseActa());
    assert.equal(result.ok, true);
    assert.equal(result.doubtful, false);
    assert.equal(result.totalVotes, 90);
    assert.equal(result.acta.acta, 'E14-001-0100-03');
  });

  it('accepts a valid acta but flags it when the accounting rule fails', () => {
    const result = validateActa(baseActa({ sufragantes: 120 }));
    assert.equal(result.ok, true);
    assert.equal(result.doubtful, true);
    assert.equal(result.totalVotes, 90);
    assert.match(doubtReason(result.acta, result.totalVotes), /30 missing/);
  });

  it('reports excess votes as well', () => {
    const result = validateActa(baseActa({ sufragantes: 60 }));
    assert.equal(result.doubtful, true);
    assert.match(doubtReason(result.acta, result.totalVotes), /30 excess/);
  });

  it('rejects a malformed acta identifier', () => {
    const result = validateActa(baseActa({ acta: 'ACTA-1' }));
    assert.equal(result.ok, false);
    assert.ok(result.errors.some((error) => error.includes('acta must match')));
  });

  it('rejects unknown vote codes', () => {
    const result = validateActa(
      baseActa({ votos: { ...baseActa().votos, C9: 3 } }),
    );
    assert.equal(result.ok, false);
    assert.ok(result.errors.some((error) => error.includes('unknown vote codes')));
  });

  it('rejects negative or fractional vote values', () => {
    const result = validateActa(
      baseActa({ votos: { ...baseActa().votos, C1: -1, C2: 1.5 } }),
    );
    assert.equal(result.ok, false);
    assert.ok(result.errors.some((error) => error.includes('votos.C1')));
    assert.ok(result.errors.some((error) => error.includes('votos.C2')));
  });

  it('rejects an out of range version', () => {
    const result = validateActa(baseActa({ version: 0 }));
    assert.equal(result.ok, false);
  });

  it('rejects a short idempotency key', () => {
    const result = validateActa(baseActa({ idem: 'short' }));
    assert.equal(result.ok, false);
  });

  it('rejects a non object payload', () => {
    assert.equal(validateActa(null).ok, false);
    assert.equal(validateActa('acta').ok, false);
  });

  it('fills missing vote codes with zero', () => {
    const result = validateActa(baseActa({ votos: { C1: 90 }, sufragantes: 90 }));
    assert.equal(result.ok, true);
    assert.equal(result.totalVotes, 90);
    assert.equal(result.acta.votos.C2, 0);
  });
});

describe('casePriority', () => {
  it('ranks bigger mismatches first', () => {
    const small = casePriority({ sufragantes: 90 }, 89);
    const medium = casePriority({ sufragantes: 90 }, 80);
    const large = casePriority({ sufragantes: 90 }, 40);
    assert.equal(large, 1);
    assert.equal(medium, 2);
    assert.equal(small, 2);
    assert.ok(large < medium);
  });
});

describe('generateActa', () => {
  it('generates actas that satisfy the accounting rule by default', () => {
    for (let index = 0; index < 200; index += 1) {
      const acta = generateActa({ mesa: 1, puesto: 1, municipio: 1 });
      const validation = validateActa(acta);
      assert.equal(validation.ok, true, validation.errors.join(', '));
      assert.equal(validation.doubtful, false);
    }
  });

  it('generates a deliberately doubtful acta on request', () => {
    let doubtful = 0;
    for (let index = 0; index < 100; index += 1) {
      const validation = validateActa(generateActa({ mesa: 2, puesto: 1, municipio: 1, doubtful: true }));
      if (validation.doubtful) doubtful += 1;
    }
    assert.ok(doubtful > 80, `expected most actas to be doubtful, got ${doubtful}`);
  });
});
