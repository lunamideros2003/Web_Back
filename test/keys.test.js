import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { actaScriptKeys, keys } from '../src/infra/keys.js';

describe('Redis key layout', () => {
  it('keeps every key inside the configured namespace', () => {
    const samples = [
      keys.acta('E14-001-0100-03'),
      keys.tally.department(),
      keys.tally.municipality(3),
      keys.channel('mun:3'),
      keys.history('dept'),
      keys.windowLeader(),
      keys.datasetLock(),
    ];
    for (const key of samples) {
      assert.match(key, /^preconteo:/, `${key} escaped the namespace`);
    }
  });

  it('separates channels from history streams', () => {
    assert.equal(keys.channel('dept'), 'preconteo:bus:dept');
    assert.equal(keys.history('dept'), 'preconteo:hist:dept');
  });

  it('builds one key per slug used by the atomic acta script', () => {
    const scriptKeys = actaScriptKeys({
      actaId: 'E14-001-0100-03',
      mesa: 100,
      puesto: 1,
      municipio: 1,
      idem: 'idem-12345678',
    });
    assert.equal(scriptKeys.length, 16);
    assert.equal(new Set(scriptKeys).size, 16);
    assert.equal(scriptKeys[0], 'preconteo:acta:E14-001-0100-03');
    assert.equal(scriptKeys[1], 'preconteo:idem:idem-12345678');
  });
});
