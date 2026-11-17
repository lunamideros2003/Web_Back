import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  LEVELS,
  compareStreamIds,
  incrementStreamId,
  isPublicTopic,
  parseTopic,
  topicId,
} from '../src/domain/election.js';

describe('parseTopic', () => {
  it('parses the department topic', () => {
    assert.deepEqual(parseTopic('dept'), { id: 'dept', level: LEVELS.DEPARTMENT, key: 'dept' });
  });

  it('parses a municipality topic', () => {
    assert.deepEqual(parseTopic('mun:7'), { id: 7, level: LEVELS.MUNICIPALITY, key: 'mun:7' });
  });

  it('parses a polling station topic', () => {
    assert.deepEqual(parseTopic('puesto:1842'), {
      id: 1842,
      level: LEVELS.PUESTO,
      key: 'puesto:1842',
    });
  });

  it('rejects unknown prefixes, zero ids and garbage', () => {
    for (const raw of ['', 'mun', 'mun:0', 'mun:-1', 'mun:abc', 'municipality:3', 'dept:1', null]) {
      assert.throws(() => parseTopic(raw), undefined, `expected ${raw} to be rejected`);
    }
  });

  it('round-trips with topicId', () => {
    assert.equal(topicId(LEVELS.MUNICIPALITY, 3), 'mun:3');
    assert.equal(topicId(LEVELS.PUESTO, 9), 'puesto:9');
    assert.equal(parseTopic(topicId(LEVELS.MUNICIPALITY, 3)).id, 3);
  });

  it('classifies public topics', () => {
    assert.equal(isPublicTopic('dept'), true);
    assert.equal(isPublicTopic('mun:1'), true);
    assert.equal(isPublicTopic('secret'), false);
  });
});

describe('stream ids', () => {
  it('compares ids by milliseconds and then by sequence', () => {
    assert.ok(compareStreamIds('100-1', '100-2') < 0);
    assert.ok(compareStreamIds('100-9', '101-0') < 0);
    assert.ok(compareStreamIds('200-0', '199-9') > 0);
    assert.equal(compareStreamIds('300-5', '300-5'), 0);
  });

  it('compares ids that exceed Number.MAX_SAFE_INTEGER exactly', () => {
    // 9007199254740993 cannot be represented as a double: BigInt is required.
    assert.ok(compareStreamIds('9007199254740993-0', '9007199254740992-0') > 0);
    assert.ok(compareStreamIds('9007199254740993-1', '9007199254740993-2') < 0);
  });

  it('increments the sequence part', () => {
    assert.equal(incrementStreamId('5-0'), '5-1');
    assert.equal(incrementStreamId('5-9'), '5-10');
    assert.equal(incrementStreamId('5'), '5-1');
  });
});
