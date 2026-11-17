import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { VOTE_CODES } from '../src/domain/election.js';
import { diffTally, emptyRank, emptyTally, sumVotes } from '../src/domain/tally.js';

describe('emptyTally', () => {
  it('contains every vote code at zero', () => {
    const tally = emptyTally();
    assert.deepEqual(Object.keys(tally).sort(), [...VOTE_CODES].sort());
    assert.equal(sumVotes(tally), 0);
  });
});

describe('diffTally', () => {
  it('returns nothing when the tally did not change', () => {
    assert.deepEqual(diffTally(emptyTally(), emptyTally()), {});
  });

  it('returns only the codes that moved', () => {
    const before = emptyTally();
    const after = { ...before, C1: 12, C2: 5 };
    assert.deepEqual(diffTally(before, after), { C1: 12, C2: 5 });
  });

  it('reports negative deltas after a correction', () => {
    const before = { ...emptyTally(), C1: 100, C2: 50 };
    const after = { ...emptyTally(), C1: 90, C2: 60 };
    assert.deepEqual(diffTally(before, after), { C1: -10, C2: 10 });
  });

  it('treats a missing previous snapshot as "no delta"', () => {
    assert.deepEqual(diffTally(null, { ...emptyTally(), C1: 3 }), {});
    assert.deepEqual(diffTally(undefined, emptyTally()), {});
  });
});

describe('sumVotes and emptyRank', () => {
  it('adds numeric strings coming from Redis', () => {
    assert.equal(sumVotes({ C1: '5', C2: 5, C3: null }), 10);
  });

  it('starts the ranking with the six candidates at zero', () => {
    const rank = emptyRank();
    assert.equal(rank.length, 6);
    assert.ok(rank.every((entry) => entry.votes === 0));
  });
});
