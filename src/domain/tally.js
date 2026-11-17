import { CANDIDATES, VOTE_CODES } from './election.js';

/** Zeroed accumulator with one field per vote code. */
export const emptyTally = () => Object.fromEntries(VOTE_CODES.map((code) => [code, 0]));

/**
 * Minimal delta between two accumulated tallies. Only the codes whose value
 * changed are present, which keeps the 2s window message small.
 */
export const diffTally = (previous, next) => {
  if (!previous) return {};
  const changes = {};
  for (const code of VOTE_CODES) {
    const before = Number(previous[code] ?? 0);
    const after = Number(next[code] ?? 0);
    if (before !== after) changes[code] = after - before;
  }
  return changes;
};

export const sumVotes = (tally) =>
  VOTE_CODES.reduce((total, code) => total + Number(tally?.[code] ?? 0), 0);

export const emptyRank = () => CANDIDATES.map((code) => ({ code, votes: 0 }));
