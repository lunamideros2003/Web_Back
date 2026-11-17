export const CANDIDATES = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6'];
export const SPECIAL_VOTES = ['BLANCO', 'NULO', 'NO_MARCADO'];
export const VOTE_CODES = [...CANDIDATES, ...SPECIAL_VOTES];

export const ACTA_PATTERN = /^[A-Z0-9]{2,8}-\d{1,6}-\d{1,6}-\d{1,4}$/;

export const LEVELS = Object.freeze({
  DEPARTMENT: 'department',
  MUNICIPALITY: 'municipality',
  PUESTO: 'puesto',
  MESA: 'mesa',
});

/**
 * Public subscription topics:
 *   dept          -> department-wide accumulated totals
 *   mun:<id>      -> one municipality
 *   puesto:<id>   -> one polling station
 */
export const topicId = (level, id) => {
  if (level === LEVELS.DEPARTMENT) return 'dept';
  if (level === LEVELS.MUNICIPALITY) return `mun:${id}`;
  if (level === LEVELS.PUESTO) return `puesto:${id}`;
  throw new Error(`Unsupported public topic level: ${level}`);
};

export const parseTopic = (raw) => {
  if (typeof raw !== 'string' || raw.length === 0) {
    throw new Error('A topic is required');
  }
  if (raw === 'dept') {
    return { id: 'dept', level: LEVELS.DEPARTMENT, key: 'dept' };
  }
  const [prefix, value] = raw.split(':');
  const id = Number.parseInt(value, 10);
  if (!Number.isInteger(id) || id <= 0) {
    throw new Error(`Invalid topic: ${raw}`);
  }
  if (prefix === 'mun') {
    return { id, level: LEVELS.MUNICIPALITY, key: `mun:${id}` };
  }
  if (prefix === 'puesto') {
    return { id, level: LEVELS.PUESTO, key: `puesto:${id}` };
  }
  throw new Error(`Invalid topic: ${raw}`);
};

export const isPublicTopic = (raw) => {
  try {
    parseTopic(raw);
    return true;
  } catch {
    return false;
  }
};

/** Compares Redis Stream IDs ("<ms>-<seq>") without losing precision. */
export const compareStreamIds = (left, right) => {
  if (left === right) return 0;
  const parse = (value) => {
    const [ms, seq] = String(value).split('-');
    return [BigInt(ms), BigInt(seq ?? 0)];
  };
  try {
    const [lms, lseq] = parse(left);
    const [rms, rseq] = parse(right);
    if (lms === rms) return lseq === rseq ? 0 : lseq < rseq ? -1 : 1;
    return lms < rms ? -1 : 1;
  } catch {
    return String(left) < String(right) ? -1 : 1;
  }
};

export const incrementStreamId = (id) => {
  const [ms, seq] = String(id).split('-');
  return `${ms}-${BigInt(seq ?? 0) + 1n}`;
};
