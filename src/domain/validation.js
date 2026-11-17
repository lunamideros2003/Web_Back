import { ACTA_PATTERN, CANDIDATES, SPECIAL_VOTES, VOTE_CODES } from './election.js';

const isPlainObject = (value) => typeof value === 'object' && value !== null && !Array.isArray(value);
const asNonNegativeInt = (value) => (Number.isInteger(value) && value >= 0 ? value : null);

/**
 * Structural validation. A structurally valid acta is always accepted into the
 * pipeline; the accounting rule only decides whether it also needs a human.
 */
export const validateActa = (input) => {
  const errors = [];

  if (!isPlainObject(input)) {
    return { ok: false, errors: ['acta payload must be an object'] };
  }

  const actaId = typeof input.acta === 'string' ? input.acta.trim() : '';
  if (!ACTA_PATTERN.test(actaId)) errors.push('acta must match E14-<puesto>-<mesa>-<corporation>');

  const mesa = asNonNegativeInt(input.mesa);
  const puesto = asNonNegativeInt(input.puesto);
  const municipio = asNonNegativeInt(input.municipio);
  if (!mesa) errors.push('mesa must be a positive integer');
  if (!puesto) errors.push('puesto must be a positive integer');
  if (!municipio) errors.push('municipio must be a positive integer');

  const version = asNonNegativeInt(input.version);
  if (!version || version < 1) errors.push('version must be an integer >= 1');

  const sufragantes = asNonNegativeInt(input.sufragantes);
  if (sufragantes === null) errors.push('sufragantes must be a non negative integer');

  if (typeof input.jurado !== 'string' || input.jurado.trim().length < 3) {
    errors.push('jurado is required');
  }

  if (!Number.isFinite(input.ts_cierre)) errors.push('ts_cierre must be a unix timestamp');

  const idem = typeof input.idem === 'string' ? input.idem.trim() : '';
  if (idem.length < 8 || idem.length > 128) errors.push('idem must be between 8 and 128 characters');

  if (input.hash_foto !== undefined && typeof input.hash_foto !== 'string') {
    errors.push('hash_foto must be a string');
  }

  if (!isPlainObject(input.votos)) {
    errors.push('votos must be an object');
    return { ok: false, errors };
  }

  const unknown = Object.keys(input.votos).filter((code) => !VOTE_CODES.includes(code));
  if (unknown.length > 0) errors.push(`unknown vote codes: ${unknown.join(', ')}`);

  const votos = {};
  for (const code of [...CANDIDATES, ...SPECIAL_VOTES]) {
    const raw = input.votos[code] ?? 0;
    const value = asNonNegativeInt(raw);
    if (value === null) {
      errors.push(`votos.${code} must be a non negative integer`);
    } else {
      votos[code] = value;
    }
  }

  if (errors.length > 0) return { ok: false, errors };

  const totalVotes = VOTE_CODES.reduce((sum, code) => sum + votos[code], 0);
  const doubtful = totalVotes !== sufragantes;

  return {
    ok: true,
    errors: [],
    doubtful,
    totalVotes,
    acta: {
      acta: actaId,
      mesa,
      puesto,
      municipio,
      version,
      jurado: input.jurado.trim(),
      ts_cierre: Math.trunc(input.ts_cierre),
      votos,
      sufragantes,
      hash_foto: input.hash_foto ?? null,
      idem,
    },
  };
};

export const doubtReason = (acta, totalVotes) => {
  const difference = acta.sufragantes - totalVotes;
  const direction = difference > 0 ? 'missing' : 'excess';
  return `accounting mismatch: sufragantes=${acta.sufragantes}, votes=${totalVotes}, ${Math.abs(difference)} ${direction}`;
};

/** Priority for the verification queue: lower score = reviewed first. */
export const casePriority = (acta, totalVotes) => {
  const difference = Math.abs(acta.sufragantes - totalVotes);
  if (difference > 20) return 1;
  if (difference > 0) return 2;
  return 3;
};
