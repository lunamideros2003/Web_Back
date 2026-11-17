import { randomUUID } from 'node:crypto';
import { CANDIDATES, SPECIAL_VOTES } from './election.js';

const randomInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

const pick = (items) => items[Math.floor(Math.random() * items.length)];

/**
 * Generates structurally valid actas that follow the official accounting rule
 * (sum of votes === sufragantes), except when `doubtful` is requested. Used by
 * the seed/load tooling and by the demo endpoint.
 */
export const generateActa = ({
  mesa,
  puesto,
  municipio,
  version = 1,
  jurorId,
  doubtful = false,
  at = Math.floor(Date.now() / 1000),
} = {}) => {
  const sufragantes = randomInt(40, 420);
  const weights = [...CANDIDATES, ...SPECIAL_VOTES].map(() => 0.2 + Math.random());
  const totalWeight = weights.reduce((sum, weight) => sum + weight, 0);

  const votes = {};
  let assigned = 0;
  const codes = [...CANDIDATES, ...SPECIAL_VOTES];
  codes.forEach((code, index) => {
    if (index === codes.length - 1) {
      votes[code] = sufragantes - assigned;
    } else {
      const value = Math.floor((weights[index] / totalWeight) * sufragantes);
      votes[code] = value;
      assigned += value;
    }
  });

  if (doubtful) {
    const delta = randomInt(1, 25) * pick([-1, 1]);
    votes.NO_MARCADO = Math.max(0, votes.NO_MARCADO + delta);
  }

  const identifier = `E14-${String(puesto).padStart(3, '0')}-${String(mesa).padStart(4, '0')}-03`;

  return {
    acta: identifier,
    mesa,
    puesto,
    municipio,
    version,
    jurado: jurorId ?? `JUR-${randomInt(10000, 99999)}`,
    ts_cierre: at,
    votos: votes,
    sufragantes,
    hash_foto: randomUUID().replace(/-/g, '').slice(0, 16),
    idem: randomUUID().replace(/-/g, ''),
  };
};
