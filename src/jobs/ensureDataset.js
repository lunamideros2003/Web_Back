import { config } from '../config.js';
import { buildDataset } from '../domain/dataset.js';
import { keys } from '../infra/keys.js';
import { redis, redisGateway } from '../infra/redis.js';
import { logger } from '../lib/logger.js';

/**
 * Denominators for "tables reported": one hash field per level.
 * The live tallies stay empty until the first acta; only these totals are required
 * for the public percentages to mean something on a fresh Redis.
 */
export const referenceTotals = (dataset) => {
  const totals = {
    dept: String(dataset.totals.mesas),
  };
  for (let id = 1; id <= dataset.totals.municipios; id += 1) {
    totals[`municipality:${id}`] = String(dataset.mesasPerMunicipio[id - 1]);
  }
  for (let id = 1; id <= dataset.totals.puestos; id += 1) {
    totals[`puesto:${id}`] = String(dataset.mesasPerPuesto[id - 1]);
  }
  return totals;
};

export const writeReferenceTotals = async (dataset) => {
  const totals = referenceTotals(dataset);
  const entries = Object.entries(totals);
  for (let index = 0; index < entries.length; index += 500) {
    const slice = Object.fromEntries(entries.slice(index, index + 500));
    // Sequential chunks keep a single HSET under Redis's argument limit.
    // eslint-disable-next-line no-await-in-loop
    await redisGateway.run(() => redis.hset(keys.totals(), slice));
  }
  return totals;
};

let inflight = null;

/**
 * One of the eight instances writes the reference dataset the first time Redis
 * is empty. Later boots, and the other seven instances, leave existing actas
 * untouched. A short lock stops a simultaneous boot from writing twice.
 */
export const ensureReferenceDataset = () => {
  if (inflight) return inflight;
  inflight = runEnsure().finally(() => {
    inflight = null;
  });
  return inflight;
};

const runEnsure = async () => {
  const ready = await redisGateway.waitUntilReady(15_000);
  if (!ready) {
    logger.warn('Skipped dataset bootstrap: Redis is not ready');
    return false;
  }

  const existing = await redisGateway.run(() => redis.hget(keys.totals(), 'dept'));
  if (existing) {
    logger.info('Reference dataset already present', { mesas: existing });
    return true;
  }

  const token = `${config.instanceId}:${Date.now()}`;
  const acquired = await redisGateway.run(() =>
    redis.set(keys.datasetLock(), token, 'NX', 'PX', 60_000),
  );
  if (acquired !== 'OK') {
    logger.info('Another instance is seeding the reference dataset');
    return false;
  }

  const again = await redisGateway.run(() => redis.hget(keys.totals(), 'dept'));
  if (again) {
    logger.info('Reference dataset already present', { mesas: again });
    return true;
  }

  const dataset = buildDataset(config.election);
  const totals = await writeReferenceTotals(dataset);
  logger.info('Reference dataset bootstrapped', {
    mesas: dataset.totals.mesas,
    puestos: dataset.totals.puestos,
    municipios: dataset.totals.municipios,
    fields: Object.keys(totals).length,
  });
  return true;
};
