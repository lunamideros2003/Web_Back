import { config } from '../src/config.js';
import { buildDataset } from '../src/domain/dataset.js';
import { redis, redisGateway } from '../src/infra/redis.js';
import { writeReferenceTotals } from '../src/jobs/ensureDataset.js';
import { logger } from '../src/lib/logger.js';

/**
 * Wipes the Redis namespace and rewrites the reference dataset
 * (6,400 polling tables, 1,842 stations, 11 municipalities).
 * Without those totals, the reported-table percentage has no denominator.
 */
const seed = async () => {
  const ready = await redisGateway.waitUntilReady(10_000);
  if (!ready) throw new Error('Redis is not reachable');
  const dataset = buildDataset(config.election);

  let cursor = '0';
  let deleted = 0;
  do {
    const [next, batch] = await redisGateway.run(() =>
      redis.scan(cursor, 'MATCH', `${config.keyPrefix}:*`, 'COUNT', 500),
    );
    cursor = next;
    if (batch.length > 0) {
      await redisGateway.run(() => redis.del(...batch));
      deleted += batch.length;
    }
  } while (cursor !== '0');

  const totals = await writeReferenceTotals(dataset);

  logger.info('Dataset seeded', {
    deletedKeys: deleted,
    mesas: dataset.totals.mesas,
    puestos: dataset.totals.puestos,
    municipios: dataset.totals.municipios,
    totalsFields: Object.keys(totals).length,
  });

  const sample = [1, 2, 3].map((mesa) => ({
    mesa,
    puesto: dataset.puestoOfMesa.get(mesa),
    municipio: dataset.municipalityOfMesa.get(mesa),
  }));
  logger.info('Sample mapping', { sample });
};

seed()
  .catch((error) => {
    logger.error('Seed failed', { message: error?.message, stack: error?.stack });
    process.exitCode = 1;
  })
  .finally(async () => {
    await redisGateway.close().catch(() => {});
    setTimeout(() => process.exit(process.exitCode ?? 0), 100);
  });
