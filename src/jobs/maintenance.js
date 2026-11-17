import { config } from '../config.js';
import { logger } from '../lib/logger.js';
import { presenceService } from '../services/presenceService.js';
import { publishVerification, sweepExpiredLocks } from '../services/verificationService.js';

/**
 * Background safety nets. Both sweeps exist because a client that dies without
 * a close frame cannot clean up after itself.
 */
export const startMaintenance = () => {
  const presenceSweep = setInterval(async () => {
    try {
      const ghosts = await presenceService.sweep();
      for (const auditorId of ghosts) {
        // eslint-disable-next-line no-await-in-loop
        await publishVerification({ type: 'presence.updated', auditorId, action: 'expired' });
        logger.info('Ghost auditor removed from presence', { auditorId });
      }
    } catch {
      // Redis unavailable; retry on the next tick.
    }
  }, config.presence.sweepMs);

  const lockSweep = setInterval(async () => {
    try {
      const requeued = await sweepExpiredLocks();
      if (requeued.length > 0) {
        logger.info('Expired locks returned to the queue', { count: requeued.length });
      }
    } catch {
      // Redis unavailable; retry on the next tick.
    }
  }, 3000);

  if (presenceSweep.unref) presenceSweep.unref();
  if (lockSweep.unref) lockSweep.unref();

  logger.info('Maintenance sweeps started', {
    presenceSweepMs: config.presence.sweepMs,
    lockSweepMs: 3000,
  });

  return () => {
    clearInterval(presenceSweep);
    clearInterval(lockSweep);
  };
};
