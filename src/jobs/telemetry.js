import { config } from '../config.js';
import { actaRepository } from '../infra/supabaseRepository.js';
import { logger } from '../lib/logger.js';
import { metrics } from '../metrics.js';
import { publishInstanceTelemetry } from '../services/clusterView.js';
import { historyService } from '../services/historyService.js';
import { readTopicState } from '../services/tallyService.js';

let lastSnapshot = null;
let timers = [];

export const getLocalSnapshot = () => {
  if (lastSnapshot && Date.now() - Date.parse(lastSnapshot.timestamp) < 5000) return lastSnapshot;
  lastSnapshot = metrics.snapshot();
  return lastSnapshot;
};

/**
 * Every instance publishes its own telemetry to a Redis hash. That is how the
 * operations dashboard sees eight processes without any instance talking to
 * another one.
 */
export const startTelemetry = () => {
  const report = setInterval(async () => {
    lastSnapshot = metrics.snapshot();
    try {
      await publishInstanceTelemetry(lastSnapshot);
    } catch {
      // Redis down: the local snapshot is still served by /metrics.
    }
  }, config.metrics.reportMs);

  const snapshot = setInterval(async () => {
    if (!config.supabase.enabled) return;
    const topics = ['dept'];
    for (let id = 1; id <= config.election.municipios; id += 1) topics.push(`mun:${id}`);

    for (const topic of topics) {
      try {
        // eslint-disable-next-line no-await-in-loop
        const state = await readTopicState(topic);
        // eslint-disable-next-line no-await-in-loop
        const seq = await historyService.latestId(topic);
        // eslint-disable-next-line no-await-in-loop
        await actaRepository.saveSnapshot(topic, state, seq);
      } catch {
        // Best effort: the next snapshot retries.
      }
    }
  }, config.supabase.snapshotIntervalMs);

  if (report.unref) report.unref();
  if (snapshot.unref) snapshot.unref();
  timers = [report, snapshot];

  logger.info('Telemetry started', {
    reportMs: config.metrics.reportMs,
    snapshotMs: config.supabase.snapshotIntervalMs,
    supabase: config.supabase.enabled,
  });
};

export const stopTelemetry = () => {
  for (const timer of timers) clearInterval(timer);
  timers = [];
};
