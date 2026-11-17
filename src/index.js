import http from 'node:http';
import { config } from './config.js';
import { createApp } from './http/app.js';
import { redisGateway } from './infra/redis.js';
import { actaRepository } from './infra/supabaseRepository.js';
import { ensureReferenceDataset } from './jobs/ensureDataset.js';
import { startMaintenance } from './jobs/maintenance.js';
import { startTelemetry, stopTelemetry } from './jobs/telemetry.js';
import { logger, serializeError } from './lib/logger.js';
import { metrics } from './metrics.js';
import { hub } from './realtime/hub.js';
import { createJuryGateway } from './realtime/juryGateway.js';
import { createVerificationGateway } from './realtime/verificationGateway.js';
import { windowPublisher } from './services/windowPublisher.js';

hub.registerGauges();

const app = createApp();
const server = http.createServer(app);
server.keepAliveTimeout = 65_000;
server.headersTimeout = 66_000;

const juryGateway = createJuryGateway(server);
const verificationGateway = createVerificationGateway(server);

actaRepository.start();
windowPublisher.start();
startTelemetry();
const stopMaintenance = startMaintenance();

const onRedisReady = () => {
  hub.resubscribeAll().catch(() => {});
  ensureReferenceDataset().catch((error) => {
    logger.error('Dataset bootstrap failed', serializeError(error));
  });
};

if (redisGateway.isReady()) onRedisReady();
redisGateway.on('status', (status) => {
  if (status === 'ready') onRedisReady();
});

server.listen(config.port, config.host, () => {
  logger.info('Instance listening', {
    port: config.port,
    env: config.env,
    redis: config.redis.url.replace(/\/\/.*@/, '//***@'),
    supabase: config.supabase.enabled,
    demoEndpoints: config.demo.enabled,
  });
});

let shuttingDown = false;
const shutdown = async (signal) => {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info('Shutting down', { signal });

  windowPublisher.stop();
  stopTelemetry();
  stopMaintenance();
  juryGateway.close();
  verificationGateway.close();

  await new Promise((resolve) => server.close(resolve));
  await redisGateway.close().catch(() => {});
  logger.info('Shutdown complete', { uptimeSeconds: Math.round(process.uptime()) });
  process.exit(0);
};

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled rejection', serializeError(reason instanceof Error ? reason : new Error(String(reason))));
});

process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception', serializeError(error));
  metrics.inc('actasRejected');
});

void metrics;
