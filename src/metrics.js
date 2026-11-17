import { config } from './config.js';
import { Metrics } from './services/metrics.js';

export const metrics = new Metrics({ instanceId: config.instanceId });
