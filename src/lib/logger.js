const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };

const configuredLevel = (process.env.LOG_LEVEL ?? 'info').toLowerCase();
const minLevel = LEVELS[configuredLevel] ?? LEVELS.info;
const jsonFormat = (process.env.LOG_FORMAT ?? (process.env.NODE_ENV === 'production' ? 'json' : 'pretty')) === 'json';

const write = (level, message, context) => {
  if (LEVELS[level] < minLevel) return;
  const payload = {
    ts: new Date().toISOString(),
    level,
    instance: process.env.INSTANCE_ID ?? process.env.RENDER_INSTANCE_ID ?? String(process.pid),
    message,
    ...(context ?? {}),
  };

  if (jsonFormat) {
    process.stdout.write(`${JSON.stringify(payload)}\n`);
    return;
  }

  const extra = context && Object.keys(context).length > 0 ? ` ${JSON.stringify(context)}` : '';
  process.stdout.write(`${payload.ts} ${level.toUpperCase().padEnd(5)} ${message}${extra}\n`);
};

export const logger = {
  debug: (message, context) => write('debug', message, context),
  info: (message, context) => write('info', message, context),
  warn: (message, context) => write('warn', message, context),
  error: (message, context) => write('error', message, context),
};

export const serializeError = (error) => ({
  name: error?.name,
  message: error?.message,
  code: error?.code,
  stack: process.env.NODE_ENV === 'production' ? undefined : error?.stack,
});
