import { spawn } from 'node:child_process';
import http from 'node:http';
import httpProxy from 'http-proxy';
import 'dotenv/config';

/**
 * Local reproduction of the production topology: N stateless instances behind
 * a round-robin load balancer, with no sticky sessions.
 *
 *   node scripts/cluster.js --instances 8 --port 8080 --base-port 3101
 */
const arg = (name, fallback) => {
  const index = process.argv.indexOf(`--${name}`);
  if (index === -1) return fallback;
  return Number.parseInt(process.argv[index + 1], 10);
};

const instances = arg('instances', 8);
const port = arg('port', 8080);
const basePort = arg('base-port', 3101);

const children = [];
const targets = [];

for (let index = 0; index < instances; index += 1) {
  const targetPort = basePort + index;
  const target = `http://127.0.0.1:${targetPort}`;
  targets.push(target);

  const child = spawn(process.execPath, ['src/index.js'], {
    env: {
      ...process.env,
      PORT: String(targetPort),
      INSTANCE_ID: `inst-${index + 1}`,
    },
    stdio: 'inherit',
  });
  children.push(child);
  child.on('exit', (code) => {
    if (code !== 0 && code !== null) console.error(`[cluster] inst-${index + 1} exited with ${code}`);
  });
}

const proxy = httpProxy.createProxyServer({ xfwd: true });
proxy.on('error', (error, req, res) => {
  if (res && typeof res.writeHead === 'function' && !res.headersSent) {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'bad_gateway', detail: error.message }));
    return;
  }
  if (res && typeof res.destroy === 'function') res.destroy();
});

let cursor = 0;
const nextTarget = () => {
  const target = targets[cursor % targets.length];
  cursor += 1;
  return target;
};

const server = http.createServer((req, res) => {
  proxy.web(req, res, { target: nextTarget() });
});

// Every upgrade is balanced independently: two sockets from the same browser
// can (and should) land on different instances.
server.on('upgrade', (req, socket, head) => {
  proxy.ws(req, socket, head, { target: nextTarget() });
});

server.listen(port, '0.0.0.0', () => {
  console.log(`[cluster] ${instances} instances behind http://127.0.0.1:${port} (round robin, no sticky sessions)`);
  console.log(`[cluster] targets: ${targets.join(', ')}`);
});

const shutdown = () => {
  console.log('[cluster] stopping instances');
  for (const child of children) child.kill('SIGTERM');
  server.close(() => process.exit(0));
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
