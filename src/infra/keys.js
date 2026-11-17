import { config } from '../config.js';

/**
 * Every Redis key used by the system lives here. Keeping the layout in one
 * module makes the memory model auditable (see docs/REPORT.md) and lets the
 * deployment use a namespace without touching the business code.
 */
const base = `${config.keyPrefix}:`;

export const keys = {
  root: (suffix) => `${base}${suffix}`,

  acta: (actaId) => `${base}acta:${actaId}`,
  idempotency: (idem) => `${base}idem:${idem}`,
  lock: (actaId) => `${base}lock:acta:${actaId}`,
  lockExpiryIndex: () => `${base}locks:expiry`,
  auditorLocks: (auditorId) => `${base}auditor:locks:${auditorId}`,

  tally: {
    department: () => `${base}tally:dept`,
    municipality: (id) => `${base}tally:mun:${id}`,
    puesto: (id) => `${base}tally:puesto:${id}`,
    mesa: (id) => `${base}tally:mesa:${id}`,
  },
  rank: {
    department: () => `${base}rank:dept`,
    municipality: (id) => `${base}rank:mun:${id}`,
  },
  reportedCount: {
    department: () => `${base}reported:dept`,
    municipality: (id) => `${base}reported:mun:${id}`,
    puesto: (id) => `${base}reported:puesto:${id}`,
  },
  totals: () => `${base}meta:totals`,

  dirtyTopics: () => `${base}dirty:topics`,
  lastPublished: (topic) => `${base}published:last:${topic}`,
  windowFirst: (topic) => `${base}win:first:${topic}`,
  windowActas: (topic) => `${base}win:actas:${topic}`,
  history: (topic) => `${base}hist:${topic}`,
  channel: (topic) => `${base}bus:${topic}`,
  verificationChannel: () => `${base}bus:verification`,

  queue: () => `${base}queue:verification`,
  case: (actaId) => `${base}case:${actaId}`,
  casesIndex: () => `${base}cases:index`,
  activeCases: () => `${base}cases:active`,

  presence: (auditorId) => `${base}presence:${auditorId}`,
  presenceIndex: () => `${base}presence:index`,

  rateLimit: (scope, subject, windowStart) => `${base}rl:${scope}:${subject}:${windowStart}`,

  windowLeader: () => `${base}coord:publisher-leader`,
  datasetLock: () => `${base}coord:dataset-seed`,
  instanceTelemetry: () => `${base}ops:instances`,
  instanceAlive: () => `${base}ops:alive`,

  stats: (field) => `${base}stats:${field}`,
};

/** Keys touched by the Lua scripts, resolved in one place. */
export const actaScriptKeys = ({ actaId, mesa, puesto, municipio, idem }) => [
  keys.acta(actaId),
  keys.idempotency(idem),
  keys.tally.mesa(mesa),
  keys.tally.puesto(puesto),
  keys.tally.municipality(municipio),
  keys.tally.department(),
  keys.rank.department(),
  keys.rank.municipality(municipio),
  keys.reportedCount.puesto(puesto),
  keys.reportedCount.municipality(municipio),
  keys.reportedCount.department(),
  keys.dirtyTopics(),
  keys.queue(),
  keys.case(actaId),
  keys.lock(actaId),
  keys.casesIndex(),
];
