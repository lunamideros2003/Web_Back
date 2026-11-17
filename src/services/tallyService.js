import { CANDIDATES, LEVELS, VOTE_CODES, parseTopic } from '../domain/election.js';
import { diffTally, emptyTally } from '../domain/tally.js';
import { keys } from '../infra/keys.js';
import { redis, redisGateway } from '../infra/redis.js';

const locate = (topic) => {
  if (topic.level === LEVELS.DEPARTMENT) {
    return {
      tallyKey: keys.tally.department(),
      rankKey: keys.rank.department(),
      reportedKey: keys.reportedCount.department(),
      totalsField: 'dept',
    };
  }
  if (topic.level === LEVELS.MUNICIPALITY) {
    return {
      tallyKey: keys.tally.municipality(topic.id),
      rankKey: keys.rank.municipality(topic.id),
      reportedKey: keys.reportedCount.municipality(topic.id),
      totalsField: `municipality:${topic.id}`,
    };
  }
  return {
    tallyKey: keys.tally.puesto(topic.id),
    rankKey: null,
    reportedKey: keys.reportedCount.puesto(topic.id),
    totalsField: `puesto:${topic.id}`,
  };
};

const deserialize = (topic, [tallyReply, rankReply, reportedReply, totalReply]) => {
  const rawTally = tallyReply?.[1] ?? {};
  const tally = emptyTally();
  for (const code of VOTE_CODES) {
    tally[code] = Number(rawTally[code] ?? 0);
  }

  const rank = [];
  const rawRank = rankReply?.[1] ?? [];
  for (let index = 0; index < rawRank.length; index += 2) {
    rank.push({ code: rawRank[index], votes: Number(rawRank[index + 1]) });
  }

  const mesas = Number(reportedReply?.[1] ?? 0);
  const total = Number(totalReply?.[1] ?? 0);

  return {
    topic: topic.key,
    level: topic.level,
    id: topic.id,
    tally,
    totalVotes: VOTE_CODES.reduce((sum, code) => sum + tally[code], 0),
    reported: {
      mesas,
      total,
      pct: total > 0 ? Math.round((mesas / total) * 10000) / 100 : 0,
    },
    rank,
    updatedAt: new Date().toISOString(),
  };
};

/**
 * Reads one public topic. Three Redis structures are fetched in a single
 * round trip: the accumulated votes, the ranking and the reported-mesas
 * counters. Reads never scan the acta collection.
 */
export const readTopicState = async (rawTopic) => {
  const topic = typeof rawTopic === 'string' ? parseTopic(rawTopic) : rawTopic;
  const target = locate(topic);

  const pipeline = redis.pipeline();
  pipeline.hgetall(target.tallyKey);
  // Always keep four replies so the deserializer stays positional even when a
  // level has no ranking (puesto level).
  pipeline.zrevrange(target.rankKey ?? keys.root('rank:__none__'), 0, -1, 'WITHSCORES');
  pipeline.get(target.reportedKey);
  pipeline.hget(keys.totals(), target.totalsField);

  const replies = await redisGateway.run(() => pipeline.exec());
  return deserialize(topic, replies);
};

export const readTopicStates = async (topics) => {
  const results = [];
  for (const topic of topics) {
    results.push(await readTopicState(topic));
  }
  return results;
};

/** Minimal delta for the subscribers of one topic. */
export { diffTally };

export const emptyDepartmentState = () => ({
  topic: 'dept',
  level: LEVELS.DEPARTMENT,
  id: 'dept',
  tally: emptyTally(),
  totalVotes: 0,
  reported: { mesas: 0, total: 0, pct: 0 },
  rank: CANDIDATES.map((code) => ({ code, votes: 0 })),
  updatedAt: new Date().toISOString(),
});

export const readActa = async (actaId) => {
  const raw = await redisGateway.run(() => redis.hgetall(keys.acta(actaId)));
  if (!raw || Object.keys(raw).length === 0) return null;
  const votos = {};
  for (const code of VOTE_CODES) votos[code] = Number(raw[code] ?? 0);
  return {
    acta: actaId,
    version: Number(raw.version),
    mesa: Number(raw.mesa),
    puesto: Number(raw.puesto),
    municipio: Number(raw.municipio),
    votos,
    sufragantes: Number(raw.sufragantes ?? 0),
    juror: raw.juror ?? null,
    updatedAt: Number(raw.updated_at ?? 0),
  };
};
