--[[
  Applies one version of an acta, atomically.

  Guarantees:
    * Idempotency: a repeated submission returns the original outcome.
    * Monotonic versions: a stale version (v2 after v3) never moves totals.
    * Correction: a newer version reverts the previous one before applying.
    * Four levels (mesa, puesto, municipio, department) plus the reported
      counters and the rankings are updated in the same indivisible step.
    * The acta is flagged for human verification in the same operation.

  KEYS
    1  acta hash                    9  reported count: puesto
    2  idempotency key             10  reported count: municipio
    3  tally: mesa                 11  reported count: department
    4  tally: puesto               12  dirty topics set
    5  tally: municipio            13  verification queue (zset)
    6  tally: department           14  case hash
    7  rank: department (zset)     15  lock key for this acta
    8  rank: municipio (zset)      16  cases index (zset)

  ARGV
    1 actaId              10 topics json
    2 version             11 now (ms)
    3 mesa                12 doubtful "1" | "0"
    4 puesto              13 queue priority
    5 municipio           14 juror id
    6 votes json          15 photo hash
    7 idempotency ttl     16 mismatch reason
    8 vote codes json     17 sufragantes
    9 rank codes json     18 total votes
]]

local actaId = ARGV[1]
local version = tonumber(ARGV[2])
local idemKey = KEYS[2]
local votes = cjson.decode(ARGV[6])
local codes = cjson.decode(ARGV[8])
local rankCodes = cjson.decode(ARGV[9])
local topics = cjson.decode(ARGV[10])
local now = tonumber(ARGV[11])
local doubtful = ARGV[12] == '1'
local priority = tonumber(ARGV[13])
local idemTtl = tonumber(ARGV[7])

-- Idempotency first: a retry must receive the same, unambiguous answer.
local cached = redis.call('GET', idemKey)
if cached then
  local separator = string.find(cached, '|', 1, true)
  if separator then
    local cachedActa = string.sub(cached, 1, separator - 1)
    local cachedVersion = tonumber(string.sub(cached, separator + 1))
    if cachedActa ~= actaId then
      return cjson.encode({ status = 'conflict', version = 0 })
    end
    return cjson.encode({ status = 'duplicate', version = cachedVersion })
  end
end

local storedVersion = tonumber(redis.call('HGET', KEYS[1], 'version'))

if storedVersion and storedVersion >= version then
  redis.call('SET', idemKey, actaId .. '|' .. storedVersion, 'EX', idemTtl)
  return cjson.encode({ status = 'stale', version = storedVersion })
end

local previous = {}
if storedVersion then
  local flat = redis.call('HMGET', KEYS[1], unpack(codes))
  for index, code in ipairs(codes) do
    previous[code] = tonumber(flat[index]) or 0
  end
end

local delta = {}
for _, code in ipairs(codes) do
  local nextValue = tonumber(votes[code]) or 0
  delta[code] = nextValue - (previous[code] or 0)
end

local function applyTo(hashKey)
  for _, code in ipairs(codes) do
    if delta[code] ~= 0 then
      redis.call('HINCRBY', hashKey, code, delta[code])
    end
  end
end

applyTo(KEYS[3])
applyTo(KEYS[4])
applyTo(KEYS[5])
applyTo(KEYS[6])

for _, code in ipairs(rankCodes) do
  if delta[code] ~= 0 then
    redis.call('ZINCRBY', KEYS[7], delta[code], code)
    redis.call('ZINCRBY', KEYS[8], delta[code], code)
  end
end

-- A mesa is counted as reported exactly once, regardless of corrections.
if not storedVersion then
  redis.call('INCR', KEYS[9])
  redis.call('INCR', KEYS[10])
  redis.call('INCR', KEYS[11])
end

local fields = {
  'version', version,
  'mesa', ARGV[3],
  'puesto', ARGV[4],
  'municipio', ARGV[5],
  'juror', ARGV[14],
  'sufragantes', ARGV[17],
  'updated_at', ARGV[11],
}
for _, code in ipairs(codes) do
  fields[#fields + 1] = code
  fields[#fields + 1] = tostring(tonumber(votes[code]) or 0)
end
if ARGV[15] ~= '' then
  fields[#fields + 1] = 'photo_hash'
  fields[#fields + 1] = ARGV[15]
end
redis.call('HSET', KEYS[1], unpack(fields))

for _, topic in ipairs(topics) do
  redis.call('SADD', KEYS[12], topic)
end

if doubtful then
  redis.call(
    'HSET', KEYS[14],
    'acta', actaId,
    'version', version,
    'mesa', ARGV[3],
    'puesto', ARGV[4],
    'municipio', ARGV[5],
    'votes', ARGV[6],
    'sufragantes', ARGV[17],
    'totalVotes', ARGV[18],
    'reason', ARGV[16],
    'priority', ARGV[13],
    'juror', ARGV[14],
    'status', 'pending',
    'updatedAt', ARGV[11]
  )
  redis.call('ZADD', KEYS[13], priority * 1000000000000 + now, actaId)
  redis.call('ZADD', KEYS[16], now, actaId)
elseif storedVersion then
  -- A correction fixed the mismatch. Only leave the queue if no auditor owns it.
  if redis.call('EXISTS', KEYS[15]) == 0 then
    redis.call('ZREM', KEYS[13], actaId)
    redis.call('ZREM', KEYS[16], actaId)
    redis.call('HSET', KEYS[14], 'status', 'resolved', 'updatedAt', ARGV[11])
  end
  redis.call('HSET', KEYS[14], 'votes', ARGV[6], 'sufragantes', ARGV[17], 'totalVotes', ARGV[18], 'updatedAt', ARGV[11])
end

redis.call('SET', idemKey, actaId .. '|' .. version, 'EX', idemTtl)

return cjson.encode({
  status = 'applied',
  version = version,
  doubtful = doubtful,
  reported = (not storedVersion),
  corrected = (storedVersion ~= nil),
})
