--[[
  Claims one verification case for a single auditor.
  The lock and the queue removal happen in the same script so two auditors can
  never take the same acta, even when they are connected to different instances.

  KEYS: 1 queue, 2 lock, 3 case hash, 4 auditor locks hash,
        5 lock expiry index, 6 cases index, 7 active cases
  ARGV: 1 actaId, 2 token, 3 lease ms, 4 now ms, 5 auditorId
  Returns: 1 claimed, 0 already locked, -1 unknown case
]]

local actaId = ARGV[1]
local token = ARGV[2]
local leaseMs = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local auditorId = ARGV[5]

if redis.call('EXISTS', KEYS[2]) == 1 then
  return 0
end

local queued = redis.call('ZSCORE', KEYS[1], actaId)
local known = redis.call('ZSCORE', KEYS[6], actaId)
if not queued and not known then
  return -1
end

redis.call('SET', KEYS[2], token, 'PX', leaseMs)
redis.call('ZREM', KEYS[1], actaId)
redis.call('HSET', KEYS[3],
  'owner', auditorId,
  'ownerToken', token,
  'claimedAt', now,
  'status', 'claimed'
)
redis.call('HSET', KEYS[4], actaId, token)
redis.call('ZADD', KEYS[5], now + leaseMs, actaId)
redis.call('ZADD', KEYS[7], now, actaId)
return 1
