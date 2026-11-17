--[[
  Safe release: the token proves ownership. An auditor whose lease expired and
  was reassigned cannot release (or complete) the new owner's lock.

  KEYS: 1 lock, 2 auditor locks hash, 3 queue, 4 case hash, 5 lock expiry index,
        6 active cases
  ARGV: 1 token, 2 actaId, 3 requeue "1"|"0", 4 priority score, 5 auditorId,
        6 resolution note
  Returns: 1 released and requeued, 2 released and resolved, 0 not the owner
]]

local token = ARGV[1]
local actaId = ARGV[2]

if redis.call('GET', KEYS[1]) ~= token then
  return 0
end

redis.call('DEL', KEYS[1])
redis.call('HDEL', KEYS[2], actaId)
redis.call('ZREM', KEYS[5], actaId)
redis.call('ZREM', KEYS[6], actaId)
redis.call('HDEL', KEYS[4], 'owner', 'ownerToken')

if ARGV[3] == '1' then
  redis.call('ZADD', KEYS[3], ARGV[4], actaId)
  redis.call('HSET', KEYS[4], 'status', 'pending', 'updatedAt', ARGV[5])
  return 1
end

redis.call('ZREM', KEYS[3], actaId)
redis.call('HSET', KEYS[4],
  'status', 'resolved',
  'resolvedAt', ARGV[5],
  'resolution', ARGV[6],
  'updatedAt', ARGV[5]
)
return 2
