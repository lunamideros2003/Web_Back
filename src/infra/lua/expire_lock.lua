--[[
  Atomically returns an abandoned case to the queue. The EXISTS check and the
  requeue happen together, so a lock renewed in the meantime is never stolen.

  KEYS: 1 lock, 2 queue, 3 case hash, 4 lock expiry index, 5 active cases,
        6 previous owner locks hash
  ARGV: 1 actaId, 2 queue score, 3 now ms
  Returns: 1 requeued, 0 lock still alive
]]

if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end

redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[5], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[3], 'status', 'pending', 'updatedAt', ARGV[3])
redis.call('HDEL', KEYS[3], 'owner', 'ownerToken', 'claimedAt')
redis.call('HDEL', KEYS[6], ARGV[1])
return 1
