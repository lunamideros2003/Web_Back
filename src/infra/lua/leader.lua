--[[
  Single publisher election. Only one of the eight instances aggregates and
  publishes a window, so clients never receive the same delta twice.

  KEYS: 1 leader key
  ARGV: 1 instanceId, 2 ttl ms
  Returns: 1 leader, 0 follower
]]

local current = redis.call('GET', KEYS[1])

if not current then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end

if current == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end

return 0
