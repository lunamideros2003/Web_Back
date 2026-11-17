--[[
  Fixed one-minute window rate limit per credential.

  KEYS: 1 counter key
  ARGV: 1 limit, 2 window ms
  Returns: { allowed, retryAfterMs }
]]

local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end

if count > tonumber(ARGV[1]) then
  local ttl = redis.call('PTTL', KEYS[1])
  if ttl < 0 then ttl = tonumber(ARGV[2]) end
  return { 0, ttl }
end

return { 1, 0 }
