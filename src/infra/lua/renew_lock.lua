--[[
  Renews a lock only if the caller still owns it. While an auditor keeps
  sending heartbeats the lease never expires, which resolves the tension
  between "expire abandoned cases" and "never steal an active case".

  KEYS: 1 lock, 2 lock expiry index
  ARGV: 1 token, 2 lease ms, 3 now ms, 4 actaId
  Returns: 1 renewed, 0 not the owner
]]

if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], tonumber(ARGV[3]) + tonumber(ARGV[2]), ARGV[4])
return 1
