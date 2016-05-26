-- KEYS:
--  [1] key of the job descriptor
-- ARGUMENTS:
--  [1] Expected JID
--  [2] New status of the job
--  [3] New expiration time for the job descriptor
--
--  Return values:
--    0  in case of success
--   -1  if the key does not exist
--   -2  if the JID is wrong

local val = redis.call("GET", KEYS[1])

if val == false then
  return -1
end

val = cjson.decode(val)
if val["jid"] ~= ARGV[1] then
  return -2
end

val["status"] = ARGV[2]
redis.call("SET", KEYS[1], cjson.encode(val))
redis.call("EXPIRE", KEYS[1], ARGV[3])
return 0
