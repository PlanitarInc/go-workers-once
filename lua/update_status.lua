-- KEYS:
--  [1] key of the job descriptor
-- ARGUMENTS:
--  [1] Expected JID
--  [2] New status of the job
--  [3] New expiration time for the job descriptor
--  [4] New last update timestamp (in ms) for the job descriptor
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
val["updated_ms"] = tonumber(ARGV[4])

local valJson = cjson.encode(val)
redis.call("SET", KEYS[1], valJson)
redis.call("EXPIRE", KEYS[1], ARGV[3])
-- Notify the waiters if the job is done
if val["status"] == "ok" or val["status"] == "failed" then
	redis.call("PUBLISH", KEYS[1], valJson)
end
return 0
