-- KEYS[1]: lock key (e.g. "lock:inventory-123")
-- KEYS[2]: queue key (e.g. "lock:queue:inventory-123")
-- ARGV[1]: tokenId (clean unique ownership token)
-- ARGV[2]: timeoutSeconds (TTL for the lock)

-- 1. If lock is already held, cannot acquire
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end

local queueLen = 0
if redis.call('EXISTS', KEYS[2]) == 1 then
    queueLen = redis.call('LLEN', KEYS[2])
end

-- 2. CASE A: Queue is empty -> Direct Acquire immediately!
if queueLen == 0 then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    return 1
end

-- 3. CASE B: Queue has waiters -> Only the HEAD of the line can acquire!
local headToken = redis.call('LINDEX', KEYS[2], 0)
local cleanHeadToken = string.match(headToken, "([^#]+)") or headToken

if cleanHeadToken == ARGV[1] then
    -- Atomically pop head entry from queue and grant lock using clean tokenId
    redis.call('LPOP', KEYS[2])
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    return 1
end

-- 4. Otherwise: Not this token's turn in line
return 0
