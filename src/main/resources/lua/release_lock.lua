-- KEYS[1]: lock key (e.g. "lock:inventory-123")
-- ARGV[1]: tokenId (expected ownership token)

-- 1. Check if the lock key exists and the token matches the owner
if redis.call('GET', KEYS[1]) == ARGV[1] then
    -- Atomically delete the lock
    return redis.call('DEL', KEYS[1])
else
    -- Lock missing (expired) or token mismatch (unauthorized release)
    return 0
end
