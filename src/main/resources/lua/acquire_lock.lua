-- KEYS[1]: lock key (e.g. "lock:inventory-123")
-- KEYS[2]: queue key (e.g. "lock:queue:inventory-123")
-- ARGV[1]: tokenId (e.g. "uuid-token")
-- ARGV[2]: timeoutSeconds (TTL in seconds)

-- 1. Check if FIFO queue has waiting requests (prevents barging / unfairness)
if redis.call('EXISTS', KEYS[2]) == 1 and redis.call('LLEN', KEYS[2]) > 0 then
    return 0
end

-- 2. Check if lock is already held
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end

-- 3. Atomically acquire lock with TTL expiration
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
