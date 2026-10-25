-- ============================================================================
-- 2. ENQUEUE SCRIPT (enqueue_lock.lua)
-- ============================================================================
-- SINGLE PURPOSE:
-- Appends clientId to the FIFO queue list ONLY if not already present,
-- sets an individual waiter alarm key with queueTimeoutSeconds TTL,
-- and refreshes the queue sliding safety TTL.
--
-- KEYS[1]: queue key (e.g. "lock:queue:order-123")
-- KEYS[2]: waiter alarm key (e.g. "lock:waiter:order-123:client-1")
-- ARGV[1]: clientId (unique client identifier)
-- ARGV[2]: queueTimeoutSeconds (TTL for individual waiter alarm key)
-- ARGV[3]: queueSafetyTtlSeconds (sliding memory cleanup TTL for queue key)
--
-- RETURNS:
--   1 -> ENQUEUED (Successfully placed in FIFO queue and alarm key armed)
--   2 -> ALREADY_QUEUED (Client already in queue; alarm and queue TTLs refreshed)
-- ============================================================================

-- 1. Check if clientId already exists in the queue list
local entries = redis.call('LRANGE', KEYS[1], 0, -1)
for _, entry in ipairs(entries) do
    if entry == ARGV[1] then
        -- Already present: refresh alarm key TTL and queue key TTL
        redis.call('EXPIRE', KEYS[2], ARGV[2])
        redis.call('EXPIRE', KEYS[1], ARGV[3])
        return 2
    end
end

-- 2. Append clientId to FIFO queue tail (O(1))
redis.call('RPUSH', KEYS[1], ARGV[1])

-- 3. Set the individual waiter alarm key with TTL (fires keyspace event on timeout)
redis.call('SET', KEYS[2], "", 'EX', ARGV[2])

-- 4. Refresh sliding safety TTL on queue list
redis.call('EXPIRE', KEYS[1], ARGV[3])

return 1
