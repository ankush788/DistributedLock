-- ============================================================================
-- 5. QUEUE LOCK ACQUIRE / HANDOVER ENGINE (queue_lock_acquire.lua)
-- ============================================================================
-- SINGLE PURPOSE:
-- Checks if the lock is currently free. If free, pops the next waiting client
-- from the head of the FIFO queue (O(1)), atomically sets the active lock with
-- lockTimeoutSeconds TTL, and returns that winning clientId to Java!
--
-- KEYS[1]: lock key (e.g. "lock:order-123")
-- KEYS[2]: queue key (e.g. "lock:queue:order-123")
-- ARGV[1]: lockTimeoutSeconds (TTL for newly granted lock)
--
-- RETURNS:
--   string -> The winning clientId that acquired the lock
--   ""     -> Empty string if lock is occupied or no waiters in queue
-- ============================================================================

-- 1. If lock is already occupied, cannot handover!
if redis.call('EXISTS', KEYS[1]) == 1 then
    return ""
end

-- 2. Pop the front waiter from the FIFO queue (O(1))
local winningClientId = redis.call('LPOP', KEYS[2])

if winningClientId and winningClientId ~= "" then
    -- 3. Atomically grant the lock to the winner with lockTimeoutSeconds TTL
    redis.call('SET', KEYS[1], winningClientId, 'EX', ARGV[1])
    return winningClientId
end

return ""
