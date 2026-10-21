-- ============================================================================
-- 1. DIRECT ACQUIRE SCRIPT (acquire_lock.lua)
-- ============================================================================
-- SINGLE PURPOSE:
-- Attempts direct lock acquisition ONLY if the lock is currently free AND
-- the FIFO queue has no waiting requests. Does not touch or modify the queue.
--
-- KEYS[1]: lock key (e.g. "lock:order-123")
-- KEYS[2]: queue key (e.g. "lock:queue:order-123")
-- ARGV[1]: tokenId (pure unique ownership token)
-- ARGV[2]: lockTimeoutSeconds (TTL for the lock key)
--
-- RETURNS:
--   1 -> ACQUIRED (Lock was free and queue was empty; acquired with pure tokenId)
--   0 -> NOT_ACQUIRED (Lock is occupied or queue has waiters ahead)
-- ============================================================================

local lockExists = redis.call('EXISTS', KEYS[1])
local queueLen = 0

if redis.call('EXISTS', KEYS[2]) == 1 then
    queueLen = redis.call('LLEN', KEYS[2])
end

if lockExists == 0 and queueLen == 0 then
    -- Lock is free and line is empty -> Acquire immediately!
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    return 1
end

return 0
