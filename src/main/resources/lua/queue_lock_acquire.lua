-- ============================================================================
-- 5. QUEUE LOCK ACQUIRE / HANDOVER ENGINE (queue_lock_acquire.lua)
-- ============================================================================
-- SINGLE PURPOSE:
-- Checks if the lock is currently free. If free, purges any dead/timed-out
-- requests at the head of the FIFO queue. If a valid, fresh waiter is found,
-- pops it from the queue, sets the active lock with pure tokenId, and returns
-- that winning tokenId to Java so the server can notify its SSE stream!
--
-- KEYS[1]: lock key (e.g. "lock:order-123")
-- KEYS[2]: queue key (e.g. "lock:queue:order-123")
-- ARGV[1]: lockTimeoutSeconds (TTL for newly granted lock)
-- ARGV[2]: queueTimeoutMillis (maximum wait patience duration in queue)
-- ARGV[3]: currentTimeMillis (current system epoch timestamp)
--
-- RETURNS:
--   string -> The winning tokenId that acquired the lock
--   ""     -> Empty string if lock is occupied or no valid waiters in queue
-- ============================================================================

-- 1. If lock is already occupied, cannot handover!
if redis.call('EXISTS', KEYS[1]) == 1 then
    return ""
end

local currentEpoch = tonumber(ARGV[3])
local queueTimeout = tonumber(ARGV[2])

-- 2. Purge dead / timed-out waiters from the head of the queue
while true do
    local headEntry = redis.call('LINDEX', KEYS[2], 0)
    if not headEntry then
        return ""
    end

    local headTimestamp = tonumber(string.match(headEntry, "#(%d+)"))
    if headTimestamp and (currentEpoch - headTimestamp > queueTimeout) then
        -- Timed out! Discard expired entry
        redis.call('LPOP', KEYS[2])
    else
        -- Found a fresh, valid waiter at the front of the line!
        local winningToken = string.match(headEntry, "([^#]+)") or headEntry

        -- Pop from queue and grant the lock using pure tokenId
        redis.call('LPOP', KEYS[2])
        redis.call('SET', KEYS[1], winningToken, 'EX', ARGV[1])

        return winningToken
    end
end

return ""
