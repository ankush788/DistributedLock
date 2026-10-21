-- ============================================================================
-- 4. REVOKE SCRIPT (revoke_lock.lua)
-- ============================================================================
-- SINGLE PURPOSE:
-- Detects if the token is currently holding the active lock or waiting in queue.
-- If holding lock: returns 1 so Java LockService can call releaseLock() internally!
-- If in queue: removes the entry from the queue and returns 2.
-- If neither: returns 0.
--
-- KEYS[1]: lock key (e.g. "lock:order-123")
-- KEYS[2]: queue key (e.g. "lock:queue:order-123")
-- ARGV[1]: tokenId to revoke
--
-- RETURNS:
--   1 -> HOLDING_LOCK (Active lock owner; Java will call release service)
--   2 -> REMOVED_FROM_QUEUE (Deleted from queue list)
--   0 -> NOT_FOUND (Token neither holds the lock nor exists in queue)
-- ============================================================================

-- 1. Check if token currently holds the active lock
local currentOwner = redis.call('GET', KEYS[1])
if currentOwner == ARGV[1] then
    return 1
end

-- 2. If not owner, search the FIFO queue list
local queueLen = redis.call('LLEN', KEYS[2])
for i = 0, queueLen - 1 do
    local entry = redis.call('LINDEX', KEYS[2], i)
    if entry then
        local token = string.match(entry, "([^#]+)") or entry
        if token == ARGV[1] then
            redis.call('LREM', KEYS[2], 1, entry)
            return 2
        end
    end
end

-- 3. Not holding lock and not found in queue
return 0
