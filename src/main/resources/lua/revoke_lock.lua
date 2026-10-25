-- ============================================================================
-- 4. REVOKE SCRIPT (revoke_lock.lua)
-- ============================================================================
-- SINGLE PURPOSE:
-- Detects if the clientId is currently holding the active lock or waiting in queue.
-- If holding lock: returns 1 so Java LockService can call releaseLock() internally.
-- If in queue: removes the clientId from the queue list, deletes its waiter alarm key, and returns 2.
-- If neither: returns 0.
--
-- KEYS[1]: lock key (e.g. "lock:order-123")
-- KEYS[2]: queue key (e.g. "lock:queue:order-123")
-- KEYS[3]: waiter alarm key (e.g. "lock:waiter:order-123:client-1")
-- ARGV[1]: clientId to revoke
--
-- RETURNS:
--   1 -> HOLDING_LOCK (Active lock owner; Java will call release service)
--   2 -> REMOVED_FROM_QUEUE (Deleted from queue list and alarm cancelled)
--   0 -> NOT_FOUND (Client neither holds the lock nor exists in queue)
-- ============================================================================

-- 1. Check if clientId currently holds the active lock
local currentOwner = redis.call('GET', KEYS[1])
if currentOwner == ARGV[1] then
    return 1
end

-- 2. Try removing the clientId from the FIFO queue list
local removed = redis.call('LREM', KEYS[2], 1, ARGV[1])

-- 3. Delete its waiter alarm key (cancel alarm timer)
redis.call('DEL', KEYS[3])

if removed > 0 then
    return 2
end

return 0
