-- ============================================================================
-- 3. RELEASE SCRIPT (release_lock.lua)
-- ============================================================================
-- SINGLE PURPOSE:
-- Verifies that the provided clientId matches the active lock holder.
-- If verified, deletes the active lock key. Does not touch the queue.
--
-- KEYS[1]: lock key (e.g. "lock:order-123")
-- ARGV[1]: clientId (unique client identifier attempting release)
--
-- RETURNS:
--   1 -> RELEASED (Lock verified and deleted successfully)
--   0 -> FAILED (Client mismatch or lock already expired)
-- ============================================================================

if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
else
    return 0
end
