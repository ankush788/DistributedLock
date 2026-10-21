-- ============================================================================
-- 2. ENQUEUE SCRIPT (enqueue_lock.lua)
-- ============================================================================
-- SINGLE PURPOSE:
-- Appends a waiting request in the format "tokenId#timestamp" to the end of
-- the FIFO queue list and refreshes the queue sliding safety TTL.
--
-- KEYS[1]: queue key (e.g. "lock:queue:order-123")
-- ARGV[1]: tokenId (pure unique ownership token)
-- ARGV[2]: currentTimeMillis (epoch timestamp when request joined the queue)
-- ARGV[3]: queueSafetyTtlSeconds (sliding memory cleanup TTL for queue key)
--
-- RETURNS:
--   1 -> ENQUEUED (Successfully placed in FIFO queue)
-- ============================================================================

local queueEntry = ARGV[1] .. "#" .. ARGV[2]

-- 1. Append to queue tail (O(1))
redis.call('RPUSH', KEYS[1], queueEntry)

-- 2. Refresh sliding safety TTL
redis.call('EXPIRE', KEYS[1], ARGV[3])

return 1
