package com.DistributedLock.service;

import com.DistributedLock.repository.RedisLockStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * LockManager manages low-level atomic acquire, release, queueing, and next-waiter signalling.
 */
@Slf4j
@Component
public class LockManager {

    private final StringRedisTemplate redisTemplate;
    private final RedisLockStore redisLockStore;

    @Value("${distributed-lock.default-timeout-seconds:30}")
    private long defaultLockTimeoutSeconds;

    @Value("${distributed-lock.default-queue-timeout-seconds:10}")
    private long defaultQueueTimeoutSeconds;

    @Value("${distributed-lock.queue-safety-ttl-ms:86400000}")
    private long queueSafetyTtlMs;

    private static final String QUEUE_KEY_PREFIX = "lock:queue:";
    private static final String NOTIFY_CHANNEL_PREFIX = "lock:notify:";
    private static final String DELIMITER = "#";

    public LockManager(StringRedisTemplate redisTemplate, RedisLockStore redisLockStore) {
        this.redisTemplate = redisTemplate;
        this.redisLockStore = redisLockStore;
    }

    /**
     * Attempts atomic lock acquisition in Redis.
     *
     * @param lockName resource name
     * @param tokenId  ownership token
     * @return true if acquired, false if held or another request is head of queue
     */
    public boolean acquire(String lockName, String tokenId) {
        return redisLockStore.acquireLock(lockName, tokenId, defaultLockTimeoutSeconds);
    }

    /**
     * Atomically releases lock verifying ownership token.
     *
     * @param lockName resource name
     * @param tokenId  ownership token
     * @return true if released, false if token mismatch or already expired
     */
    public boolean release(String lockName, String tokenId) {
        return redisLockStore.releaseLock(lockName, tokenId);
    }

    /**
     * Enqueues request into the fair FIFO queue in Redis.
     *
     * @param lockName resource name
     * @param tokenId  ownership token
     * @return combined queue entry string stored in Redis
     */
    public String enqueue(String lockName, String tokenId) {
        String queueKey = formatQueueKey(lockName);
        String queueEntry = tokenId + DELIMITER + System.currentTimeMillis();

        try {
            redisTemplate.opsForList().rightPush(queueKey, queueEntry);
            redisTemplate.expire(queueKey, queueSafetyTtlMs, TimeUnit.MILLISECONDS);
            log.info("LockManager: Enqueued entry '{}' into queue for lock '{}'", queueEntry, lockName);
            return queueEntry;
        } catch (Exception ex) {
            log.error("LockManager: Failed to enqueue entry '{}' for lock '{}': {}", queueEntry, lockName, ex.getMessage(), ex);
            return queueEntry;
        }
    }

    /**
     * Signals the specific next waiting request at the head of the queue (if present) via Redis Pub/Sub.
     * Targeted unicast notification wakes up only the specific next-in-line token (eliminating thundering herd).
     *
     * @param lockName resource name
     */
    public void signalNext(String lockName) {
        String queueKey = formatQueueKey(lockName);
        try {
            // Peek at the very first waiter in line (Index 0 of FIFO queue)
            String headEntry = redisTemplate.opsForList().index(queueKey, 0);
            if (headEntry != null && !headEntry.isBlank()) {
                String nextTokenId = headEntry.split(DELIMITER)[0];
                String targetedChannel = formatNotifyChannel(lockName, nextTokenId);

                // Wake up ONLY this specific next token
                redisTemplate.convertAndSend(targetedChannel, "ACQUIRE_NOW"); 
                // What it does: Sends the message "ACQUIRE_NOW" directly to that specific token's channel (targetedChannel).
                //What happens next: The waiting service listening on that channel receives the message instantly (sub-millisecond) and proceeds to acquire the lock.
                log.info("LockManager: Targeted signal sent to next-in-line token '{}' on channel '{}'", nextTokenId, targetedChannel);
            } else {
                log.debug("LockManager: No waiting requests in queue for lock '{}' to signal", lockName);
            }
        } catch (Exception ex) {
            log.error("LockManager: Failed to signal next request for lock '{}': {}", lockName, ex.getMessage(), ex);
        }
    }

    /**
     * Checks if lock is currently held in Redis.
     */
    public boolean isLocked(String lockName) {
        return redisLockStore.isLocked(lockName);
    }

    /**
     * Checks if elapsed time exceeds defaultQueueTimeoutSeconds using pure in-memory calculation.
     */
    public boolean isQueueTimeoutExceeded(long enqueueTime) {
        long elapsedSeconds = (System.currentTimeMillis() - enqueueTime) / 1000;
        return elapsedSeconds > defaultQueueTimeoutSeconds;
    }

    /**
     * Gets the current owner tokenId of the lock from Redis, or null if not held.
     */
    public String getLockOwner(String lockName) {
        return redisTemplate.opsForValue().get(redisLockStore.formatLockKey(lockName));
    }

    /**
     * Searches for a tokenId in the FIFO queue and removes it.
     * If the removed token was at the head of the queue (index 0) and the lock is free,
     * signals the next waiter.
     *
     * @param lockName resource name
     * @param tokenId  ownership token to remove
     * @return true if found and removed from queue, false otherwise
     */
    public boolean removeTokenFromQueue(String lockName, String tokenId) {
        String queueKey = formatQueueKey(lockName);
        try {
            java.util.List<String> entries = redisTemplate.opsForList().range(queueKey, 0, -1);
            if (entries == null || entries.isEmpty()) {
                return false;
            }

            for (int i = 0; i < entries.size(); i++) {
                String entry = entries.get(i);
                if (entry.startsWith(tokenId + DELIMITER)) {
                    Long removed = redisTemplate.opsForList().remove(queueKey, 1, entry);
                    boolean isRemoved = removed != null && removed > 0;
                    if (isRemoved) {
                        log.info("LockManager: Revoked entry '{}' at position {} from queue for lock '{}'", entry, i, lockName);
                        // If it was at index 0 and lock is currently free, wake up the new head
                        if (i == 0 && !isLocked(lockName)) {
                            signalNext(lockName);
                        }
                    }
                    return isRemoved;
                }
            }
            return false;
        } catch (Exception ex) {
            log.error("LockManager: Failed to remove token '{}' from queue for lock '{}': {}", tokenId, lockName, ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * Removes a combined entry from the queue list.
     */
    public boolean removeFromQueue(String lockName, String queueEntry) {
        String queueKey = formatQueueKey(lockName);
        try {
            Long removed = redisTemplate.opsForList().remove(queueKey, 1, queueEntry);
            return removed != null && removed > 0;
        } catch (Exception ex) {
            log.error("LockManager: Failed to remove entry '{}' from queue for lock '{}': {}", queueEntry, lockName, ex.getMessage(), ex);
            return false;
        }
    }

    public String formatQueueKey(String lockName) {
        return QUEUE_KEY_PREFIX + lockName;
    }

    public String formatNotifyChannel(String lockName, String tokenId) {
        return NOTIFY_CHANNEL_PREFIX + lockName + ":" + tokenId;
    }

    public String formatNotifyChannel(String lockName) {
        return NOTIFY_CHANNEL_PREFIX + lockName;
    }
}
