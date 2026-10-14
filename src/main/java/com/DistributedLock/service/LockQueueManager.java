package com.DistributedLock.service;

import com.DistributedLock.exception.LockException;
import com.DistributedLock.repository.RedisLockStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Manages fair FIFO queuing for lock requests using Redis Lists.
 * Stores combined "tokenId#timestamp" entries in a single Redis list, eliminating secondary timestamp keys.
 */
@Slf4j
@Service
public class LockQueueManager {

    private final StringRedisTemplate redisTemplate;
    private final RedisLockStore redisLockStore;

    public LockQueueManager(StringRedisTemplate redisTemplate, RedisLockStore redisLockStore) {
        this.redisTemplate = redisTemplate;
        this.redisLockStore = redisLockStore;
    }

    @Value("${distributed-lock.default-timeout-seconds:30}")
    private long defaultLockTimeoutSeconds;

    @Value("${distributed-lock.default-queue-timeout-seconds:10}")
    private long defaultQueueTimeoutSeconds;

    @Value("${distributed-lock.queue-safety-ttl-hours:24}")
    private long queueSafetyTtlHours;

    @Value("${distributed-lock.max-acquire-attempts:3}")
    private int maxAcquireAttempts;

    @Value("${distributed-lock.retry-backoff-ms:100}")
    private long retryBackoffMs;

    private static final String QUEUE_KEY_PREFIX = "lock:queue:";
    private static final String DELIMITER = "#";

    /**
     * Enqueues a combined "tokenId#timestamp" entry into the FIFO queue list.
     *
     * @param lockName name of the resource
     * @param tokenId  the unique token for this request
     * @return the combined queue entry string stored in Redis
     */
    public String enqueue(String lockName, String tokenId) {
        String queueKey = formatQueueKey(lockName);
        String queueEntry = tokenId + DELIMITER + System.currentTimeMillis();

        try {
            redisTemplate.opsForList().rightPush(queueKey, queueEntry);
            redisTemplate.expire(queueKey, queueSafetyTtlHours, TimeUnit.HOURS);
            log.info("Enqueued combined entry '{}' for lock '{}'", queueEntry, lockName);
            return queueEntry;
        } catch (Exception ex) {
            log.error("Failed to enqueue entry '{}' for lock '{}': {}", queueEntry, lockName, ex.getMessage(), ex);
            return queueEntry;
        }
    }

    /**
     * Attempts to acquire lock with bounded retries.
     * Separates the combined queue entry into tokenId and enqueue timestamp,
     * checks the timeout deadline, and calls atomic acquire with the clean tokenId.
     *
     * @param lockName   name of the resource
     * @param queueEntry combined "tokenId#timestamp" entry
     * @return true if acquired within bounded attempts, false if still waiting in queue
     */
    public boolean tryAcquireWithBoundedRetries(String lockName, String queueEntry) {
        // Separate the combined value into clean tokenId and enqueue timestamp
        String[] parts = queueEntry.split(DELIMITER);
        String tokenId = parts[0];
        long enqueueTime = (parts.length > 1) ? Long.parseLong(parts[1]) : System.currentTimeMillis();

        // 1. Check if deadline has already been exceeded
        if (isQueueTimeoutExceeded(enqueueTime)) {
            removeFromQueue(lockName, queueEntry);
            log.warn("Token '{}' exceeded queue timeout of {}s for lock '{}'", tokenId, defaultQueueTimeoutSeconds, lockName);
            throw new LockException.Timeout(lockName, defaultQueueTimeoutSeconds);
        }

        // 2. Perform up to configured bounded attempts
        for (int attempt = 1; attempt <= maxAcquireAttempts; attempt++) {
            if (isQueueTimeoutExceeded(enqueueTime)) {
                removeFromQueue(lockName, queueEntry);
                throw new LockException.Timeout(lockName, defaultQueueTimeoutSeconds);
            }

            // Atomic Lua acquire using clean tokenId
            boolean acquired = redisLockStore.acquireLock(lockName, tokenId, defaultLockTimeoutSeconds);
            if (acquired) {
                log.info("Token '{}' successfully acquired lock '{}' on attempt {}", tokenId, lockName, attempt);
                return true;
            }

            // Brief backoff between bounded attempts
            if (attempt < maxAcquireAttempts) {
                try {
                    Thread.sleep(retryBackoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    removeFromQueue(lockName, queueEntry);
                    throw new LockException.AcquisitionFailed(lockName, "Interrupted while waiting in queue");
                }
            }
        }

        log.debug("Token '{}' did not acquire lock '{}' after {} bounded attempts (remains in queue)",
                tokenId, lockName, maxAcquireAttempts);
        return false;
    }

    /**
     * Checks if elapsed time exceeds defaultQueueTimeoutSeconds using pure in-memory math.
     * Zero Redis round-trips needed!
     */
    public boolean isQueueTimeoutExceeded(long enqueueTime) {
        long elapsedSeconds = (System.currentTimeMillis() - enqueueTime) / 1000;
        return elapsedSeconds > defaultQueueTimeoutSeconds;
    }

    /**
     * Removes a combined entry from the queue list.
     *
     * @param lockName   name of the resource
     * @param queueEntry combined "tokenId#timestamp" entry to remove
     * @return true if removed, false otherwise
     */
    public boolean removeFromQueue(String lockName, String queueEntry) {
        String queueKey = formatQueueKey(lockName);
        try {
            Long removed = redisTemplate.opsForList().remove(queueKey, 1, queueEntry);
            boolean success = removed != null && removed > 0;
            if (success) {
                log.info("Removed entry '{}' from queue for lock '{}'", queueEntry, lockName);
            }
            return success;
        } catch (Exception ex) {
            log.error("Failed to remove entry '{}' from queue for lock '{}': {}", queueEntry, lockName, ex.getMessage(), ex);
            return false;
        }
    }

    private String formatQueueKey(String lockName) {
        return QUEUE_KEY_PREFIX + lockName;
    }
}
