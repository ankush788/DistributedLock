package com.DistributedLock.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.Collections;

/**
 * Low-level Redis data access layer executing single-purpose atomic Lua scripts.
 */
@Slf4j
@Repository
public class RedisLockStore {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> acquireLockScript;
    private final DefaultRedisScript<Long> enqueueLockScript;
    private final DefaultRedisScript<Long> releaseLockScript;
    private final DefaultRedisScript<Long> revokeLockScript;
    private final DefaultRedisScript<String> queueLockAcquireScript;

    public RedisLockStore(StringRedisTemplate redisTemplate,
                          DefaultRedisScript<Long> acquireLockScript,
                          DefaultRedisScript<Long> enqueueLockScript,
                          DefaultRedisScript<Long> releaseLockScript,
                          DefaultRedisScript<Long> revokeLockScript,
                          DefaultRedisScript<String> queueLockAcquireScript) {
        this.redisTemplate = redisTemplate;
        this.acquireLockScript = acquireLockScript;
        this.enqueueLockScript = enqueueLockScript;
        this.releaseLockScript = releaseLockScript;
        this.revokeLockScript = revokeLockScript;
        this.queueLockAcquireScript = queueLockAcquireScript;
    }

    public static final String LOCK_KEY_PREFIX = "lock:";
    public static final String QUEUE_KEY_PREFIX = "lock:queue:";
    public static final String WAITER_KEY_PREFIX = "lock:waiter:";

    /**
     * 1. Direct acquire only: Tries to acquire lock if free and queue is empty.
     */
    public boolean acquireDirect(String lockName, String clientId, long timeoutSeconds) {
        String lockKey = formatLockKey(lockName);
        String queueKey = formatQueueKey(lockName);

        try {
            Long result = redisTemplate.execute(
                    acquireLockScript,
                    Arrays.asList(lockKey, queueKey),
                    clientId,
                    String.valueOf(timeoutSeconds)
            );
            return result != null && result == 1L;
        } catch (Exception ex) {
            log.error("RedisLockStore: Error executing acquireLockScript for '{}': {}", lockName, ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * 2. Enqueue only: Appends clientId to queue and arms individual waiter alarm key.
     * Returns: 1 = newly enqueued, 2 = already queued, 0 = failed/error.
     */
    public int enqueue(String lockName, String clientId, long queueTimeoutSeconds, long queueSafetyTtlSeconds) {
        String queueKey = formatQueueKey(lockName);
        String waiterKey = formatWaiterKey(lockName, clientId);

        try {
            Long result = redisTemplate.execute(
                    enqueueLockScript,
                    Arrays.asList(queueKey, waiterKey),
                    clientId,
                    String.valueOf(queueTimeoutSeconds),
                    String.valueOf(queueSafetyTtlSeconds)
            );
            return result != null ? result.intValue() : 0;
        } catch (Exception ex) {
            log.error("RedisLockStore: Error executing enqueueLockScript for '{}': {}", lockName, ex.getMessage(), ex);
            return 0;
        }
    }

    /**
     * 3. Release only: Deletes active lock key if clientId matches holder.
     */
    public boolean release(String lockName, String clientId) {
        String lockKey = formatLockKey(lockName);

        try {
            Long result = redisTemplate.execute(
                    releaseLockScript,
                    Collections.singletonList(lockKey),
                    clientId
            );
            return result != null && result == 1L;
        } catch (Exception ex) {
            log.error("RedisLockStore: Error executing releaseLockScript for '{}': {}", lockName, ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * 4. Revoke only: Checks if holding lock (1), removes from queue and deletes alarm key (2), or not found (0).
     */
    public int revoke(String lockName, String clientId) {
        String lockKey = formatLockKey(lockName);
        String queueKey = formatQueueKey(lockName);
        String waiterKey = formatWaiterKey(lockName, clientId);

        try {
            Long result = redisTemplate.execute(
                    revokeLockScript,
                    Arrays.asList(lockKey, queueKey, waiterKey),
                    clientId
            );
            return result != null ? result.intValue() : 0;
        } catch (Exception ex) {
            log.error("RedisLockStore: Error executing revokeLockScript for '{}': {}", lockName, ex.getMessage(), ex);
            return 0;
        }
    }

    /**
     * 5. Queue lock acquire: Hands over freed lock to first waiter in queue (O(1)).
     * Returns winning clientId, or empty string if none.
     */
    public String queueLockAcquire(String lockName, long lockTimeoutSeconds) {
        String lockKey = formatLockKey(lockName);
        String queueKey = formatQueueKey(lockName);

        try {
            String winningClientId = redisTemplate.execute(
                    queueLockAcquireScript,
                    Arrays.asList(lockKey, queueKey),
                    String.valueOf(lockTimeoutSeconds)
            );
            return winningClientId != null ? winningClientId.trim() : "";
        } catch (Exception ex) {
            log.error("RedisLockStore: Error executing queueLockAcquireScript for '{}': {}", lockName, ex.getMessage(), ex);
            return "";
        }
    }

    /**
     * Deletes the waiter alarm key (e.g. after lock grant or manual cleanup).
     */
    public void deleteWaiterKey(String lockName, String clientId) {
        try {
            redisTemplate.delete(formatWaiterKey(lockName, clientId));
        } catch (Exception ex) {
            log.warn("RedisLockStore: Could not delete waiter key for '{}:{}': {}", lockName, clientId, ex.getMessage());
        }
    }

    /**
     * Removes a client directly from the Redis queue list (used on waiter timeout).
     */
    public void removeFromQueue(String lockName, String clientId) {
        try {
            redisTemplate.opsForList().remove(formatQueueKey(lockName), 1, clientId);
        } catch (Exception ex) {
            log.warn("RedisLockStore: Could not remove client '{}' from queue for '{}': {}", clientId, lockName, ex.getMessage());
        }
    }

    /**
     * Retrieves the current holder clientId from Redis, or null if lock key does not exist.
     */
    public String getLockHolder(String lockName) {
        try {
            return redisTemplate.opsForValue().get(formatLockKey(lockName));
        } catch (Exception ex) {
            log.warn("RedisLockStore: Could not get lock holder for '{}': {}", lockName, ex.getMessage());
            return null;
        }
    }

    /**
     * Checks if a client is currently present in the Redis FIFO queue.
     */
    public boolean isClientInQueue(String lockName, String clientId) {
        try {
            var queue = redisTemplate.opsForList().range(formatQueueKey(lockName), 0, -1);
            return queue != null && queue.contains(clientId);
        } catch (Exception ex) {
            log.warn("RedisLockStore: Could not check queue for '{}:{}': {}", lockName, clientId, ex.getMessage());
            return false;
        }
    }

    /**
     * Checks if a lock currently exists in Redis.
     */
    public boolean isLocked(String lockName) {
        Boolean exists = redisTemplate.hasKey(formatLockKey(lockName));
        return Boolean.TRUE.equals(exists);
    }

    public String formatLockKey(String lockName) {
        return LOCK_KEY_PREFIX + lockName;
    }

    public String formatQueueKey(String lockName) {
        return QUEUE_KEY_PREFIX + lockName;
    }

    public String formatWaiterKey(String lockName, String clientId) {
        return WAITER_KEY_PREFIX + lockName + ":" + clientId;
    }
}
