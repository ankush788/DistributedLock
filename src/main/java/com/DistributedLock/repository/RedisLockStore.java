package com.DistributedLock.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Low-level Redis Store executing the 5 single-purpose Lua scripts.
 */
@Slf4j
@Component
public class RedisLockStore {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<Long> acquireLockScript;
    private final RedisScript<Long> enqueueLockScript;
    private final RedisScript<Long> releaseLockScript;
    private final RedisScript<Long> revokeLockScript;
    private final RedisScript<String> queueLockAcquireScript;

    public RedisLockStore(StringRedisTemplate redisTemplate,
                          RedisScript<Long> acquireLockScript,
                          RedisScript<Long> enqueueLockScript,
                          RedisScript<Long> releaseLockScript,
                          RedisScript<Long> revokeLockScript,
                          RedisScript<String> queueLockAcquireScript) {
        this.redisTemplate = redisTemplate;
        this.acquireLockScript = acquireLockScript;
        this.enqueueLockScript = enqueueLockScript;
        this.releaseLockScript = releaseLockScript;
        this.revokeLockScript = revokeLockScript;
        this.queueLockAcquireScript = queueLockAcquireScript;
    }

    public static final String LOCK_KEY_PREFIX = "lock:";
    public static final String QUEUE_KEY_PREFIX = "lock:queue:";

    /**
     * 1. Direct acquire only: Tries to acquire lock if free and queue is empty.
     */
    public boolean acquireDirect(String lockName, String tokenId, long timeoutSeconds) {
        String lockKey = formatLockKey(lockName);
        String queueKey = formatQueueKey(lockName);

        try {
            Long result = redisTemplate.execute(
                    acquireLockScript,
                    Arrays.asList(lockKey, queueKey),
                    tokenId,
                    String.valueOf(timeoutSeconds)
            );
            return result != null && result == 1L;
        } catch (Exception ex) {
            log.error("RedisLockStore: Error executing acquireLockScript for '{}': {}", lockName, ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * 2. Enqueue only: Appends tokenId#timestamp to queue list.
     */
    public boolean enqueue(String lockName, String tokenId, long currentTimeMillis, long queueSafetyTtlSeconds) {
        String queueKey = formatQueueKey(lockName);

        try {
            Long result = redisTemplate.execute(
                    enqueueLockScript,
                    Collections.singletonList(queueKey),
                    tokenId,
                    String.valueOf(currentTimeMillis),
                    String.valueOf(queueSafetyTtlSeconds)
            );
            return result != null && result == 1L;
        } catch (Exception ex) {
            log.error("RedisLockStore: Error executing enqueueLockScript for '{}': {}", lockName, ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * 3. Release only: Deletes active lock key if token matches.
     */
    public boolean release(String lockName, String tokenId) {
        String lockKey = formatLockKey(lockName);

        try {
            Long result = redisTemplate.execute(
                    releaseLockScript,
                    Collections.singletonList(lockKey),
                    tokenId
            );
            return result != null && result == 1L;
        } catch (Exception ex) {
            log.error("RedisLockStore: Error executing releaseLockScript for '{}': {}", lockName, ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * 4. Revoke only: Checks if holding lock (1), removes from queue (2), or not found (0).
     */
    public int revoke(String lockName, String tokenId) {
        String lockKey = formatLockKey(lockName);
        String queueKey = formatQueueKey(lockName);

        try {
            Long result = redisTemplate.execute(
                    revokeLockScript,
                    Arrays.asList(lockKey, queueKey),
                    tokenId
            );
            return result != null ? result.intValue() : 0;
        } catch (Exception ex) {
            log.error("RedisLockStore: Error executing revokeLockScript for '{}': {}", lockName, ex.getMessage(), ex);
            return 0;
        }
    }

    /**
     * 5. Queue lock acquire: Hands over freed lock to first fresh waiter in queue.
     * Returns winning tokenId, or empty string if none.
     */
    public String queueLockAcquire(String lockName, long lockTimeoutSeconds, long queueTimeoutMillis, long currentTimeMillis) {
        String lockKey = formatLockKey(lockName);
        String queueKey = formatQueueKey(lockName);

        try {
            String winningToken = redisTemplate.execute(
                    queueLockAcquireScript,
                    Arrays.asList(lockKey, queueKey),
                    String.valueOf(lockTimeoutSeconds),
                    String.valueOf(queueTimeoutMillis),
                    String.valueOf(currentTimeMillis)
            );
            return winningToken != null ? winningToken.trim() : "";
        } catch (Exception ex) {
            log.error("RedisLockStore: Error executing queueLockAcquireScript for '{}': {}", lockName, ex.getMessage(), ex);
            return "";
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
}
