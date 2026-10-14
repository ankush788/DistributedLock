package com.DistributedLock.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Low-level Redis Store handling atomic lock operations using unified Lua scripts.
 */
@Slf4j
@Component
public class RedisLockStore {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<Long> acquireLockScript;
    private final RedisScript<Long> releaseLockScript;

    public RedisLockStore(StringRedisTemplate redisTemplate,
                          RedisScript<Long> acquireLockScript,
                          RedisScript<Long> releaseLockScript) {
        this.redisTemplate = redisTemplate;
        this.acquireLockScript = acquireLockScript;
        this.releaseLockScript = releaseLockScript;
    }

    public static final String LOCK_KEY_PREFIX = "lock:";
    public static final String QUEUE_KEY_PREFIX = "lock:queue:";

    /**
     * Atomically acquires a lock if it is free and either:
     * 1. No queue exists (direct acquire), OR
     * 2. This tokenId is at the head of the FIFO queue (queue handover).
     *
     * @param lockName       name of the resource
     * @param tokenId        generated unique ownership token
     * @param timeoutSeconds lock expiration duration (TTL)
     * @return true if acquired, false if held by another or not at head of queue
     */
    public boolean acquireLock(String lockName, String tokenId, long timeoutSeconds) {
        String lockKey = formatLockKey(lockName);
        String queueKey = formatQueueKey(lockName);

        try {
            Long result = redisTemplate.execute(
                    acquireLockScript,
                    Arrays.asList(lockKey, queueKey),
                    tokenId,
                    String.valueOf(timeoutSeconds)
            );
            boolean acquired = result != null && result == 1L;
            if (acquired) {
                log.info("Lock acquired successfully: key='{}', tokenId='{}', ttl={}s", lockKey, tokenId, timeoutSeconds);
            } else {
                log.debug("Lock acquire rejected (held or not head of queue): key='{}', token='{}'", lockKey, tokenId);
            }
            return acquired;
        } catch (Exception ex) {
            log.error("Error executing atomic acquire script for lock '{}': {}", lockName, ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * Atomically releases the lock only if the provided tokenId matches the owner.
     *
     * @param lockName name of the resource
     * @param tokenId  ownership token to authenticate release
     * @return true if lock was deleted, false if expired or token mismatch
     */
    public boolean releaseLock(String lockName, String tokenId) {
        String lockKey = formatLockKey(lockName);

        try {
            Long result = redisTemplate.execute(
                    releaseLockScript,
                    Collections.singletonList(lockKey),
                    tokenId
            );
            boolean released = result != null && result == 1L;
            if (released) {
                log.info("Lock released successfully: key='{}', tokenId='{}'", lockKey, tokenId);
            } else {
                log.warn("Lock release failed (token mismatch or already expired): key='{}', tokenId='{}'", lockKey, tokenId);
            }
            return released;
        } catch (Exception ex) {
            log.error("Error executing atomic release script for lock '{}': {}", lockName, ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * Checks if a lock currently exists in Redis (safe boolean check, does not expose tokenId).
     */
    public boolean isLocked(String lockName) {
        Boolean exists = redisTemplate.hasKey(formatLockKey(lockName));
        return Boolean.TRUE.equals(exists);
    }

    /**
     * Gets remaining TTL for the lock in seconds (-2 if does not exist, -1 if no TTL).
     */
    public Long getRemainingTtl(String lockName) {
        return redisTemplate.getExpire(formatLockKey(lockName), TimeUnit.SECONDS);
    }

    /**
     * INTERNAL SYSTEM USE ONLY.
     * Package-private: Inaccessible to controllers or external packages.
     * Used exclusively by the DeadlockDetector to terminate circular wait deadlocks.
     */
    boolean internalForceRelease(String lockName) {
        String lockKey = formatLockKey(lockName);
        Boolean deleted = redisTemplate.delete(lockKey);
        log.warn("SYSTEM: Force release executed on lock key='{}', deleted={}", lockKey, deleted);
        return Boolean.TRUE.equals(deleted);
    }

    public String formatLockKey(String lockName) {
        return LOCK_KEY_PREFIX + lockName;
    }

    public String formatQueueKey(String lockName) {
        return QUEUE_KEY_PREFIX + lockName;
    }
}
