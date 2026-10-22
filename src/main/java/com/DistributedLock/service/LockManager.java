package com.DistributedLock.service;

import com.DistributedLock.repository.RedisLockStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * LockManager coordinates the single-purpose Redis lock operations.
 */
@Slf4j
@Component
public class LockManager {

    private final RedisLockStore redisLockStore;

    @Value("${distributed-lock.default-timeout-seconds:30}")
    private long defaultLockTimeoutSeconds;

    @Value("${distributed-lock.queue-timeout-ms:60000}")
    private long queueTimeoutMillis;

    @Value("${distributed-lock.queue-safety-ttl-ms:86400000}")
    private long queueSafetyTtlMs;

    public LockManager(RedisLockStore redisLockStore) {
        this.redisLockStore = redisLockStore;
    }

    /**
     * 1. Attempts direct acquire. Returns true if acquired, false if occupied or queue has waiters.
     */
    public boolean acquireDirect(String lockName, String tokenId) {
        return redisLockStore.acquireDirect(lockName, tokenId, defaultLockTimeoutSeconds);
    }

    /**
     * 2. Places request in FIFO queue.
     */
    public boolean enqueue(String lockName, String tokenId) {
        long queueSafetyTtlSeconds = Math.max(1, queueSafetyTtlMs / 1000);
        return redisLockStore.enqueue(lockName, tokenId, System.currentTimeMillis(), queueSafetyTtlSeconds);
    }

    /**
     * 3. Releases active lock verifying ownership token.
     */
    public boolean release(String lockName, String tokenId) {
        return redisLockStore.release(lockName, tokenId);
    }

    /**
     * 4. Revoke check: 1 = holds lock, 2 = removed from queue, 0 = not found.
     */
    public int revoke(String lockName, String tokenId) {
        return redisLockStore.revoke(lockName, tokenId);
    }

    /**
     * 5. Handover engine: Hands over freed lock to first valid waiter in queue.
     * Returns winning tokenId, or empty string if none.
     */
    public String queueLockAcquire(String lockName) {
        return redisLockStore.queueLockAcquire(
                lockName,
                defaultLockTimeoutSeconds,
                queueTimeoutMillis,
                System.currentTimeMillis()
        );
    }

    /**
     * Checks if lock is currently held in Redis.
     */
    public boolean isLocked(String lockName) {
        return redisLockStore.isLocked(lockName);
    }
}
