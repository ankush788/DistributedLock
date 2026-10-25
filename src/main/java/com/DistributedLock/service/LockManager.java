package com.DistributedLock.service;

import com.DistributedLock.repository.RedisLockStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Domain-level coordinator executing atomic operations against RedisLockStore.
 */
@Slf4j
@Service
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
    public boolean acquireDirect(String lockName, String clientId) {
        return redisLockStore.acquireDirect(lockName, clientId, defaultLockTimeoutSeconds);
    }

    /**
     * 2. Places request in FIFO queue and arms waiter alarm key.
     * Returns: 1 = newly enqueued, 2 = already queued, 0 = failed.
     */
    public int enqueue(String lockName, String clientId) {
        long queueTimeoutSeconds = Math.max(1, queueTimeoutMillis / 1000);
        long queueSafetyTtlSeconds = Math.max(1, queueSafetyTtlMs / 1000);
        return redisLockStore.enqueue(lockName, clientId, queueTimeoutSeconds, queueSafetyTtlSeconds);
    }

    /**
     * 3. Releases active lock verifying clientId matches current holder.
     */
    public boolean release(String lockName, String clientId) {
        return redisLockStore.release(lockName, clientId);
    }

    /**
     * 4. Revoke check: 1 = holds lock, 2 = removed from queue, 0 = not found.
     */
    public int revoke(String lockName, String clientId) {
        return redisLockStore.revoke(lockName, clientId);
    }

    /**
     * 5. Handover engine: Hands over freed lock to first waiter in queue (O(1)).
     * Returns winning clientId, or empty string if none.
     */
    public String queueLockAcquire(String lockName) {
        return redisLockStore.queueLockAcquire(lockName, defaultLockTimeoutSeconds);
    }

    /**
     * Cancels / deletes the waiter alarm key.
     */
    public void deleteWaiterKey(String lockName, String clientId) {
        redisLockStore.deleteWaiterKey(lockName, clientId);
    }

    /**
     * Removes client directly from the queue list.
     */
    public void removeFromQueue(String lockName, String clientId) {
        redisLockStore.removeFromQueue(lockName, clientId);
    }

    /**
     * Gets the current lock holder directly from Redis.
     */
    public String getLockHolder(String lockName) {
        return redisLockStore.getLockHolder(lockName);
    }

    /**
     * Checks if a client is currently present in the Redis FIFO queue.
     */
    public boolean isClientInQueue(String lockName, String clientId) {
        return redisLockStore.isClientInQueue(lockName, clientId);
    }

    /**
     * Checks if lock is currently held in Redis.
     */
    public boolean isLocked(String lockName) {
        return redisLockStore.isLocked(lockName);
    }
}
