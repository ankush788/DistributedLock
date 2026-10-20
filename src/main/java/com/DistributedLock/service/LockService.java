package com.DistributedLock.service;

import com.DistributedLock.dto.LockResponseDto;
import com.DistributedLock.dto.LockResponseDto.LockStatus;
import com.DistributedLock.exception.LockException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Service coordinating lock operations with LockManager.
 */
@Slf4j
@Service
public class LockService {

    private final LockManager lockManager;

    public LockService(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    /**
     * Tries to acquire lock from LockManager.
     * If fail -> puts in queue.
     *
     * @param lockName name of the shared resource to lock
     * @return LockResponseDto with token and status (ACQUIRED or QUEUED)
     */
    public LockResponseDto acquireLock(String lockName) {
        if (lockName == null || lockName.isBlank()) {
            throw new IllegalArgumentException("Lock name must not be blank");
        }

        String tokenId = UUID.randomUUID().toString();
        log.info("LockService: Attempting acquire for resource '{}' [token={}]", lockName, tokenId);

        // 1. Try to acquire lock from lock manager
        boolean acquired = lockManager.acquire(lockName, tokenId);
        if (acquired) {
            log.info("LockService: Lock acquired successfully for '{}' [token={}]", lockName, tokenId);
            return new LockResponseDto(
                    lockName,
                    tokenId,
                    LockStatus.ACQUIRED,
                    null,
                    "Lock acquired successfully"
            );
        }

        // 2. Fail -> Put in queue
        log.info("LockService: Lock '{}' occupied. Putting in queue [token={}]", lockName, tokenId);
        lockManager.enqueue(lockName, tokenId);

        return new LockResponseDto(
                lockName,
                tokenId,
                LockStatus.QUEUED,
                null,
                "Lock is occupied; request placed in FIFO queue"
        );
    }

    /**
     * Removes current lock request and signals the next waiting request if present.
     *
     * @param lockName name of the locked resource
     * @param tokenId  ownership token proving caller owns the lock
     * @return LockResponseDto with RELEASED status
     */
    public LockResponseDto releaseLock(String lockName, String tokenId) {
        if (lockName == null || lockName.isBlank()) {
            throw new IllegalArgumentException("Lock name must not be blank");
        }
        if (tokenId == null || tokenId.isBlank()) {
            throw new IllegalArgumentException("Token ID must not be blank");
        }

        log.info("LockService: Releasing lock for resource '{}' [token={}]", lockName, tokenId);

        // 1. Remove current request (release lock)
        boolean released = lockManager.release(lockName, tokenId);

        if (!released) {
            if (lockManager.isLocked(lockName)) {
                log.warn("LockService: Unauthorized release attempt on lock '{}' with invalid token '{}'", lockName, tokenId);
                throw new LockException.UnauthorizedRelease(lockName, tokenId);
            } else {
                log.warn("LockService: Release failed for lock '{}': lock does not exist or has expired", lockName);
                throw new LockException.NotFound(lockName);
            }
        }

        // 2. Signal next request if present
        lockManager.signalNext(lockName);

        log.info("LockService: Lock '{}' released successfully and signaled next request [token={}]", lockName, tokenId);
        return new LockResponseDto(
                lockName,
                tokenId,
                LockStatus.RELEASED,
                null,
                "Lock released successfully"
        );
    }

    /**
     * Revokes a request:
     * 1. If currently held (acquired), releases lock and signals next in queue.
     * 2. If queued, removes the request from the queue.
     * 3. Otherwise, returns not found / failed.
     *
     * @param lockName name of the locked resource
     * @param tokenId  ownership token to revoke
     * @return LockResponseDto with the revoke outcome
     */
    public LockResponseDto revokeLock(String lockName, String tokenId) {
        if (lockName == null || lockName.isBlank()) {
            throw new IllegalArgumentException("Lock name must not be blank");
        }
        if (tokenId == null || tokenId.isBlank()) {
            throw new IllegalArgumentException("Token ID must not be blank");
        }

        log.info("LockService: Revoke requested for resource '{}' [token={}]", lockName, tokenId);

        // 1. Check if the lock is currently held by this token
        String currentOwner = lockManager.getLockOwner(lockName);
        if (tokenId.equals(currentOwner)) {
            log.info("LockService: Token '{}' is current holder of '{}'. Releasing active lock.", tokenId, lockName);
            boolean released = lockManager.release(lockName, tokenId);
            if (released) {
                lockManager.signalNext(lockName);
                return new LockResponseDto(
                        lockName,
                        tokenId,
                        LockStatus.RELEASED,
                        null,
                        "Active lock was revoked and released successfully; next waiter signaled"
                );
            }
        }

        // 2. Check if the token is waiting in the FIFO queue
        boolean removedFromQueue = lockManager.removeTokenFromQueue(lockName, tokenId);
        if (removedFromQueue) {
            log.info("LockService: Token '{}' revoked from queue for resource '{}'", tokenId, lockName);
            return new LockResponseDto(
                    lockName,
                    tokenId,
                    LockStatus.REVOKED,
                    null,
                    "Queued request was revoked and removed from the queue successfully"
            );
        }

        // 3. Neither held nor in queue
        log.warn("LockService: Revoke failed. Token '{}' not found for lock '{}'", tokenId, lockName);
        return new LockResponseDto(
                lockName,
                tokenId,
                LockStatus.FAILED,
                null,
                "Request not found: token is neither holding the lock nor waiting in queue"
        );
    }

    public LockManager getLockManager() {
        return lockManager;
    }
}
