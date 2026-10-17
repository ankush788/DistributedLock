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

    public LockManager getLockManager() {
        return lockManager;
    }
}
