package com.DistributedLock.service;

import com.DistributedLock.dto.LockResponseDto;
import com.DistributedLock.dto.LockResponseDto.LockStatus;
import com.DistributedLock.exception.LockException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service coordinating single-responsibility lock operations and SSE streaming.
 */
@Slf4j
@Service
public class LockService {

    private final LockManager lockManager;
    private final Map<String, SseEmitter> activeEmitters = new ConcurrentHashMap<>();

    public LockService(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    /**
     * 1. ACQUIRE (SSE STREAMING):
     * - Fast path: Direct acquire -> sends ACQUIRED event and CLOSES connection immediately.
     * - Waiting path: Enqueued -> sends QUEUED event and KEEPS connection open at 0% CPU.
     * - Safety: Disconnect/timeout calls revokeLock() internally and closes connection.
     */
    public SseEmitter acquireLock(String lockName) {
        if (lockName == null || lockName.isBlank()) {
            throw new IllegalArgumentException("Lock name must not be blank");
        }

        long queueTimeoutMillis = lockManager.getDefaultQueueTimeoutMillis();
        SseEmitter emitter = new SseEmitter(queueTimeoutMillis);
        String tokenId = UUID.randomUUID().toString();

        log.info("LockService: Acquire requested for resource '{}' [token={}]", lockName, tokenId);

        // FAST PATH: Direct Acquire
        boolean acquired = lockManager.acquireDirect(lockName, tokenId);
        if (acquired) {
            log.info("LockService: Direct acquire succeeded for '{}' [token={}]", lockName, tokenId);
            sendSseEvent(emitter, new LockResponseDto(
                    lockName,
                    tokenId,
                    LockStatus.ACQUIRED,
                    null,
                    "Lock acquired successfully"
            ));
            emitter.complete(); // Connection closes immediately!
            return emitter;
        }

        // WAITING PATH: Enqueue and keep connection open
        log.info("LockService: Lock '{}' occupied. Enqueueing request [token={}]", lockName, tokenId);
        lockManager.enqueue(lockName, tokenId);

        // Store emitter for subsequent handover notification
        activeEmitters.put(tokenId, emitter);

        // Send initial QUEUED event
        sendSseEvent(emitter, new LockResponseDto(
                lockName,
                tokenId,
                LockStatus.QUEUED,
                null,
                "Lock is occupied; request placed in FIFO queue"
        ));

        // SAFETY ON TIMEOUT: Revokes internally and closes
        emitter.onTimeout(() -> {
            log.warn("LockService: SSE timeout for '{}' [token={}]. Revoking internally.", lockName, tokenId);
            activeEmitters.remove(tokenId);
            revokeLock(lockName, tokenId);
            try {
                emitter.complete();
            } catch (Exception ignored) {}
        });

        // SAFETY ON ERROR / CLIENT DISCONNECT: Revokes internally
        emitter.onError((ex) -> {
            log.warn("LockService: SSE disconnect/error for '{}' [token={}]. Revoking internally.", lockName, tokenId);
            activeEmitters.remove(tokenId);
            revokeLock(lockName, tokenId);
        });

        emitter.onCompletion(() -> {
            activeEmitters.remove(tokenId);
        });

        // Immediate check in case the lock was freed in this exact millisecond
        triggerQueueHandover(lockName);

        return emitter;
    }

    /**
     * 2. RELEASE:
     * - Validates token against active lock.
     * - Deletes active lock key.
     * - Triggers queueLockAcquire to hand over to next waiter.
     */
    public LockResponseDto releaseLock(String lockName, String tokenId) {
        if (lockName == null || lockName.isBlank()) {
            throw new IllegalArgumentException("Lock name must not be blank");
        }
        if (tokenId == null || tokenId.isBlank()) {
            throw new IllegalArgumentException("Token ID must not be blank");
        }

        log.info("LockService: Release requested for '{}' [token={}]", lockName, tokenId);

        boolean released = lockManager.release(lockName, tokenId);

        if (!released) {
            if (lockManager.isLocked(lockName)) {
                log.warn("LockService: Unauthorized release on '{}' with token '{}'", lockName, tokenId);
                throw new LockException.UnauthorizedRelease(lockName, tokenId);
            } else {
                log.warn("LockService: Release failed for '{}': lock does not exist or expired", lockName);
                throw new LockException.NotFound(lockName);
            }
        }

        log.info("LockService: Lock '{}' released successfully. Triggering queue handover.", lockName);

        // Hand over lock to the next waiter in line
        triggerQueueHandover(lockName);

        return new LockResponseDto(
                lockName,
                tokenId,
                LockStatus.RELEASED,
                null,
                "Lock released successfully"
        );
    }

    /**
     * 3. REVOKE:
     * - If token holds the active lock -> delegates to releaseLock() internally!
     * - If token is in the queue -> removes from queue.
     * - If neither -> returns not found.
     */
    public LockResponseDto revokeLock(String lockName, String tokenId) {
        if (lockName == null || lockName.isBlank()) {
            throw new IllegalArgumentException("Lock name must not be blank");
        }
        if (tokenId == null || tokenId.isBlank()) {
            throw new IllegalArgumentException("Token ID must not be blank");
        }

        log.info("LockService: Revoke requested for resource '{}' [token={}]", lockName, tokenId);

        int outcome = lockManager.revoke(lockName, tokenId);

        // Remove from active emitters map if present
        SseEmitter emitter = activeEmitters.remove(tokenId);
        if (emitter != null) {
            try {
                emitter.complete();
            } catch (Exception ignored) {}
        }

        if (outcome == 1) {
            // Token is active holder: Call release service internally!
            log.info("LockService: Token '{}' holds the lock. Delegating to releaseLock() internally.", tokenId);
            return releaseLock(lockName, tokenId);
        } else if (outcome == 2) {
            log.info("LockService: Token '{}' revoked and removed from queue for '{}'", tokenId, lockName);
            return new LockResponseDto(
                    lockName,
                    tokenId,
                    LockStatus.REVOKED,
                    null,
                    "Queued request was revoked and removed from the queue successfully"
            );
        } else {
            log.warn("LockService: Revoke failed. Token '{}' not found for lock '{}'", tokenId, lockName);
            return new LockResponseDto(
                    lockName,
                    tokenId,
                    LockStatus.FAILED,
                    null,
                    "Request not found: token is neither holding the lock nor waiting in queue"
            );
        }
    }

    /**
     * 4. HANDOVER ENGINE:
     * Executes queueLockAcquire. If a valid waiter won the lock, pushes ACQUIRED event
     * to its open SSE stream and closes the connection!
     */
    public void triggerQueueHandover(String lockName) {
        String winningTokenId = lockManager.queueLockAcquire(lockName);

        if (winningTokenId != null && !winningTokenId.isBlank()) {
            log.info("LockService: Queue handover granted lock '{}' to token '{}'", lockName, winningTokenId);

            SseEmitter waitingEmitter = activeEmitters.remove(winningTokenId);
            if (waitingEmitter != null) {
                sendSseEvent(waitingEmitter, new LockResponseDto(
                        lockName,
                        winningTokenId,
                        LockStatus.ACQUIRED,
                        null,
                        "Lock acquired from queue successfully"
                ));
                waitingEmitter.complete(); // Connection closes now that lock is acquired!
            } else {
                // If waiting client had disconnected, pass the lock to the next waiter
                log.warn("LockService: Client for granted token '{}' already disconnected. Releasing to next.", winningTokenId);
                releaseLock(lockName, winningTokenId);
            }
        }
    }

    private void sendSseEvent(SseEmitter emitter, LockResponseDto response) {
        try {
            emitter.send(SseEmitter.event()
                    .name("lock-event")
                    .data(response, MediaType.APPLICATION_JSON));
        } catch (IOException e) {
            log.warn("LockService: Failed to send SSE event for token '{}': {}", response.tokenId(), e.getMessage());
        }
    }

    public LockManager getLockManager() {
        return lockManager;
    }
}
