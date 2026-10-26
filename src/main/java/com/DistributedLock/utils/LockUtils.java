package com.DistributedLock.utils;

import com.DistributedLock.deadlock.LockDependencyGraph;
import com.DistributedLock.dto.LockResponseDto;
import com.DistributedLock.dto.LockResponseDto.LockStatus;
import com.DistributedLock.service.LockManager;
import com.DistributedLock.service.LockService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility and helper component handling:
 * 1. SSE Emitter lifecycle (registration, event streaming, timeouts, disconnect safety).
 * 2. Queue Handover engine.
 * 3. Waiter timeout handling.
 * 4. Input validation and queue cleanup helpers.
 */
@Slf4j
@Component
public class LockUtils {

    private final LockManager lockManager;
    private final LockDependencyGraph dependencyGraph;
    private final LockService lockService;

    // Registry of open SSE streaming connections for queued clients, keyed by clientId
    private final Map<String, SseEmitter> activeEmitters = new ConcurrentHashMap<>();

    public LockUtils(LockManager lockManager,
                     LockDependencyGraph dependencyGraph,
                     @Lazy LockService lockService) {
        this.lockManager = lockManager;
        this.dependencyGraph = dependencyGraph;
        this.lockService = lockService;
    }

    // =========================================================================
    // 1. QUEUE HANDOVER ENGINE
    // =========================================================================

    /**
     * Executes queueLockAcquire. If a valid waiter won the lock:
     * - Cancels its waiter alarm key.
     * - Updates graph ownership.
     * - Sends ACQUIRED event to its open SSE stream and closes connection.
     */
    public void triggerQueueHandover(String lockName) {
        String winningClientId = lockManager.queueLockAcquire(lockName);

        if (winningClientId != null && !winningClientId.isBlank()) {
            log.info("LockUtils: Queue handover granted lock '{}' to client '{}'", lockName, winningClientId);

            // Cancel alarm key so it won't fire timeout later
            lockManager.deleteWaiterKey(lockName, winningClientId);

            // Clean wait dependencies for winning client since it's no longer waiting
            dependencyGraph.removeClient(winningClientId);

            // Update dependency graph with new owner
            dependencyGraph.recordAcquired(lockName, winningClientId);

            SseEmitter waitingEmitter = activeEmitters.remove(winningClientId);
            if (waitingEmitter != null) {
                completeWithEvent(waitingEmitter, new LockResponseDto(
                        lockName,
                        winningClientId,
                        LockStatus.ACQUIRED,
                        null,
                        "Lock acquired from queue successfully"
                ));
            } else {
                // If client had already disconnected, pass lock to the next waiter
                log.warn("LockUtils: Granted client '{}' already disconnected. Releasing to next.", winningClientId);
                lockService.releaseLock(lockName, winningClientId);
            }
        }
    }

    // =========================================================================
    // 2. WAITER TIMEOUT HANDLER (REDIS KEYSPACE ALARM)
    // =========================================================================

    /**
     * Intercepts Redis keyspace alarm expiration for lock:waiter:<lockName>:<clientId>.
     * Cleans graph, notifies SSE connection with TIMEOUT, and purges Redis queue list.
     */
    public void handleWaiterTimeout(String lockName, String clientId) {
        log.warn("LockUtils: Waiter timeout triggered by Redis alarm for '{}' [client={}]", lockName, clientId);

        // 1. Remove from in-memory wait graph
        dependencyGraph.removeClient(clientId);

        // 2. Remove and notify waiting SSE stream
        SseEmitter emitter = activeEmitters.remove(clientId);
        if (emitter != null) {
            completeWithEvent(emitter, new LockResponseDto(
                    lockName,
                    clientId,
                    LockStatus.TIMEOUT,
                    null,
                    "Queue wait timeout exceeded (60s)"
            ));
        }

        // 3. Purge from Redis queue list
        lockManager.removeFromQueue(lockName, clientId);
    }

    // =========================================================================
    // 3. SSE EMITTER & HELPER METHODS
    // =========================================================================

    public void registerWaitingEmitter(String lockName, String clientId, SseEmitter emitter, int enqueueResult) {
        activeEmitters.put(clientId, emitter);

        // Send initial QUEUED event
        sendSseEvent(emitter, new LockResponseDto(
                lockName,
                clientId,
                LockStatus.QUEUED,
                null,
                enqueueResult == 2
                        ? "Request is already in FIFO queue"
                        : "Lock is occupied; request placed in FIFO queue"
        ));

        // Safety on timeout: revokes internally
        emitter.onTimeout(() -> {
            log.warn("LockUtils: SSE timeout for '{}' [client={}]. Revoking internally.", lockName, clientId);
            activeEmitters.remove(clientId);
            lockService.revokeLock(lockName, clientId);
        });

        // Safety on disconnect/error: revokes internally
        emitter.onError((ex) -> {
            log.warn("LockUtils: SSE disconnect/error for '{}' [client={}]. Revoking internally.", lockName, clientId);
            activeEmitters.remove(clientId);
            lockService.revokeLock(lockName, clientId);
        });

        emitter.onCompletion(() -> activeEmitters.remove(clientId));
    }

    public void cleanPendingQueueEntry(String lockName, String clientId) {
        lockManager.removeFromQueue(lockName, clientId);
        lockManager.deleteWaiterKey(lockName, clientId);
    }

    public void completeWithEvent(SseEmitter emitter, LockResponseDto response) {
        sendSseEvent(emitter, response);
        try {
            emitter.complete();
        } catch (Exception ignored) {}
    }

    public void sendSseEvent(SseEmitter emitter, LockResponseDto response) {
        try {
            emitter.send(SseEmitter.event()
                    .name("lock-event")
                    .data(response, MediaType.APPLICATION_JSON));
        } catch (Exception e) {
            log.warn("LockUtils: Failed to send SSE event for client '{}': {}", response.clientId(), e.getMessage());
        }
    }

    public void validateInputs(String lockName, String clientId) {
        if (lockName == null || lockName.isBlank()) {
            throw new IllegalArgumentException("Lock name must not be blank");
        }
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException("Client ID must not be blank");
        }
    }

    public SseEmitter removeEmitter(String clientId) {
        return activeEmitters.remove(clientId);
    }

    public Map<String, SseEmitter> getActiveEmitters() {
        return activeEmitters;
    }
}
