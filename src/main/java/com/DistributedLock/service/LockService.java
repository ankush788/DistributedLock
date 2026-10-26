package com.DistributedLock.service;

import com.DistributedLock.deadlock.DeadlockDetector;
import com.DistributedLock.deadlock.LockDependencyGraph;
import com.DistributedLock.dto.LockResponseDto;
import com.DistributedLock.dto.LockResponseDto.LockStatus;
import com.DistributedLock.exception.LockException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core Distributed Lock Service coordinating:
 * 1. Redis atomic Lua scripts (via LockManager).
 * 2. Real-time Server-Sent Events (SSE) streaming lifecycle.
 * 3. In-memory Directed Wait-For Graph and cycle detection (DeadlockDetector).
 * 4. Fair FIFO queue handover with Redis Keyspace Alarm timeouts.
 * 5. Layer 2 Post-Enqueue Ground Truth verification to prevent race conditions.
 */
@Slf4j
@Service
public class LockService {

    private final LockManager lockManager;
    private final LockDependencyGraph dependencyGraph;
    private final DeadlockDetector deadlockDetector;

    @Value("${distributed-lock.queue-timeout-ms:60000}")
    private long sseTimeoutMillis;

    // Registry of open SSE streaming connections for queued clients, keyed by clientId
    private final Map<String, SseEmitter> activeEmitters = new ConcurrentHashMap<>();

    public LockService(LockManager lockManager,
                       LockDependencyGraph dependencyGraph,
                       DeadlockDetector deadlockDetector) {
        this.lockManager = lockManager;
        this.dependencyGraph = dependencyGraph;
        this.deadlockDetector = deadlockDetector;
    }

    // =========================================================================
    // 1. ACQUIRE LOCK (SSE STREAMING WITH LAYER 2 VERIFICATION)
    // =========================================================================

    /**
     * Attempts to acquire a distributed lock.
     *
     * Flow:
     * - Step 1: Fast-path direct acquire check (acquire_lock.lua). If free, returns ACQUIRED immediately (0ms).
     * - Step 2: Enqueues client into Redis FIFO list and arms individual 60s alarm key (enqueue_lock.lua).
     * - Step 3: Layer 2 Verification - checks queue: if popped during handover, grants ACQUIRED immediately.
     * - Step 4: Layer 2 Verification - checks Redis lock holder:
     *           - If free (null) -> triggers handover immediately.
     *           - If held by same client -> catches duplicate culprit, returns FAILED.
     *           - If held by another client -> runs proactive deadlock cycle check against true holder.
     * - Step 5: Safe to wait -> records wait edge in graph, stores emitter, sends QUEUED, and keeps connection open.
     */
    public SseEmitter acquireLock(String lockName, String clientId) {
        validateInputs(lockName, clientId);

        SseEmitter emitter = new SseEmitter(sseTimeoutMillis);
        log.info("LockService: Acquire requested for '{}' [client={}]", lockName, clientId);

        // --- STEP 1: Direct Acquire (Fast-Path) ---
        boolean acquired = lockManager.acquireDirect(lockName, clientId);
        if (acquired) {
            log.info("LockService: Direct acquire succeeded for '{}' [client={}]", lockName, clientId);
            dependencyGraph.recordAcquired(lockName, clientId);
            completeWithEvent(emitter, new LockResponseDto(lockName, clientId, LockStatus.ACQUIRED, null, "Lock acquired successfully"));
            return emitter;
        }

        // --- STEP 2: Enqueue in Redis & Arm Alarm Key ---
        int enqueueResult = lockManager.enqueue(lockName, clientId);
        if (enqueueResult <= 0) {
            log.error("LockService: Failed to enqueue client '{}' for lock '{}' in Redis", clientId, lockName);
            completeWithEvent(emitter, new LockResponseDto(lockName, clientId, LockStatus.FAILED, null, "Failed to enqueue lock request in Redis"));
            return emitter;
        }

        // --- STEP 3: Layer 2 Check 1 - Did handover pop client from queue while enqueuing? ---
        boolean inQueue = lockManager.isClientInQueue(lockName, clientId);
        if (!inQueue) {
            String currentHolder = lockManager.getLockHolder(lockName);
            if (clientId.equals(currentHolder)) {
                log.info("LockService: Client '{}' was popped from queue and granted lock '{}' via handover!", clientId, lockName);
                dependencyGraph.removeClient(clientId);
                dependencyGraph.recordAcquired(lockName, clientId);
                lockManager.deleteWaiterKey(lockName, clientId);
                completeWithEvent(emitter, new LockResponseDto(lockName, clientId, LockStatus.ACQUIRED, null, "Lock acquired successfully"));
                return emitter;
            }
        }

        // --- STEP 4: Layer 2 Check 2 - Query Redis for the true, current lock holder ---
        String realHolder = lockManager.getLockHolder(lockName);

        // Case 4A: Lock became free during enqueue (old holder released)
        if (realHolder == null) {
            log.info("LockService: Lock '{}' became free in Redis during enqueue. Triggering immediate handover.", lockName);
            triggerQueueHandover(lockName);

            realHolder = lockManager.getLockHolder(lockName);
            if (clientId.equals(realHolder)) {
                log.info("LockService: Immediate handover granted lock '{}' to client '{}'!", lockName, clientId);
                completeWithEvent(emitter, new LockResponseDto(lockName, clientId, LockStatus.ACQUIRED, null, "Lock acquired successfully"));
                return emitter;
            }
        }

        // Case 4B: Duplicate request caught - client already holds this lock
        if (clientId.equals(realHolder)) {
            log.warn("LockService: Client '{}' already holds lock '{}' in Redis", clientId, lockName);
            cleanPendingQueueEntry(lockName, clientId);
            completeWithEvent(emitter, new LockResponseDto(lockName, clientId, LockStatus.FAILED, null, "Client '" + clientId + "' already holds this lock"));
            return emitter;
        }

        // Case 4C: Proactive Deadlock Check against the verified Redis holder
        if (realHolder != null && deadlockDetector.willCauseDeadlock(clientId, realHolder)) {
            log.warn("LockService: DEADLOCK DETECTED! Aborting for '{}' [waiting={}, holder={}]", lockName, clientId, realHolder);
            cleanPendingQueueEntry(lockName, clientId);
            completeWithEvent(emitter, new LockResponseDto(
                    lockName,
                    clientId,
                    LockStatus.DEADLOCK_DETECTED,
                    null,
                    "Deadlock detected: circular wait cycle detected with client '" + realHolder + "'"
            ));
            return emitter;
        }

        // --- STEP 5: Safe to Wait ---
        if (realHolder != null) {
            dependencyGraph.recordWaiting(clientId, realHolder);
        }

        log.info("LockService: Lock '{}' occupied. Request waiting in FIFO queue [client={}]", lockName, clientId);
        registerWaitingEmitter(lockName, clientId, emitter, enqueueResult);

        // Immediate check in case lock was freed in this exact millisecond
        triggerQueueHandover(lockName);

        return emitter;
    }

    // =========================================================================
    // 2. RELEASE LOCK
    // =========================================================================

    /**
     * Atomically releases an active lock verifying ownership, updates graph,
     * and triggers handover to the next queued client.
     */
    public LockResponseDto releaseLock(String lockName, String clientId) {
        validateInputs(lockName, clientId);
        log.info("LockService: Release requested for '{}' [client={}]", lockName, clientId);

        boolean released = lockManager.release(lockName, clientId);

        if (!released) {
            if (lockManager.isLocked(lockName)) {
                log.warn("LockService: Unauthorized release on '{}' by client '{}'", lockName, clientId);
                throw new LockException.UnauthorizedRelease(lockName, clientId);
            } else {
                log.warn("LockService: Release failed for '{}': lock does not exist or expired", lockName);
                throw new LockException.NotFound(lockName);
            }
        }

        log.info("LockService: Lock '{}' released by client '{}'. Triggering handover.", lockName, clientId);

        // Clear ownership and incoming edges from wait-for graph
        dependencyGraph.recordReleased(lockName, clientId);

        // Hand over lock to the next waiting client in FIFO line
        triggerQueueHandover(lockName);

        return new LockResponseDto(lockName, clientId, LockStatus.RELEASED, null, "Lock released successfully");
    }

    // =========================================================================
    // 3. REVOKE LOCK
    // =========================================================================

    /**
     * Cancels / revokes a lock request:
     * - If client currently holds the lock -> delegates to releaseLock internally.
     * - If client is waiting in queue -> removes from queue, deletes alarm key, and closes SSE stream.
     */
    public LockResponseDto revokeLock(String lockName, String clientId) {
        validateInputs(lockName, clientId);
        log.info("LockService: Revoke requested for '{}' [client={}]", lockName, clientId);

        int outcome = lockManager.revoke(lockName, clientId);

        if (outcome == 1) {
            // Active lock owner: delegate to releaseLock service internally
            log.info("LockService: Client '{}' holds the lock. Delegating to releaseLock() internally.", clientId);
            return releaseLock(lockName, clientId);
        } else if (outcome == 2) {
            // Confirmed removed from queue: clean wait dependencies from graph
            dependencyGraph.removeClient(clientId);

            // Close active waiting SSE connection
            SseEmitter emitter = activeEmitters.remove(clientId);
            if (emitter != null) {
                try {
                    emitter.complete();
                } catch (Exception ignored) {}
            }
            log.info("LockService: Client '{}' revoked and removed from queue for '{}'", clientId, lockName);
            return new LockResponseDto(lockName, clientId, LockStatus.REVOKED, null, "Queued request was revoked successfully");
        } else {
            log.warn("LockService: Revoke failed. Client '{}' not found for lock '{}'", clientId, lockName);
            return new LockResponseDto(lockName, clientId, LockStatus.FAILED, null, "Request not found: client neither holds lock nor waits in queue");
        }
    }

    // =========================================================================
    // 4. QUEUE HANDOVER ENGINE
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
            log.info("LockService: Queue handover granted lock '{}' to client '{}'", lockName, winningClientId);

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
                log.warn("LockService: Granted client '{}' already disconnected. Releasing to next.", winningClientId);
                releaseLock(lockName, winningClientId);
            }
        }
    }

    // =========================================================================
    // 5. WAITER TIMEOUT HANDLER (REDIS KEYSPACE ALARM)
    // =========================================================================

    /**
     * Intercepts Redis keyspace alarm expiration for lock:waiter:<lockName>:<clientId>.
     * Cleans graph, notifies SSE connection with TIMEOUT, and purges Redis queue list.
     */
    public void handleWaiterTimeout(String lockName, String clientId) {
        log.warn("LockService: Waiter timeout triggered by Redis alarm for '{}' [client={}]", lockName, clientId);

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
    // HELPER METHODS (MODULARITY & READABILITY)
    // =========================================================================

    private void registerWaitingEmitter(String lockName, String clientId, SseEmitter emitter, int enqueueResult) {
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
            log.warn("LockService: SSE timeout for '{}' [client={}]. Revoking internally.", lockName, clientId);
            activeEmitters.remove(clientId);
            revokeLock(lockName, clientId);
        });

        // Safety on disconnect/error: revokes internally
        emitter.onError((ex) -> {
            log.warn("LockService: SSE disconnect/error for '{}' [client={}]. Revoking internally.", lockName, clientId);
            activeEmitters.remove(clientId);
            revokeLock(lockName, clientId);
        });

        emitter.onCompletion(() -> activeEmitters.remove(clientId));
    }

    private void cleanPendingQueueEntry(String lockName, String clientId) {
        lockManager.removeFromQueue(lockName, clientId);
        lockManager.deleteWaiterKey(lockName, clientId);
    }

    private void completeWithEvent(SseEmitter emitter, LockResponseDto response) {
        sendSseEvent(emitter, response);
        try {
            emitter.complete();
        } catch (Exception ignored) {}
    }

    private void sendSseEvent(SseEmitter emitter, LockResponseDto response) {
        try {
            emitter.send(SseEmitter.event()
                    .name("lock-event")
                    .data(response, MediaType.APPLICATION_JSON));
        } catch (Exception e) {
            log.warn("LockService: Failed to send SSE event for client '{}': {}", response.clientId(), e.getMessage());
        }
    }

    private void validateInputs(String lockName, String clientId) {
        if (lockName == null || lockName.isBlank()) {
            throw new IllegalArgumentException("Lock name must not be blank");
        }
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException("Client ID must not be blank");
        }
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    public LockDependencyGraph getDependencyGraph() {
        return dependencyGraph;
    }

    public DeadlockDetector getDeadlockDetector() {
        return deadlockDetector;
    }
}
