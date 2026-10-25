package com.DistributedLock.deadlock;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe Directed "Wait-For Graph" tracking active locks and client dependencies.
 *
 * Concepts:
 * - lockHolders: Maps resource lockName -> clientId that currently owns it.
 * - waitGraph:   Maps waitingClientId -> Set of holdingClientIds it is waiting for.
 */
@Slf4j
@Component
public class LockDependencyGraph {

    // lockName -> clientId currently holding the lock
    private final Map<String, String> lockHolders = new ConcurrentHashMap<>();

    // waitingClientId -> set of holdingClientIds it is waiting for
    private final Map<String, Set<String>> waitGraph = new ConcurrentHashMap<>();

    /**
     * Records that a client successfully acquired a lock.
     */
    public void recordAcquired(String lockName, String clientId) {
        lockHolders.put(lockName, clientId);
        log.debug("Graph: Client '{}' acquired lock '{}'", clientId, lockName);
    }

    /**
     * Records that a client is waiting for a lock held by holdingClientId.
     * Adds a directed edge: waitingClientId -> holdingClientId
     */
    public void recordWaiting(String waitingClientId, String holdingClientId) {
        waitGraph.computeIfAbsent(waitingClientId, k -> ConcurrentHashMap.newKeySet()).add(holdingClientId);
        log.debug("Graph: Edge added [{} -> {}]", waitingClientId, holdingClientId);
    }

    /**
     * Removes all waiting edges and holdings for a client (called on cleanup/disconnect/timeout).
     */
    public void removeClient(String clientId) {
        waitGraph.remove(clientId);
        // Also remove as a target from all other nodes
        waitGraph.values().forEach(targets -> targets.remove(clientId));
    }

    /**
     * Records that a lock was released. Cleans up ownership and edges.
     */
    public void recordReleased(String lockName, String clientId) {
        lockHolders.remove(lockName, clientId);
        // Remove incoming wait edges for this holder from all waiting clients
        waitGraph.values().forEach(targets -> targets.remove(clientId));
        log.debug("Graph: Lock '{}' released by client '{}'", lockName, clientId);
    }

    /**
     * Returns the clientId currently holding the specified lock, or null if not tracked.
     */
    public String getLockHolder(String lockName) {
        return lockHolders.get(lockName);
    }

    /**
     * Returns an unmodifiable snapshot view of the wait graph.
     */
    public Map<String, Set<String>> getWaitGraph() {
        return Collections.unmodifiableMap(waitGraph);
    }

    /**
     * Clears all graph data (useful for testing or reset).
     */
    public void clear() {
        lockHolders.clear();
        waitGraph.clear();
    }
}
