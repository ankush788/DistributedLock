package com.DistributedLock.deadlock;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Detects circular wait-for cycles in the LockDependencyGraph.
 *
 * Algorithm:
 * Reachability Check (BFS):
 * If 'waitingClientId' wants to wait on 'holdingClientId', we check:
 * Can 'holdingClientId' already reach 'waitingClientId' through existing dependency edges?
 * If YES: Adding this new edge would close the loop (waitingClientId -> holdingClientId -> ... -> waitingClientId).
 * Therefore, a circular DEADLOCK would occur!
 */
@Slf4j
@Component
public class DeadlockDetector {

    private final LockDependencyGraph dependencyGraph;

    public DeadlockDetector(LockDependencyGraph dependencyGraph) {
        this.dependencyGraph = dependencyGraph;
    }

    /**
     * Proactively checks if adding an edge [waitingClientId -> holdingClientId] will cause a deadlock.
     *
     * @param waitingClientId The client requesting the lock.
     * @param holdingClientId The client that currently holds the desired lock.
     * @return true if a cycle (deadlock) would be formed; false if safe.
     */
    public boolean willCauseDeadlock(String waitingClientId, String holdingClientId) {
        if (waitingClientId == null || holdingClientId == null) {
            return false;
        }

        // Case 1: Self-deadlock (client already holds this lock and is requesting it again)
        if (waitingClientId.equals(holdingClientId)) {
            log.warn("DeadlockDetector: Self-deadlock detected! Client '{}' is waiting for its own lock.", waitingClientId);
            return true;
        }

        // Case 2: Circular Dependency Check
        // Check if holdingClientId can already reach waitingClientId
        Map<String, Set<String>> graph = dependencyGraph.getWaitGraph();
        boolean cycle = isReachable(holdingClientId, waitingClientId, graph);

        if (cycle) {
            log.warn("DeadlockDetector: Circular deadlock detected! [{}] is already waiting for [{}]. Adding edge would form a cycle.",
                    holdingClientId, waitingClientId);
        }

        return cycle;
    }

    /**
     * Checks if 'target' is reachable from 'start' in the directed graph using Breadth-First Search (BFS).
     */
    private boolean isReachable(String start, String target, Map<String, Set<String>> graph) {
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new ArrayDeque<>();

        queue.add(start);
        visited.add(start);

        while (!queue.isEmpty()) {
            String current = queue.poll();

            if (current.equals(target)) {
                return true; // Cycle detected: path leads directly back to target!
            }

            Set<String> neighbors = graph.getOrDefault(current, Collections.emptySet());
            for (String neighbor : neighbors) {
                if (visited.add(neighbor)) {
                    queue.add(neighbor);
                }
            }
        }

        return false;
    }
}
