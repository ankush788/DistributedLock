package com.DistributedLock.deadlock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DeadlockDetectorTest {

    private LockDependencyGraph graph;
    private DeadlockDetector detector;

    @BeforeEach
    void setUp() {
        graph = new LockDependencyGraph();
        detector = new DeadlockDetector(graph);
    }

    @Test
    @DisplayName("Should detect 2-node circular deadlock (A -> B -> A)")
    void testTwoNodeDeadlock() {
        // Client A holds "order-1"
        graph.recordAcquired("order-1", "client-A");
        // Client B holds "order-2"
        graph.recordAcquired("order-2", "client-B");

        // Client A waits for "order-2" (held by Client B) -> edge: client-A -> client-B
        graph.recordWaiting("client-A", "client-B");

        // Now Client B attempts to wait for "order-1" (held by Client A) -> edge: client-B -> client-A
        boolean deadlock = detector.willCauseDeadlock("client-B", "client-A");

        assertTrue(deadlock, "Deadlock must be detected when B tries to wait on A while A waits on B");
    }

    @Test
    @DisplayName("Should detect 3-node circular deadlock (A -> B -> C -> A)")
    void testThreeNodeDeadlock() {
        // A holds lock1, B holds lock2, C holds lock3
        graph.recordAcquired("lock1", "client-A");
        graph.recordAcquired("lock2", "client-B");
        graph.recordAcquired("lock3", "client-C");

        // A waits on B, B waits on C
        graph.recordWaiting("client-A", "client-B");
        graph.recordWaiting("client-B", "client-C");

        // Now C attempts to wait on A -> closes the circle A -> B -> C -> A!
        boolean deadlock = detector.willCauseDeadlock("client-C", "client-A");

        assertTrue(deadlock, "Deadlock must be detected for 3-node circular dependency");
    }

    @Test
    @DisplayName("Should allow safe linear dependency chain without deadlock")
    void testSafeChain() {
        graph.recordAcquired("lock1", "client-A");
        graph.recordAcquired("lock2", "client-B");

        // A waits on B (safe)
        boolean deadlock1 = detector.willCauseDeadlock("client-A", "client-B");
        assertFalse(deadlock1);
        graph.recordWaiting("client-A", "client-B");

        // C waits on B (safe tree structure)
        boolean deadlock2 = detector.willCauseDeadlock("client-C", "client-B");
        assertFalse(deadlock2);
    }

    @Test
    @DisplayName("Should detect self-deadlock when client waits on its own lock")
    void testSelfDeadlock() {
        graph.recordAcquired("lock1", "client-A");

        // Client A tries to wait on Client A
        boolean deadlock = detector.willCauseDeadlock("client-A", "client-A");
        assertTrue(deadlock, "Self-deadlock must be detected");
    }
}
