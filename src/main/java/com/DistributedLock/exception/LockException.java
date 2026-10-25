package com.DistributedLock.exception;

import lombok.Getter;

/**
 * Base distributed lock exception enclosing all specific lock failure types as static nested classes.
 */
@Getter
public class LockException extends RuntimeException {

    private final String lockName;

    public LockException(String message) {
        super(message);
        this.lockName = null;
    }

    public LockException(String message, String lockName) {
        super(message);
        this.lockName = lockName;
    }

    public LockException(String message, String lockName, Throwable cause) {
        super(message, cause);
        this.lockName = lockName;
    }

    /**
     * Thrown when a queued lock request exceeds its maximum queue waiting duration.
     */
    public static class Timeout extends LockException {
        public Timeout(String lockName, long timeoutSeconds) {
            super(String.format("Timed out waiting in queue for lock '%s' after %d seconds", lockName, timeoutSeconds), lockName);
        }
    }

    /**
     * Thrown when an unauthorized caller attempts to release a lock with an invalid or mismatched clientId.
     */
    public static class UnauthorizedRelease extends LockException {
        public UnauthorizedRelease(String lockName, String invalidClientId) {
            super(String.format("Unauthorized release attempt for lock '%s'. Provided clientId '%s' does not match owner.", lockName, invalidClientId), lockName);
        }
    }

    /**
     * Thrown when operating on a lock that does not exist or has already auto-expired in Redis.
     */
    public static class NotFound extends LockException {
        public NotFound(String lockName) {
            super(String.format("Lock '%s' was not found or has already expired.", lockName), lockName);
        }
    }

    /**
     * Thrown when the DeadlockDetector detects a circular wait cycle and aborts the transaction.
     */
    public static class Deadlock extends LockException {
        public Deadlock(String lockName, String cycleDetails) {
            super(String.format("Deadlock detected on lock '%s'. Cycle: %s. Transaction aborted.", lockName, cycleDetails), lockName);
        }
    }

    /**
     * Thrown when lock acquisition fails due to contention, capacity, or internal state issues.
     */
    public static class AcquisitionFailed extends LockException {
        public AcquisitionFailed(String lockName, String reason) {
            super(String.format("Failed to acquire lock '%s': %s", lockName, reason), lockName);
        }

        public AcquisitionFailed(String lockName, Throwable cause) {
            super(String.format("Failed to acquire lock '%s' due to internal error.", lockName), lockName, cause);
        }
    }
}
