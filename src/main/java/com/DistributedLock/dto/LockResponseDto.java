package com.DistributedLock.dto;

/**
 * Response payload describing lock acquisition status.
 * param lockName      Name of the resource lock.
 * param tokenId       Secret UUID token proving lock ownership.
 * param status        Current state of the lock request.
 * param queuePosition Waiting position in the FIFO queue (null if acquired immediately).
 * param message       Human-readable explanation of the result.
 */

public record LockResponseDto(
    String lockName,
    String tokenId,
    LockStatus status,
    Integer queuePosition,
    String message
) {
    public enum LockStatus {
        ACQUIRED,
        QUEUED,
        FAILED,
        RELEASED,
        REVOKED,
        TIMEOUT,
        DEADLOCK_DETECTED
    }
}
