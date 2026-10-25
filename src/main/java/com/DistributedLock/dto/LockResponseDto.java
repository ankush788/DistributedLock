package com.DistributedLock.dto;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Response payload describing lock acquisition status.
 *
 * @param lockName      Name of the resource lock.
 * @param clientId      Unique client identifier.
 * @param status        Current state of the lock request.
 * @param queuePosition Waiting position in the FIFO queue (null if acquired immediately).
 * @param message       Human-readable explanation of the result.
 */
@Schema(description = "Response payload describing lock acquisition status")
public record LockResponseDto(
    @Schema(description = "Name of the resource lock", example = "order-123")
    String lockName,

    @Schema(description = "Unique client identifier", example = "client-1")
    String clientId,

    @Schema(description = "Current lock status")
    LockStatus status,

    @Schema(description = "Position in queue (null if acquired directly)", example = "1")
    Integer queuePosition,

    @Schema(description = "Status explanation message")
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
