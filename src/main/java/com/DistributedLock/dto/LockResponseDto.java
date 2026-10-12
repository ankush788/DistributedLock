package com.DistributedLock.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LockResponseDto {

    /** Name of the resource lock this response corresponds to. */
    private String lockName;

    /** Secret UUID token proving lock ownership (required to release the lock). */
    private String tokenId;

    /** Current state of the lock request (ACQUIRED, QUEUED, FAILED, RELEASED, DEADLOCK_DETECTED). */
    private LockStatus status;

    /** Waiting position in the FIFO queue (null if acquired immediately). */
    private Integer queuePosition;

    /** Human-readable explanation of the result for logs and debugging. */
    private String message;

    public enum LockStatus {
        ACQUIRED,
        QUEUED,
        FAILED,
        RELEASED,
        DEADLOCK_DETECTED
    }
}
