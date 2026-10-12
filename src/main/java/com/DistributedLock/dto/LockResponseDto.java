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

    private String lockName;
    private String tokenId;
    private LockStatus status;
    private Integer queuePosition;
    private String message;

    public enum LockStatus {
        ACQUIRED,
        QUEUED,
        FAILED,
        RELEASED,
        DEADLOCK_DETECTED
    }
}
