package com.DistributedLock.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Builder;

/**
 * Request payload for acquiring a distributed lock.
 *
 * @param lockName Unique identifier of the shared resource to lock (e.g. inventory-123).
 */
@Builder
public record LockRequestDto(
    @NotBlank(message = "Lock name cannot be empty")
    String lockName
) {}
