package com.DistributedLock.dto;
import jakarta.validation.constraints.NotBlank;

/**
 * Request payload for acquiring a distributed lock.
 * param lockName Unique identifier of the shared resource to lock (e.g. inventory-123).
 */

public record LockRequestDto(
    @NotBlank(message = "Lock name cannot be empty")
    String lockName
) {}
