package com.DistributedLock.dto;
import jakarta.validation.constraints.NotBlank;

/**
 * Request payload for releasing a distributed lock.
 *
 * param lockName Name of the locked resource to be released.
 * param tokenId  Ownership token previously issued when lock was acquired.
 */

public record LockReleaseDto(
    @NotBlank(message = "Lock name cannot be empty")
    String lockName,

    @NotBlank(message = "Token ID cannot be empty")
    String tokenId
) {}
