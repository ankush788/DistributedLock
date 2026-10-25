package com.DistributedLock.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

/**
 * Request payload for releasing a distributed lock.
 *
 * @param lockName Name of the locked resource to be released.
 * @param clientId Unique client identifier that holds the lock.
 */
@Schema(description = "Request payload for releasing a distributed lock")
public record LockReleaseDto(
    @NotBlank(message = "Lock name cannot be empty")
    @Schema(description = "Name of the resource lock", example = "order-123")
    String lockName,

    @NotBlank(message = "Client ID cannot be empty")
    @Schema(description = "Unique client identifier", example = "client-1")
    String clientId
) {}
