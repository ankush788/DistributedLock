package com.DistributedLock.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

/**
 * Request payload for acquiring a distributed lock.
 *
 * @param lockName Unique identifier of the shared resource to lock (e.g. inventory-123).
 * @param clientId Unique identifier of the client requesting the lock (e.g. client-1).
 */
@Schema(description = "Request payload for acquiring a distributed lock")
public record LockRequestDto(
    @NotBlank(message = "Lock name cannot be empty")
    @Schema(description = "Name of the resource to lock", example = "order-123")
    String lockName,

    @NotBlank(message = "Client ID cannot be empty")
    @Schema(description = "Unique client identifier", example = "client-1")
    String clientId
) {}
