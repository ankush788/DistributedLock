package com.DistributedLock.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

/**
 * Request payload for revoking a lock request.
 * If the client is currently in the queue, it will be removed.
 * If the lock has already been acquired by this client, it will be released and the next waiter signaled.
 */
@Schema(description = "Request payload for revoking a lock request")
public record LockRevokeDto(
    @NotBlank(message = "Lock name cannot be empty")
    @Schema(description = "Name of the resource lock", example = "order-123")
    String lockName,

    @NotBlank(message = "Client ID cannot be empty")
    @Schema(description = "Unique client identifier", example = "client-1")
    String clientId
) {}
