package com.DistributedLock.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

/**
 * Request payload for revoking a lock request.
 * If the request is currently in the queue, it will be removed.
 * If the lock has already been acquired by this token, it will be released and the next waiter signaled.
 */
@Schema(description = "Request payload for revoking a lock request")
public record LockRevokeDto(
    @NotBlank(message = "Lock name cannot be empty")
    @Schema(description = "Name of the resource lock", example = "order-999")
    String lockName,

    @NotBlank(message = "Token ID cannot be empty")
    @Schema(description = "Ownership token to be revoked", example = "ae0c1190-83ee-443c-a6dd-35a49ec10242")
    String tokenId
) {}
