package com.DistributedLock.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;

import java.time.LocalDateTime;

/**
 * Standardized API Error Response Payload.
 *
 * @param timestamp Timestamp when the error occurred.
 * @param status    HTTP Status code (e.g. 400, 403, 404, 408, 409, 500).
 * @param error     Short error code / classification (e.g. "LOCK_TIMEOUT", "DEADLOCK_DETECTED").
 * @param message   User-friendly detailed explanation of the error.
 * @param path      The API request path that caused the error.
 * @param lockName  The lockName associated with the failure, if applicable.
 */
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponseDto(
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    LocalDateTime timestamp,
    int status,
    String error,
    String message,
    String path,
    String lockName
) {}
