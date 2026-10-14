package com.DistributedLock.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Standardized API Error Response Payload.
 */
@Data
@Builder
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErrorResponseDto {

    public ErrorResponseDto(LocalDateTime timestamp, int status, String error, String message, String path, String lockName) {
        this.timestamp = timestamp;
        this.status = status;
        this.error = error;
        this.message = message;
        this.path = path;
        this.lockName = lockName;
    }

    /** Timestamp when the error occurred. */
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime timestamp;

    /** HTTP Status code (e.g. 400, 403, 404, 408, 409, 500). */
    private int status;

    /** Short error code / classification (e.g. "LOCK_TIMEOUT", "DEADLOCK_DETECTED"). */
    private String error;

    /** User-friendly detailed explanation of the error. */
    private String message;

    /** The API request path that caused the error (e.g. "/api/v1/locks/release"). */
    private String path;

    /** The lockName associated with the failure, if applicable. */
    private String lockName;
}
