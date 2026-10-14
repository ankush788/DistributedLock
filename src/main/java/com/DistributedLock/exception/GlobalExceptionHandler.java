package com.DistributedLock.exception;

import com.DistributedLock.dto.ErrorResponseDto;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.stream.Collectors;

/**
 * Centralized exception interceptor converting all exceptions into standardized ErrorResponseDto.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(LockException.Timeout.class)
    public ResponseEntity<ErrorResponseDto> handleLockTimeout(LockException.Timeout ex, HttpServletRequest request) {
        log.warn("Lock timeout: {}", ex.getMessage());
        return buildResponse(HttpStatus.REQUEST_TIMEOUT, "LOCK_TIMEOUT", ex.getMessage(), request.getRequestURI(), ex.getLockName());
    }

    @ExceptionHandler(LockException.UnauthorizedRelease.class)
    public ResponseEntity<ErrorResponseDto> handleUnauthorizedRelease(LockException.UnauthorizedRelease ex, HttpServletRequest request) {
        log.warn("Security Alert: {}", ex.getMessage());
        return buildResponse(HttpStatus.FORBIDDEN, "UNAUTHORIZED_LOCK_RELEASE", ex.getMessage(), request.getRequestURI(), ex.getLockName());
    }

    @ExceptionHandler(LockException.NotFound.class)
    public ResponseEntity<ErrorResponseDto> handleLockNotFound(LockException.NotFound ex, HttpServletRequest request) {
        log.warn("Lock not found: {}", ex.getMessage());
        return buildResponse(HttpStatus.NOT_FOUND, "LOCK_NOT_FOUND", ex.getMessage(), request.getRequestURI(), ex.getLockName());
    }

    @ExceptionHandler(LockException.Deadlock.class)
    public ResponseEntity<ErrorResponseDto> handleDeadlock(LockException.Deadlock ex, HttpServletRequest request) {
        log.error("Deadlock Resolved by Abort: {}", ex.getMessage());
        return buildResponse(HttpStatus.CONFLICT, "DEADLOCK_DETECTED", ex.getMessage(), request.getRequestURI(), ex.getLockName());
    }

    @ExceptionHandler(LockException.AcquisitionFailed.class)
    public ResponseEntity<ErrorResponseDto> handleLockAcquisition(LockException.AcquisitionFailed ex, HttpServletRequest request) {
        log.warn("Lock acquisition failed: {}", ex.getMessage());
        return buildResponse(HttpStatus.CONFLICT, "LOCK_ACQUISITION_FAILED", ex.getMessage(), request.getRequestURI(), ex.getLockName());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponseDto> handleValidationErrors(MethodArgumentNotValidException ex, HttpServletRequest request) {
        String validationErrors = ex.getBindingResult().getFieldErrors().stream()
                .map(FieldError::getDefaultMessage)
                .collect(Collectors.joining("; "));
        log.warn("Validation error on {}: {}", request.getRequestURI(), validationErrors);
        return buildResponse(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", validationErrors, request.getRequestURI(), null);
    }

    @ExceptionHandler(LockException.class)
    public ResponseEntity<ErrorResponseDto> handleGenericLockException(LockException ex, HttpServletRequest request) {
        log.error("General Lock Error: {}", ex.getMessage(), ex);
        return buildResponse(HttpStatus.INTERNAL_SERVER_ERROR, "LOCK_ERROR", ex.getMessage(), request.getRequestURI(), ex.getLockName());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponseDto> handleUnexpectedException(Exception ex, HttpServletRequest request) {
        log.error("Unhandled System Exception on {}: {}", request.getRequestURI(), ex.getMessage(), ex);
        return buildResponse(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR", "An unexpected system error occurred: " + ex.getMessage(), request.getRequestURI(), null);
    }

    private ResponseEntity<ErrorResponseDto> buildResponse(HttpStatus status, String errorCode, String message, String path, String lockName) {
        ErrorResponseDto errorDto = new ErrorResponseDto(
                LocalDateTime.now(),
                status.value(),
                errorCode,
                message,
                path,
                lockName
        );
        return new ResponseEntity<>(errorDto, status);
    }
}
