package com.DistributedLock.controller;

import com.DistributedLock.dto.LockReleaseDto;
import com.DistributedLock.dto.LockRequestDto;
import com.DistributedLock.dto.LockResponseDto;
import com.DistributedLock.dto.LockResponseDto.LockStatus;
import com.DistributedLock.dto.LockRevokeDto;
import com.DistributedLock.service.LockService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * REST API Controller for distributed locking operations.
 * All operations identify callers via a unique clientId.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/locks")
@Tag(name = "Distributed Lock API", description = "Endpoints for acquiring, releasing, and revoking distributed locks")
public class LockController {

    private final LockService lockService;

    public LockController(LockService lockService) {
        this.lockService = lockService;
    }

    /**
     * Acquire endpoint using Server-Sent Events (SSE) via GET.
     * Perfect for browser EventSource and curl -N.
     */
    @GetMapping(value = "/acquire", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Acquire a distributed lock (SSE via GET)", description = "Streams lock state. Returns immediately if free, or streams until lock is acquired in fair FIFO order.")
    public SseEmitter acquireLockGet(
            @RequestParam("lockName") String lockName,
            @RequestParam("clientId") String clientId) {
        log.info("REST SSE (GET): Lock acquire request for resource '{}' [clientId={}]", lockName, clientId);
        return lockService.acquireLock(lockName, clientId);
    }

    /**
     * Acquire endpoint using Server-Sent Events (SSE) via POST.
     * Compatible with REST clients passing JSON body.
     */
    @PostMapping(value = "/acquire", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Acquire a distributed lock (SSE via POST)", description = "Streams lock state. Returns immediately if free, or streams until lock is acquired in fair FIFO order.")
    public SseEmitter acquireLockPost(@Valid @RequestBody LockRequestDto request) {
        log.info("REST SSE (POST): Lock acquire request for resource '{}' [clientId={}]", request.lockName(), request.clientId());
        return lockService.acquireLock(request.lockName(), request.clientId());
    }

    @PostMapping("/release")
    @Operation(summary = "Release a distributed lock", description = "Releases lock atomically after verifying client ownership.")
    public ResponseEntity<LockResponseDto> releaseLock(@Valid @RequestBody LockReleaseDto request) {
        log.info("REST: Lock release request for resource '{}' [clientId={}]", request.lockName(), request.clientId());
        LockResponseDto response = lockService.releaseLock(request.lockName(), request.clientId());
        return ResponseEntity.ok(response);
    }

    @PostMapping("/revoke")
    @Operation(summary = "Revoke a lock request", description = "Removes request from queue if waiting, or releases lock and signals next if currently held.")
    public ResponseEntity<LockResponseDto> revokeLock(@Valid @RequestBody LockRevokeDto request) {
        log.info("REST: Lock revoke request for resource '{}' [clientId={}]", request.lockName(), request.clientId());
        LockResponseDto response = lockService.revokeLock(request.lockName(), request.clientId());

        HttpStatus httpStatus = (response.status() == LockStatus.FAILED)
                ? HttpStatus.NOT_FOUND
                : HttpStatus.OK;

        return ResponseEntity.status(httpStatus).body(response);
    }
}
