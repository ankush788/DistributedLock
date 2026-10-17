package com.DistributedLock.controller;

import com.DistributedLock.dto.LockReleaseDto;
import com.DistributedLock.dto.LockRequestDto;
import com.DistributedLock.dto.LockResponseDto;
import com.DistributedLock.dto.LockResponseDto.LockStatus;
import com.DistributedLock.service.LockService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API Controller for distributed locking operations.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/locks")
@Tag(name = "Distributed Lock API", description = "Endpoints for acquiring and releasing distributed locks")
public class LockController {

    private final LockService lockService;

    public LockController(LockService lockService) {
        this.lockService = lockService;
    }

    @PostMapping("/acquire")
    @Operation(summary = "Acquire a distributed lock", description = "Attempts immediate atomic acquire or queues in fair FIFO order.")
    public ResponseEntity<LockResponseDto> acquireLock(@Valid @RequestBody LockRequestDto request) {
        log.info("REST: Lock acquire request for resource '{}'", request.lockName());
        LockResponseDto response = lockService.acquireLock(request.lockName());

        HttpStatus httpStatus = (response.status() == LockStatus.ACQUIRED)
                ? HttpStatus.OK
                : HttpStatus.ACCEPTED;

        return ResponseEntity.status(httpStatus).body(response);
    }

    @PostMapping("/release")
    @Operation(summary = "Release a distributed lock", description = "Releases lock atomically after verifying token ownership.")
    public ResponseEntity<LockResponseDto> releaseLock(@Valid @RequestBody LockReleaseDto request) {
        log.info("REST: Lock release request for resource '{}' [token={}]", request.lockName(), request.tokenId());
        LockResponseDto response = lockService.releaseLock(request.lockName(), request.tokenId());
        return ResponseEntity.ok(response);
    }
}
