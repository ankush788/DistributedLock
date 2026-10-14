package com.DistributedLock.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
public class LockReleaseDto {

    public LockReleaseDto(String lockName, String tokenId) {
        this.lockName = lockName;
        this.tokenId = tokenId;
    }

    /** Name of the locked resource to be released. */
    @NotBlank(message = "Lock name cannot be empty")
    private String lockName;

    /** Ownership token previously issued when lock was acquired (verifies caller owns the lock). */
    @NotBlank(message = "Token ID cannot be empty")
    private String tokenId;
}
