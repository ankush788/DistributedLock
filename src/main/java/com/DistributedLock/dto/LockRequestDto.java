package com.DistributedLock.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
public class LockRequestDto {

    public LockRequestDto(String lockName) {
        this.lockName = lockName;
    }

    /** Unique identifier of the shared resource to lock (e.g. inventory-123). */
    @NotBlank(message = "Lock name cannot be empty")
    private String lockName;
}
