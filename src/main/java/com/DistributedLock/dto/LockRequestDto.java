package com.DistributedLock.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LockRequestDto {

    @NotBlank(message = "Lock name cannot be empty")
    private String lockName;
}
