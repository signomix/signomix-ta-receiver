package com.signomix.receiver.domain.dto;

public record DockerContainerDto(
    String id,
    String name,
    String status,
    double memoryUsage,
    double memoryLimit,
    double cpuUsage,
    double netIn,
    double netOut,
    double blockIn,
    double blockOut
) {}
