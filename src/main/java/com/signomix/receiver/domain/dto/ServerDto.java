package com.signomix.receiver.domain.dto;

public record ServerDto(
    String name,
    double memoryTotal,
    double memoryFree,
    double cpuLoad,
    double diskTotal,
    double diskFree
) {}
