package com.signomix.receiver.domain.dto;

public enum CheckStatus {
    OK(1.0),
    ERROR(0.0);

    private final double value;

    CheckStatus(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }
}
