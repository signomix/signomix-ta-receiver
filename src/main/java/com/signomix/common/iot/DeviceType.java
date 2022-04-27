package com.signomix.common.iot;

public enum DeviceType {
    CHIRPCSTACK(0),
    GENERIC(1),
    TTN(2),
    KPN(3),
    VIRTUAL(4);

    private int type;

    DeviceType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static DeviceType getByType(int type) {
        switch (type) {
            case 0:
                return DeviceType.CHIRPCSTACK;
            case 1:
                return DeviceType.GENERIC;
            case 2:
                return DeviceType.TTN;
            case 3:
                return DeviceType.KPN;
            case 4:
                return DeviceType.VIRTUAL;
            default:
                return null;
        }
    }
}
