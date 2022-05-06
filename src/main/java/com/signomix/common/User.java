package com.signomix.common;

import javax.enterprise.context.Dependent;

@Dependent
public class User {

    public Integer type;
    public String uid;
    public String email;
    public String name;
    public String surname;
    public String role;
    public Boolean confirmed;
    public Boolean unregisterRequested;
    public String confirmString;
    public String password;
    public String generalNotificationChannel = "";
    public String infoNotificationChannel = "";
    public String warningNotificationChannel = "";
    public String alertNotificationChannel = "";
    public Integer authStatus;
    public long createdAt;
    public long number;
    public int services;
    public String phonePrefix;
    public long credits;
    public boolean autologin;
    public String preferredLanguage;

    public User() {
    }

    public String[] getChannelConfig(String eventTypeName) {
        String channel = "";
        switch (eventTypeName.toUpperCase()) {
            case "GENERAL":
            case "DEVICE_LOST":
                channel = generalNotificationChannel;
                break;
            case "INFO":
                channel = infoNotificationChannel;
                break;
            case "WARNING":
                channel = warningNotificationChannel;
                break;
            case "ALERT":
                channel = alertNotificationChannel;
                break;
        }
        if (channel == null) {
            channel = "";
        }
        return channel.split(":");
    }

}
