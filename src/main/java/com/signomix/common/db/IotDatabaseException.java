package com.signomix.common.db;

import java.sql.SQLException;

public class IotDatabaseException extends Exception {
    public static int SQL_EXCEPTION = 500;
    public static int NOT_FOUND = 404;
    public static int CONFLICT = 409;
    public static int UNKNOWN = 1000;
    
    private String message;
    private int code;
    private SQLException sqlException=null;
    
    public IotDatabaseException(int code){
        this.code = code;
        switch (code){
            case 1000:
            default:
                message = "unknown error";
                break;
        }
    }
    
    public IotDatabaseException(int code, String message, SQLException sqlException){
        this.code = code;
        this.message = message;
        this.sqlException=sqlException;
    }
    
    public String getMessage(){
        return message;
    }
    
    public int getCode(){
        return code;
    }
}
