package com.signomix.receiver.adapter.in;

public class ReceiverException extends Exception{
    public static int UNKNOWN = 1000;
    public static int DELAYED = 1;
    
    private String message;
    private int code;
    
    public ReceiverException(int code){
        this.code = code;
        switch (code){
            case 1000:
            default:
                message = "unknown error";
                break;
        }
    }
    
    public ReceiverException(int code, String message){
        this.code = code;
        this.message = message;
    }
    
    public String getMessage(){
        return message;
    }
    
    public int getCode(){
        return code;
    }

}
