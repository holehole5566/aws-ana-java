package com.example.kafka;

public class BusinessException extends RuntimeException {
    
    public BusinessException(String message, Throwable cause) {
        super(message, cause);
    }
}
