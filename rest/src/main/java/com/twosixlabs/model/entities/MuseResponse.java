package com.twosixlabs.model.entities;

public class MuseResponse extends BaseMetadata{
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    String message;
}
