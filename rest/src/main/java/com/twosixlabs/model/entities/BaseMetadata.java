package com.twosixlabs.model.entities;

import java.util.HashSet;

public class BaseMetadata {
    public BaseMetadata(){}
    protected String key;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    protected String value;
    protected String url;

    public long getElapsedTimeMsec() {
        return elapsedTimeMsec;
    }

    public void setElapsedTimeMsec(long elapsedTimeMsec) {
        this.elapsedTimeMsec = elapsedTimeMsec;
    }

    protected long elapsedTimeMsec;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getKey() {
        if (key == null) return ""; else return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

}
