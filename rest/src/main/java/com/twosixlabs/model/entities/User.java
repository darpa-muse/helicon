package com.twosixlabs.model.entities;

import java.time.YearMonth;
import java.util.List;

public class User {
    String apiKey;

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public List<String> getReadPeers() {
        return readPeers;
    }

    public void setReadPeers(List<String> readPeers) {
        this.readPeers = readPeers;
    }

    public List<String> getWritePeers() {
        return writePeers;
    }

    public void setWritePeers(List<String> writePeers) {
        this.writePeers = writePeers;
    }

    String alias;
    List<String> readPeers;
    List<String> writePeers;
    String role;

    YearMonth created;

    public YearMonth getExpires() {
        return expires;
    }

    public void setExpires(YearMonth expires) {
        this.expires = expires;
    }

    YearMonth expires;

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public YearMonth getCreated() {
        return created;
    }

    public void setCreated(YearMonth created) {
        this.created = created;
    }
/*
    System.out.printf("Days in month year %s: No of days: %s \n",
    currentYearMonth, currentYearMonth.lengthOfMonth());
    YearMonth creditCardExpiry = YearMonth.of(2018, Month.FEBRUARY);
    System.out.printf("Your credit card expires on %s: No of days: %s \n",
    creditCardExpiry, creditCardExpiry.lengthOfMonth());
    long
    int expiration; // in hours
    */
}
