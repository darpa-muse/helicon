package com.twosixlabs.model.entities;

import java.util.HashMap;
import java.util.HashSet;

public class ProjectSearchResult extends BaseMetadata {
    public HashSet<Project> getProjectSet() {
        return projectSet;
    }

    public void setProjectSet(HashSet<Project> projectSet) {
        this.projectSet = projectSet;
    }

    private HashSet<Project> projectSet;

    public long getQueryTimeSpanMsec() {
        return queryTimeSpanMsec;
    }

    public void setQueryTimeSpanMsec(long queryTimeSpanMsec) {
        this.queryTimeSpanMsec = queryTimeSpanMsec;
    }

    private long queryTimeSpanMsec;

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    private String queryString;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    private int count;
    public ProjectSearchResult(String queryString, long queryTimeSpanMsec,
                               HashSet<Project> projectSet ){
        this.queryTimeSpanMsec = queryTimeSpanMsec;
        this.queryString = queryString;
        this.projectSet = projectSet;
        this.count = projectSet.size();
    }
}
