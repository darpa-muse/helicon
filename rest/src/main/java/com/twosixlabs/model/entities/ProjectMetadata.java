package com.twosixlabs.model.entities;

import java.util.HashMap;

public class ProjectMetadata extends Project {
    public ProjectMetadata(){}

    private HashMap<String, String> metadata;

    public ProjectMetadata(String absoluteUrl, String projectRow) {
        super(absoluteUrl, projectRow);
    }
    public ProjectMetadata(String s) {
        super(s);
    }

    public HashMap<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(HashMap<String, String> metadata) {
        this.metadata = metadata;
    }
}
