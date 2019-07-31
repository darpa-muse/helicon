package com.twosixlabs.model.entities;

import java.util.List;

public class UserDataRecord extends BaseMetadata {
    public List<String> getReadPeers() {
        return readPeers;
    }

    public void setReadPeers(List<String> readPeers) {
        if (readPeers != null)
            this.readPeers = readPeers;
    }

    public List<String> getWritePeers() {
        return writePeers;
    }

    public void setWritePeers(List<String> writePeers) {
        if (writePeers != null)
            this.writePeers = writePeers;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        if (owner != null)
            this.owner = owner;
    }

    /*
    {
        “owner”: “ray.garcia@twosixlabs.com”,
        “readPeers”: [“david.slater@twosixlabs.com”,
                               “ben .gelman@twosixlabs.com”],
        “writePeers”: [“david.slater@twosixlabs.com”],
         “apiKey”: “readability”
         “value”: “.4”
    }
         */
    List<String> readPeers;
    List<String> writePeers;
    String owner;
    public void update(UserDataRecord rec){
        setOwner(rec.getOwner());
        setReadPeers(rec.getReadPeers());
        setWritePeers(rec.getWritePeers());
    }
}
