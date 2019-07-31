package com.twosixlabs.model.entities;

import java.util.HashSet;

public class UniqueCorpusMetadata extends BaseMetadata{
    private HashSet<String> values;
    private Long count;

    public HashSet<String> getValues() {
        return values;
    }

    public void setValues(HashSet<String> values) {
        this.values = values;
    }

    public Long getCount() {
        return Long.valueOf(values.size());
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
