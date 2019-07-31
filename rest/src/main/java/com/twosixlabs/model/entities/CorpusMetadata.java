package com.twosixlabs.model.entities;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class CorpusMetadata extends BaseMetadata {
    public ConcurrentHashMap<String, Integer> getMapValues() {
        return mapValues;
    }

    public void setMapValues(ConcurrentHashMap<String, Integer> mapValues) {
        this.mapValues = mapValues;
    }

    private ConcurrentHashMap<String, Integer> mapValues;

    public ArrayList<String> getArrayValues() {
        return arrayValues;
    }

    public void setArrayValues(ArrayList<String> arrayValues) {
        this.arrayValues = arrayValues;
    }

    private ArrayList<String> arrayValues;
    private int count;

    public int getCount() {
        return mapValues != null? mapValues.size() : arrayValues.size();
    }

    public void setCount(int count) {
        this.count = count;
    }
}
