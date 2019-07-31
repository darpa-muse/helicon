package com.twosixlabs.model.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Iterator;
import java.util.Map;

public class IteratorWrapper {
    Iterator<Map.Entry<Key, Value>> iterator;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    long count;
    public IteratorWrapper(Iterator<Map.Entry<Key, Value>> scannerIterator, long count){
        this.iterator = scannerIterator;
    }

    public boolean hasNext(){
        return iterator.hasNext();
    }

    public Map.Entry<Key, Value> next(){
        count++;
        return iterator.next();
    }
}
