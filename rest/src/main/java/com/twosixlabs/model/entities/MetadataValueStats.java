package com.twosixlabs.model.entities;

import com.twosixlabs.muse_utils.App;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.math3.util.FastMath;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import static com.twosixlabs.model.accumulo.ScanWrapper.getProjectMetadata;
import static java.util.Arrays.stream;

public class MetadataValueStats extends BaseMetadata {

    public double getVariance() {
        return variance;
    }

    public void setVariance(double variance) {
        this.variance = variance;
    }

    private double variance;

    public String getName() {
        return name;
    }

    private String name;

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    double min;

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getStd() {
        return std;
    }

    public void setStd(double std) {
        this.std = std;
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    double max;
    double std;
    double mean;
    double sum;
    double count;

    public double getMode() {
        return mode;
    }

    public void setMode(double mode) {
        this.mode = mode;
    }

    double mode;

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    private void initialize(String name, String metadataKey, double factor){
        initialize( null, name, metadataKey, factor);
    }

    private void initialize(String row, String name, String metadataKey, double factor){
        double[] data = stream(getProjectMetadata(row, metadataKey, Long.MAX_VALUE, 1L, true).stream().mapToLong(v -> Long.parseLong(v.toString())).toArray()).mapToDouble(l -> l).toArray();
        this.name = name;
        key = metadataKey;

        mean = StatUtils.mean(data)/factor;
        variance = StatUtils.variance(data)/factor;
        std = FastMath.sqrt(variance)/factor;
        min = StatUtils.min(data)/factor;
        max = StatUtils.max(data)/factor;
        sum = StatUtils.sum(data)/factor;
        mode = (StatUtils.mode(data)[0])/factor;
        count = data.length;
    }

    private void initializeWithArray(HashSet<String> rows, String name, String metadataKey, double factor){

        try {
            ArrayList<Double> vals = new ArrayList<Double>();
            for (String row : rows) {
                double[] temp = stream(getProjectMetadata(row, metadataKey, Long.MAX_VALUE, 1L, true).stream().mapToLong(v -> Long.parseLong(v.toString())).toArray()).mapToDouble(l -> l).toArray();

                for (double d : temp)
                    vals.add(d);
            }
            double[] data = vals.stream().mapToDouble(d -> d).toArray();
            this.name = name;
            key = metadataKey;

            mean = StatUtils.mean(data)/factor;
            variance = StatUtils.variance(data)/factor;
            std = FastMath.sqrt(variance)/factor;
            min = StatUtils.min(data)/factor;
            max = StatUtils.max(data)/factor;
            sum = StatUtils.sum(data)/factor;
            mode = (StatUtils.mode(data)[0])/factor;
            count = data.length;
        } catch (MathIllegalArgumentException e) {
            App.logException(e);
        }
    }
    public MetadataValueStats(String name,String metadataKey){
        initialize(name, metadataKey, 1.0);
    }
    public MetadataValueStats(String row, String name,String metadataKey){
        initialize(row, name, metadataKey, 1.0);
    }

    public MetadataValueStats(HashSet<String> rows, String name,String metadataKey){
        initializeWithArray(rows, name, metadataKey, 1.0);
    }


    public MetadataValueStats(String row, String name, String metadataKey, double factor) {
        initialize(row, name, metadataKey, factor);
    }
    public MetadataValueStats(HashSet<String> rows, String name, String metadataKey, double factor) {
        initializeWithArray(rows, name, metadataKey, factor);
    }

    public MetadataValueStats(String name, String metadataKey, double factor) {
        initialize(name, metadataKey, factor);
    }

    public MetadataValueStats(){}

    public MetadataValueStats(double[] data){
        mean = StatUtils.mean(data);
        variance = StatUtils.variance(data);
        std = FastMath.sqrt(variance);
        min = StatUtils.min(data);
        max = StatUtils.max(data);
        sum = StatUtils.sum(data);
        mode = (StatUtils.mode(data)[0]);
        count = data.length;
    }

    public void recalc(MetadataValueStats mvStats){
        this.count += mvStats.count;
        this.key = mvStats.key;
        this.name = mvStats.name;

        this.min = Double.min(this.min, mvStats.min);
        this.max = Double.max(this.max, mvStats.max);
    }
}
