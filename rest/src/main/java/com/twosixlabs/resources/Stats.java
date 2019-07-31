package com.twosixlabs.resources;

import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.CorpusMetadata;
import com.twosixlabs.model.entities.MetadataValueStats;
import com.twosixlabs.model.entities.UniqueCorpusMetadata;
import com.twosixlabs.muse_utils.App;
import org.apache.accumulo.core.data.Value;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

@Path("muse/stats")
public class Stats extends ResourceBase{

    // get the number of different values of certain metadata
    @GET
    @Path("msd/metadata")
    @Produces(MediaType.APPLICATION_JSON)
    public MetadataValueStats serveCountMetadataKeys() {
        long start = System.currentTimeMillis();
        MetadataValueStats rVal = ScanWrapper.crank();
        rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        return rVal;
    }

    // get the number of different values of certain metadata
    @GET
    @Path("count/metadata/{key}/pagesize/{pageSize}/page/{page}")
    @Produces(MediaType.APPLICATION_JSON)
    public UniqueCorpusMetadata serveCountMetadataValues(@PathParam("key") String key,
                                                         @PathParam("pageSize")  Long pageSize,
                                                         @PathParam("page") Long page) {

        HashSet<String> rVal = new HashSet<>();
        try {
            rVal.add(String.valueOf(ScanWrapper.getProjectMetadata(null, key, pageSize, page, true).size()));
        } catch (Exception e){
            App.logException(e);

        }
        return setupUniqueMetadataValues(key, rVal);
    }

    @GET
    @Path("ave/metadata/{key}/pagesize/{pageSize}/page/{page}")
    @Produces(MediaType.APPLICATION_JSON)
    public UniqueCorpusMetadata serveAveMetadataValues(@PathParam("key") String key,
                                                       @PathParam("pageSize")  Long pageSize,
                                                       @PathParam("page") Long page) {
        Double val =0.0;
        ArrayList<Value> vals = ScanWrapper.getProjectMetadata(null, key, pageSize, page, true);
        List<Double> r1 = vals.stream().map(v -> Double.parseDouble(v.toString())).collect(Collectors.toList());

        Double sum = r1.stream().reduce(0.0, Double::sum);
        val = sum/(double)(r1.size());

        HashSet<String> values = new HashSet<String>();
        values.add(String.valueOf(val));
        return setupUniqueMetadataValues(key,values);
    }

    @GET
    @Path("minmax/metadata/{key}/pagesize/{pageSize}/page/{page}")
    @Produces(MediaType.APPLICATION_JSON)
    public CorpusMetadata serveMinMaxMetadataValues(@PathParam("key") String key,
                                                    @PathParam("pageSize")  Long pageSize,
                                                    @PathParam("page") Long page) {
        ArrayList<String> values = null;

        try {
            ArrayList<Value> vals = ScanWrapper.getProjectMetadata(null, key, pageSize, page, true);
            List<Double> r1 = vals.stream().map(v -> Double.parseDouble(v.toString())).collect(Collectors.toList());

            Double vMax = r1.stream().mapToDouble(i -> i).max().getAsDouble();
            Double vMin = r1.stream().mapToDouble(i -> i).min().getAsDouble();

            values = new ArrayList<>(Arrays.asList(vMin, vMax).stream().map(d -> String.valueOf(d)).collect(toSet()));
        } catch (Exception e) {
            App.logException(e);
        }

        return setupMetadataValues(key, values);
    }
}
