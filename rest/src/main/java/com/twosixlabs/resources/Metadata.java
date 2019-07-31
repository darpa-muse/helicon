package com.twosixlabs.resources;

import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.CorpusMetadata;
import com.twosixlabs.model.entities.UniqueCorpusMetadata;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashSet;

@Path("muse")
public class Metadata extends ResourceBase{
    String ver;

    @GET
    @Path("metadata/{key}/pagesize/{pageSize}/page/{page}/unique")
    @Produces(MediaType.APPLICATION_JSON)
    public UniqueCorpusMetadata serveUniqueProjectMetadataValues(@PathParam("key") String key,
                                                                 @PathParam("pageSize")  Long pageSize,
                                                                 @PathParam("page") Long page) {
        HashSet<String> values = new HashSet<>();
        ScanWrapper.getProjectMetadata(null, key, pageSize, page, true).stream().forEach(v -> values.add(v.toString()));

        return setupUniqueMetadataValues(key, values);
    }

    @GET
    @Path("metadata/{key}/pagesize/{pageSize}/page/{page}")
    @Produces(MediaType.APPLICATION_JSON)
    public CorpusMetadata serveProjectMetadataValues(@PathParam("key") String key,
                                                     @PathParam("pageSize")  Long pageSize,
                                                     @PathParam("page") Long page) {
        ArrayList<String> values = new ArrayList<>();
        ScanWrapper.getProjectMetadata(null, key, pageSize, page, true).stream().forEach(v -> values.add(v.toString()));
        return setupMetadataValues(key, values);
    }

    @GET
    @Path("metadata/keys/pagesize/{pageSize}/page/{page}")
    @Produces(MediaType.APPLICATION_JSON)
    public UniqueCorpusMetadata serveCorpusMetadataKeys(@PathParam("pageSize")  Long pageSize,
                                                        @PathParam("page") Long page){
        return setupUniqueMetadataValues("keys", ScanWrapper.getMetadataKeys(pageSize, page));
    }
}
