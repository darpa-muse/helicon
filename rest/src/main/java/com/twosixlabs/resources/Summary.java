package com.twosixlabs.resources;

import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.UniqueCorpusMetadata;
import com.twosixlabs.muse_utils.App;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.HashSet;

// end points to grab summary table details
@Path("muse/summary")
public class Summary extends ResourceBase{

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





}
