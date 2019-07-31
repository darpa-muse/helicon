package com.twosixlabs.resources;

import com.twosixlabs.model.accumulo.ScanWrapper;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;

@Path("muse/content")
public class Content extends ResourceBase {

    @GET
    @Path("projects/{project}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getFile(@PathParam("project") String project) {
        String url = ScanWrapper.getProjectCodeUrl(project);
        File file = new File(url);
        Response.ResponseBuilder response = Response.ok((Object) file);
        response.header("Content-Disposition", "attachment; filename=newfile.tgz");
        return response.build();
    }
}
