package com.twosixlabs.muse_utils;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class EMapper implements ExceptionMapper<RuntimeException> {
    public Response toResponse(RuntimeException exception)
    {
        return Response.status(Response.Status.NOT_FOUND)
                .entity(exception.getMessage())
                .type("text/plain").build();
    }

}
