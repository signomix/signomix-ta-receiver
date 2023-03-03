package com.signomix.receiver.application.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Provider
public class ServiceExceptionHandler implements ExceptionMapper<ServiceException> {

    @ConfigProperty(name ="com.signomix.receiver.exception.api.param.missing")
    String apiParamMissing;


    @Override
    public Response toResponse(ServiceException e) {

        if (e.getMessage().equalsIgnoreCase(apiParamMissing)) {
            return Response.status(Response.Status.BAD_REQUEST).entity(new ErrorMessage(e.getMessage()))
                    .build();
        //}else if (e.getMessage().equalsIgnoreCase(userNotAuthorizedException)) {
        //    return Response.status(Response.Status.FORBIDDEN).entity(new ErrorMessage(e.getMessage()))
        //            .build();
        } else {
            return Response.status(Response.Status.BAD_REQUEST).entity(new ErrorMessage(e.getMessage()))
                    .build();
        }
    }
}
