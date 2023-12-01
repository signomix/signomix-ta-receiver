package com.signomix.receiver;

import java.util.Map;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.annotations.jaxrs.QueryParam;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

@Path("/")
@RegisterRestClient
public interface CoreSystemService {

    @GET
    Map getNewCommandId(@QueryParam String appkey, @QueryParam String eui);

}
