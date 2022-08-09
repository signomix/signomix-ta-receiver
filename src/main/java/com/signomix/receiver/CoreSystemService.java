package com.signomix.receiver;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.annotations.jaxrs.QueryParam;

@Path("/")
@RegisterRestClient
public interface CoreSystemService {

    @GET
    Map getNewCommandId(@QueryParam String appkey, @QueryParam String eui);

}
