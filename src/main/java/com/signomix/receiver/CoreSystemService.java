package com.signomix.receiver;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import java.util.Map;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@Path("/")
@RegisterRestClient
public interface CoreSystemService {
    @GET
    Map getNewCommandId(
        @QueryParam("appkey") String appkey,
        @QueryParam("eui") String eui
    );
}
