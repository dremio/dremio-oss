/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.dac.resource;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.uri.UriComponent;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.server.tokens.TokenInfo;
import com.dremio.service.tokens.TokenManager;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Resource for generating temporary tokens
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/temp-token")
public class TemporaryTokenResource {

  @Inject private TokenInfo tokenInfo;
  private final TokenManager tokenManager;

  @Inject
  public TemporaryTokenResource(TokenManager tokenManager) {
    this.tokenManager = tokenManager;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response createTemporaryToken(@QueryParam("request") String request,
                                       @QueryParam("durationSeconds") long duration) {
    if (request == null || duration <= 0) {
      return Response.status(Status.BAD_REQUEST).build();
    }
    final URI requestUri;
    try {
      requestUri = new URI(request).normalize();
      if (requestUri.getPath() == null) {
        throw new URISyntaxException(request, "Path is undefined.");
      }
    } catch (URISyntaxException e) {
      return Response.status(Status.BAD_REQUEST).build();
    }

    final long sessionRemainingTime = tokenInfo.getExpiresAt();
    // custom duration cannot exceed session remaining time
    final long adjustedDurationMillis = Math.min(TimeUnit.SECONDS.toMillis(duration), sessionRemainingTime - System.currentTimeMillis());
    if (adjustedDurationMillis <= 0) {
      return Response.status(Status.UNAUTHORIZED).build();
    }

    String token = tokenManager.createTemporaryToken(tokenInfo.getUsername(),
      requestUri.getPath(), UriComponent.decodeQuery(requestUri, true),
      adjustedDurationMillis).token;
    return Response.ok().entity(new TempTokenResponse(token)).build();
  }

  /**
   * Temporary token sent to UI.
   */
  public static final class TempTokenResponse {
    private final String token;

    public TempTokenResponse(@JsonProperty("token") String token) {
      this.token = token;
    }

    public String getToken() {
      return token;
    }
  }

}
