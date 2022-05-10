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

import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

import java.net.URI;
import java.net.URISyntaxException;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.dremio.config.DremioConfig;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.server.GenericErrorMessage;

/**
 * API for SSO redirect
 */
@RestResource
@Path("/sso_redirect")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SSORedirectResource {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SSORedirectResource.class);

  private final DremioConfig dremioConfig;

  @Inject
  public SSORedirectResource(
      DremioConfig dremioConfig
  ) {
    this.dremioConfig = dremioConfig;
  }

  @GET
  public Response ssoRedirect() {
    try {
      URI issuer = new URI(dremioConfig.getString(DremioConfig.SSO_ISSUER) + "/auth");
      String state = "W6KHckDT4k8GNj3RjMJlKtTahXUtP-hxTk3S3qm3ip4";
      String clientId = dremioConfig.getString(DremioConfig.SSO_CLIENT_ID);
      String callbackUri = dremioConfig.getString(DremioConfig.SSO_CALLBACK_URI);
      URI uri = new URI(
        issuer.getScheme(),
        issuer.getAuthority(),
        issuer.getPath(),
        "response_type=code&redirect_uri="+ callbackUri +"&state="+state+"&client_id="+clientId+"&scope=openid+email+profile",
        null);
      return Response.seeOther(uri).build();
    } catch (URISyntaxException e) {
      e.printStackTrace();
      return Response.status(UNAUTHORIZED).entity(new GenericErrorMessage(e.getMessage())).build();
    }
  }
}
