/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.dremio.dac.annotations.Bootstrap;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.daemon.ServerHealthMonitor;
import com.dremio.dac.model.system.ServerStatus;

/**
 * Check server status.
 */
@Bootstrap
@RestResource
@Path("/server_status")
public class ServerStatusResource {

  private final Provider<ServerHealthMonitor> serverHealthMonitor;

  @Inject
  public ServerStatusResource(Provider<ServerHealthMonitor> serverHealthMonitor) {
    this.serverHealthMonitor = serverHealthMonitor;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatus() {
    ServerStatus serverStatus = serverHealthMonitor.get().getStatus();
    if (serverStatus != ServerStatus.OK) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(serverStatus).build();
    } else {
      return Response.ok().entity(serverStatus).build();
    }
  }
}
