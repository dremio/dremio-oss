/*
 * Copyright (C) 2017 Dremio Corporation
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

import static com.dremio.dac.server.WebServer.MediaType.APPLICATION_TDS;
import static com.dremio.dac.server.WebServer.MediaType.APPLICATION_TDS_DRILL;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.WebServer;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Resource to create tableau exports for a given dataset
 *
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/tableau/{datasetPath}")
public class TableauResource {
  private final NamespaceService namespace;
  private final DatasetPath datasetPath;

  @Inject
  public TableauResource(
      NamespaceService namespace,
      @PathParam("datasetPath") DatasetPath datasetPath) {
    this.namespace = namespace;
    this.datasetPath = datasetPath;
  }

  /**
   * returns a Tableau export for the dataset
   * @return
   * @throws DatasetNotFoundException
   * @throws NamespaceException
   */
  @GET
  @Produces({APPLICATION_TDS, APPLICATION_TDS_DRILL})
  public Response get(@HeaderParam(HttpHeaders.HOST) String host) throws DatasetNotFoundException, NamespaceException {
    // make sure path exists
    DatasetConfig datasetConfig = namespace.getDataset(datasetPath.toNamespaceKey());

    ResponseBuilder builder =  Response.ok().entity(datasetConfig);
    if (host == null) {
      return builder.build();
    }

    final String hostOnly;
    int portIndex = host.indexOf(":");
    if (portIndex == -1) {
      hostOnly = host;
    } else {
      hostOnly = host.substring(0, portIndex);
    }

    builder.header(WebServer.X_DREMIO_HOSTNAME, hostOnly);

    return builder.build();
  }
}
