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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.graph.DataGraph;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.dataset.DatasetVersion;

/**
 * Datagraph resources.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/datagraph/{cpath}")
public class DataGraphResource {
  private static final Logger logger = LoggerFactory.getLogger(DataGraphResource.class);

  private final DatasetVersionMutator datasetService;
  private final DatasetPath datasetPath;

  @Inject
  public DataGraphResource(DatasetVersionMutator datasetService,
                         @PathParam("cpath") DatasetPath datasetPath) {
    this.datasetPath = datasetPath;
    this.datasetService = datasetService;
  }

  @GET
  @Produces(APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public DataGraph getDataGraph(@QueryParam("currentView") DatasetPath currentView,
                                @QueryParam("searchParents") String parentFilter,
                                @QueryParam("searchChildren") String childrenFilter) throws NamespaceException {
    return datasetService.getDataGraph(datasetPath, currentView, parentFilter, childrenFilter);
  }

  @GET
  @Produces(APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{version}")
  public DataGraph getDataGraph(@PathParam("version") DatasetVersion version,
                                @QueryParam("currentView") DatasetPath currentView,
                                @QueryParam("searchParents") String parentFilter,
                                @QueryParam("searchChildren") String childrenFilter) throws NamespaceException {
    return datasetService.getDataGraph(datasetPath, version, currentView, parentFilter, childrenFilter);
  }

  @POST
  @Produces(APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{version}/recenter")
  public DataGraph getDataGraph(@PathParam("version") DatasetVersion version) throws NamespaceException {
    return datasetService.getDataGraph(datasetPath, version, null, null, null);
  }

}
