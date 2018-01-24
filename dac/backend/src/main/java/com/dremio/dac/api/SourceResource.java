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
package com.dremio.dac.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.sources.SourceDefinitions;
import com.dremio.dac.service.errors.ServerErrorException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.annotations.VisibleForTesting;

/**
 * Resource for information about sources.
 */
@APIResource
@Secured
@Path("/source")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class SourceResource {
  private static final Logger logger = LoggerFactory.getLogger(SourceResource.class);

  private final SourceService sourceService;

  @Inject
  public SourceResource(SourceService sourceService) {
    this.sourceService = sourceService;
  }

  @GET
  @RolesAllowed({"admin", "user"})
  public ResponseList<Source> getSources() {
    final ResponseList<Source> sources = new ResponseList<>();

    final List<SourceConfig> sourceConfigs = sourceService.getSources();
    for (SourceConfig sourceConfig : sourceConfigs) {
      Source source = fromSourceConfig(sourceConfig);

      SourceState state = sourceService.getStateForSource(sourceConfig);
      source.setState(state);

      sources.add(source);
    }

    return sources;
  }

  @POST
  @RolesAllowed({"admin"})
  public Source addSource(Source source) {
    try {
      SourceConfig newSourceConfig = sourceService.createSource(source.toSourceConfig(), source.getConfig());

      final SourceState sourceState = sourceService.getSourceState(source.getName());

      Source resultSource = fromSourceConfig(newSourceConfig);
      resultSource.setState(sourceState);

      return resultSource;
    } catch (NamespaceException | ExecutionSetupException e) {
      throw new ServerErrorException(e);
    }
  }

  @GET
  @RolesAllowed({"admin", "user"})
  @Path("/{id}")
  public Source getSource(@PathParam("id") String id) {
    SourceConfig sourceConfig = sourceService.getById(id);
    return fromSourceConfig(sourceConfig);
  }

  @PUT
  @RolesAllowed({"admin"})
  @Path("/{id}")
  public Source updateSource(@PathParam("id") String id, Source source) {
    SourceConfig sourceConfig = null;
    try {
      sourceConfig = sourceService.updateSource(id, source.toSourceConfig(), source.getConfig());

      Source resultSource = fromSourceConfig(sourceConfig);

      final SourceState sourceState = sourceService.getSourceState(source.getName());
      resultSource.setState(sourceState);

      return resultSource;
    } catch (NamespaceException | ExecutionSetupException e) {
      throw new ServerErrorException(e);
    }
  }

  @DELETE
  @RolesAllowed("admin")
  @Path("/{id}")
  public Response deleteSource(@PathParam("id") String id) {
    SourceConfig config = sourceService.getById(id);

    try {
      sourceService.deleteSource(config);
      return Response.ok().build();
    } catch (NamespaceException e) {
      throw new ServerErrorException(e);
    }
  }

  @VisibleForTesting
  protected Source fromSourceConfig(SourceConfig sourceConfig) {
    Source source = new Source(sourceConfig);

    // we should not set fields that expose passwords and other private parts of the source
    SourceDefinitions.clearSource(source.getConfig());

    return source;
  }
}
