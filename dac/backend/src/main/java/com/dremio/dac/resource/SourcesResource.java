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
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.Sources;
import com.dremio.dac.service.source.SourceService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.source.proto.SourceConfig;

/**
 * Resource for information about sources.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/sources")
@Consumes(APPLICATION_JSON) @Produces(APPLICATION_JSON)
public class SourcesResource {
  private static final Logger logger = LoggerFactory.getLogger(SourcesResource.class);

  private final NamespaceService namespaceService;
  private final SourceService sourceService;

  @Inject
  public SourcesResource(NamespaceService namespaceService, SourceService sourceService) {
    this.namespaceService = namespaceService;
    this.sourceService = sourceService;
  }

  @GET
  public Sources getSources() throws Exception {
    final Sources sources = new Sources();
    for (SourceConfig sourceConfig : sourceService.getSources()) {
      SourceUI source = newSource(sourceConfig);

      source.setNumberOfDatasets(namespaceService.getAllDatasetsCount(new NamespaceKey(source.getName())));

      SourceState state = sourceService.getStateForSource(sourceConfig);
      source.setState(state);

      sources.add(source);
    }
    return sources;
  }

  protected SourceUI newSource(SourceConfig sourceConfig) throws Exception {
    return SourceUI.get(sourceConfig);
  }
}
