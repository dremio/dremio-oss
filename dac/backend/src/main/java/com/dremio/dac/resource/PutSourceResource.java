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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.dac.service.errors.SourceNotFoundException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.source.proto.SourceConfig;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Rest resource for sources. */
@RestResource
@Secured
@RolesAllowed({"admin"})
@Path("/source/{sourceName}")
public class PutSourceResource {
  private static final Logger logger = LoggerFactory.getLogger(PutSourceResource.class);

  private final SourceService sourceService;
  private final SourceName sourceName;
  private final SourcePath sourcePath;

  @Inject
  public PutSourceResource(
      SourceService sourceService, @PathParam("sourceName") SourceName sourceName)
      throws SourceNotFoundException {
    this.sourceService = sourceService;
    this.sourceName = sourceName;
    this.sourcePath = new SourcePath(sourceName);
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public SourceUI putSource(SourceUI source) throws NamespaceException, SourceNotFoundException {

    try {
      // Following are set at server side.
      source.setName(sourceName.getName());

      SourceConfig sourceConfig = sourceService.registerSourceWithRuntime(source);

      final SourceState sourceState =
          sourceService.getSourceState(sourcePath.getSourceName().getName());
      if (sourceState == null) {
        throw new SourceNotFoundException(sourcePath.getSourceName().getName());
      }
      source.setState(sourceState);
      source.setTag(sourceConfig.getTag());
      source.setId(sourceConfig.getId().getId());
      source.setCtime(sourceConfig.getCtime());
      source.clearSecrets();
      return source;
    } catch (ExecutionSetupException e) {
      // if we are getting an ExecutionSetupException it will most likely contain a "useful" error
      // message
      throw UserExceptionMapper.withStatus(
              UserException.resourceError(e), Response.Status.INTERNAL_SERVER_ERROR)
          .message("Failure while attempting to register source: %s", e.getMessage())
          .build(logger);
    }
  }
}
