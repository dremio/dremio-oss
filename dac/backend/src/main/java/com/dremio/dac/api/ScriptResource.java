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

package com.dremio.dac.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.apache.parquet.Preconditions;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.scripts.PaginatedResponse;
import com.dremio.dac.model.scripts.ScriptData;
import com.dremio.service.script.DuplicateScriptNameException;
import com.dremio.service.script.ScriptNotAccessible;
import com.dremio.service.script.ScriptNotFoundException;
import com.dremio.service.script.ScriptService;

@APIResource
@Secured
@Path("/scripts")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
public class ScriptResource {
  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(ScriptResource.class);
  private final ScriptService scriptService;
  private final SecurityContext securityContext;

  @Inject
  public ScriptResource(ScriptService scriptService, @Context SecurityContext securityContext) {
    this.scriptService = scriptService;
    this.securityContext = securityContext;
  }

  @GET
  public PaginatedResponse<ScriptData> getScripts(@QueryParam("offset") Integer offset,
                                                  @QueryParam("maxResults") Integer maxResults,
                                                  @QueryParam("search") String search,
                                                  @QueryParam("orderBy") String orderBy) {
    // validations and assigning default values
    offset = (offset == null) ? 0 : offset;
    maxResults = (maxResults == null) ? 25 : Math.min(maxResults, 1000);
    search = (search == null) ? "" : search;
    orderBy = (orderBy == null) ? "" : orderBy;

    Long totalScripts = scriptService.getCountOfMatchingScripts(search, "");
    List<ScriptData> scripts = scriptService.getScripts(offset, maxResults, search, orderBy, "")
      .stream()
      .map(ScriptData::fromScript)
      .collect(Collectors.toList());
    return new PaginatedResponse<>(totalScripts, scripts);
  }

  @POST
  public ScriptData postScripts(ScriptData scriptData) {
    try {
      return ScriptData.fromScript(scriptService.createScript(ScriptData.toScriptRequest(scriptData)));
    } catch (DuplicateScriptNameException exception) {
      throw new BadRequestException(exception.getMessage());
    }
  }

  @GET
  @Path("/{id}")
  public ScriptData getScript(@PathParam("id") String scriptId) {
    // check if scriptId is valid;
    validateScriptId(scriptId);
    try {
      return ScriptData.fromScript(scriptService.getScriptById(scriptId));
    } catch (ScriptNotFoundException exception) {
      throw new NotFoundException(exception.getMessage());
    } catch (ScriptNotAccessible exception) {
      throw new NotAuthorizedException(exception.getMessage());
    }
  }

  private void validateScriptId(String scriptId) {
    Preconditions.checkNotNull(scriptId, "scriptId must be provided.");
    try {
      UUID.fromString(scriptId);
    } catch (IllegalArgumentException exception) {
      throw new BadRequestException("scriptId must be valid UUID.");
    }
  }

  @PUT
  @Path("/{id}")
  public ScriptData updateScript(@PathParam("id") String scriptId, ScriptData scriptData) {
    // check if scriptId is valid
    validateScriptId(scriptId);
    // check if script exists with given scriptId
    try {
      // update the script
      return ScriptData.fromScript(scriptService.updateScript(scriptId,
                                                              ScriptData.toScriptRequest(scriptData)));
    } catch (ScriptNotFoundException exception) {
      throw new NotFoundException(exception.getMessage());
    } catch (DuplicateScriptNameException exception) {
      throw new BadRequestException(exception.getMessage());
    } catch (ScriptNotAccessible exception) {
      throw new NotAuthorizedException(exception.getMessage());
    }
  }

  @DELETE
  @Path(("/{id}"))
  public Response deleteScript(@PathParam("id") String scriptId) {
    validateScriptId(scriptId);
    try {
      scriptService.deleteScriptById(scriptId);
      return Response.noContent().build();
    } catch (ScriptNotFoundException exception) {
      throw new NotFoundException(exception.getMessage());
    } catch (ScriptNotAccessible exception) {
      throw new NotAuthorizedException(exception.getMessage());
    }
  }

}
