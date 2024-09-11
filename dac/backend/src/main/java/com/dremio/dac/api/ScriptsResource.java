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

import static com.dremio.dac.api.ScriptsAPIOptions.ENABLE_SCRIPTS_API_V3;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.scripts.PaginatedResponse;
import com.dremio.dac.model.scripts.ScriptEntity;
import com.dremio.options.OptionManager;
import com.dremio.service.script.DuplicateScriptNameException;
import com.dremio.service.script.MaxScriptsLimitReachedException;
import com.dremio.service.script.ScriptNotAccessible;
import com.dremio.service.script.ScriptNotFoundException;
import com.dremio.service.script.ScriptService;
import com.dremio.service.script.proto.ScriptProto;
import com.dremio.service.users.UserNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

/** Scripts API resource. */
@APIResource
@Secured
@RolesAllowed({"user", "admin"})
@Path("/")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
public class ScriptsResource {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ScriptsResource.class);
  private final ScriptService scriptService;
  private final OptionManager optionManager;

  @Inject
  public ScriptsResource(ScriptService scriptService, OptionManager optionManager) {
    this.scriptService = scriptService;
    this.optionManager = optionManager;
  }

  @GET
  @Path("scripts")
  public PaginatedResponse<ScriptEntity> getScripts(
      @QueryParam("offset") Integer offset,
      @QueryParam("maxResults") Integer maxResults,
      @QueryParam("search") String search,
      @QueryParam("orderBy") String orderBy,
      @QueryParam("createdBy") String createdBy) {
    preCheck();
    // validations and assigning default values
    int finalOffset = (offset == null) ? 0 : offset;
    int finalMaxResults = (maxResults == null) ? 25 : Math.min(maxResults, 1000);
    String finalSearch = (search == null) ? "" : search;
    String finalOrderBy = (orderBy == null) ? "" : orderBy;

    Long totalScripts = scriptService.getCountOfMatchingScripts(finalSearch, "", createdBy);
    List<ScriptEntity> scripts =
        scriptService
            .getScripts(finalOffset, finalMaxResults, finalSearch, finalOrderBy, "", createdBy)
            .parallelStream()
            .map(script -> convertScriptToScriptEntity(script))
            .collect(Collectors.toList());
    return new PaginatedResponse<>(totalScripts, scripts);
  }

  @POST
  @Path("scripts")
  public ScriptEntity createScript(ScriptEntity scriptEntity) {
    preCheck(
        scriptEntity != null,
        "Can't create a null script. You have to specify at least name and content.");

    try {
      ScriptProto.Script script =
          scriptService.createScript(convertScriptEntityToScriptRequest(scriptEntity, false));
      return convertScriptToScriptEntity(script);
    } catch (UserNotFoundException
        | DuplicateScriptNameException
        | MaxScriptsLimitReachedException exception) {
      throw new BadRequestException(exception.getMessage());
    }
  }

  @POST
  @Path("scripts:batchDelete")
  public BatchDeleteScriptsResponse batchDeleteScripts(BatchDeleteScriptsRequest request) {
    preCheck();
    final List<String> unauthorizedIds = new ArrayList<>();
    final List<String> notFoundIds = new ArrayList<>();
    final List<String> otherErrorIds = new ArrayList<>();
    final Set<String> processedIds = new HashSet<>();

    for (String scriptId : request.getIds()) {
      if (processedIds.contains(scriptId)) {
        continue;
      }
      processedIds.add(scriptId);

      try {
        scriptService.deleteScriptById(scriptId);
      } catch (ScriptNotFoundException exception) {
        notFoundIds.add(scriptId);
      } catch (ScriptNotAccessible exception) {
        unauthorizedIds.add(scriptId);
      } catch (Exception exception) {
        otherErrorIds.add(scriptId);
      }
    }
    return new BatchDeleteScriptsResponse(unauthorizedIds, notFoundIds, otherErrorIds);
  }

  @GET
  @Path("scripts/{id}")
  public ScriptEntity getScript(@PathParam("id") String scriptId) {
    preCheck();
    try {
      ScriptProto.Script script = scriptService.getScriptById(scriptId);
      return convertScriptToScriptEntity(script);
    } catch (ScriptNotFoundException exception) {
      throw new NotFoundException(exception.getMessage());
    } catch (ScriptNotAccessible exception) {
      throw new ForbiddenException(exception.getMessage());
    }
  }

  @PATCH
  @Path("scripts/{id}")
  public ScriptEntity updateScript(@PathParam("id") String scriptId, ScriptEntity scriptEntity) {
    preCheck(
        scriptEntity != null,
        "Nothing is updated cause the script is null. You have to specify the script attribute(s) that you want to update.");

    try {
      ScriptProto.Script script =
          scriptService.updateScript(
              scriptId, convertScriptEntityToScriptRequest(scriptEntity, true));
      return convertScriptToScriptEntity(script);
    } catch (ScriptNotFoundException exception) {
      throw new NotFoundException(exception.getMessage());
    } catch (UserNotFoundException
        | DuplicateScriptNameException
        | MaxScriptsLimitReachedException exception) {
      throw new BadRequestException(exception.getMessage());
    } catch (ScriptNotAccessible exception) {
      throw new ForbiddenException(exception.getMessage());
    }
  }

  @DELETE
  @Path("scripts/{id}")
  public Response deleteScript(@PathParam("id") String scriptId) {
    preCheck();
    try {
      scriptService.deleteScriptById(scriptId);
      return Response.noContent().build();
    } catch (ScriptNotFoundException exception) {
      throw new NotFoundException(exception.getMessage());
    } catch (ScriptNotAccessible exception) {
      throw new ForbiddenException(exception.getMessage());
    }
  }

  public void preCheck() {
    if (!optionManager.getOption(ENABLE_SCRIPTS_API_V3)) {
      throw UserException.unsupportedError().message("Scripts API is disabled!").buildSilently();
    }
  }

  public void preCheck(boolean expression, String errorMessage) {
    preCheck();
    if (!expression) {
      throw new BadRequestException(errorMessage);
    }
  }

  protected ScriptProto.ScriptRequest.Builder setScriptRequestBuilder(
      @NotNull ScriptEntity scriptEntity) {
    ScriptProto.ScriptRequest.Builder scriptRequestBuilder = ScriptProto.ScriptRequest.newBuilder();

    if (scriptEntity.getName() != null) {
      scriptRequestBuilder.setName(scriptEntity.getName());
    }
    if (scriptEntity.getContext() != null) {
      scriptRequestBuilder.addAllContext(scriptEntity.getContext());
    }
    if (scriptEntity.getContent() != null) {
      scriptRequestBuilder.setContent(scriptEntity.getContent());
    }

    return scriptRequestBuilder;
  }

  private ScriptProto.ScriptRequest convertScriptEntityToScriptRequest(
      @NotNull ScriptEntity scriptEntity, boolean isUpdate) {
    ScriptProto.ScriptRequest.Builder scriptRequestBuilder = setScriptRequestBuilder(scriptEntity);
    scriptRequestBuilder.setIsContentUpdated(isUpdate && scriptEntity.getContent() != null);
    scriptRequestBuilder.setIsContextUpdated(isUpdate && scriptEntity.getContext() != null);
    return scriptRequestBuilder.build();
  }

  private ScriptEntity convertScriptToScriptEntity(ScriptProto.Script script) {
    return new ScriptEntity(
        script.getScriptId(),
        script.getName(),
        script.getContent(),
        script.getContextList(),
        script.getCreatedAt(),
        script.getCreatedBy(),
        script.getModifiedAt(),
        script.getModifiedBy());
  }
}
