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

import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.sqlrunner.SQLRunnerSessionJson;
import com.dremio.service.sqlrunner.LastTabException;
import com.dremio.service.sqlrunner.SQLRunnerSession;
import com.dremio.service.sqlrunner.SQLRunnerSessionService;
import com.dremio.service.sqlrunner.TabNotFoundException;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/** Resource for SQL Runner */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/sql-runner")
public class SQLRunnerResource {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SQLRunnerResource.class);
  private final SQLRunnerSessionService sqlRunnerSessionService;

  @Inject
  public SQLRunnerResource(Provider<SQLRunnerSessionService> sqlRunnerSessionServiceProvider) {
    this.sqlRunnerSessionService = sqlRunnerSessionServiceProvider.get();
  }

  @GET
  @Path("session")
  @Produces(MediaType.APPLICATION_JSON)
  public SQLRunnerSessionJson getSqlRunnerSession() {
    final String userId = RequestContext.current().get(UserContext.CTX_KEY).getUserId();
    SQLRunnerSession session = sqlRunnerSessionService.getSession(userId);
    return new SQLRunnerSessionJson(session);
  }

  @PUT
  @Path("session")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public SQLRunnerSessionJson putSqlRunnerSession(SQLRunnerSessionJson update) {
    final String userId = RequestContext.current().get(UserContext.CTX_KEY).getUserId();
    SQLRunnerSession updatedSession =
        new SQLRunnerSession(userId, update.getScriptIds(), update.getCurrentScriptId());
    SQLRunnerSession session = sqlRunnerSessionService.updateSession(updatedSession);
    return new SQLRunnerSessionJson(session);
  }

  @PUT
  @Path("session/tabs/{scriptId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public SQLRunnerSessionJson putSqlRunnerSessionTab(@PathParam("scriptId") String scriptId) {
    final String userId = RequestContext.current().get(UserContext.CTX_KEY).getUserId();
    SQLRunnerSession session = sqlRunnerSessionService.newTab(userId, scriptId);
    return new SQLRunnerSessionJson(session);
  }

  @DELETE
  @Path("session/tabs/{scriptId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteSqlRunnerSessionTab(@PathParam("scriptId") String scriptId) {
    final String userId = RequestContext.current().get(UserContext.CTX_KEY).getUserId();
    try {
      sqlRunnerSessionService.deleteTab(userId, scriptId);
      return Response.noContent().build();
    } catch (TabNotFoundException ex) {
      return Response.noContent().build();
    } catch (LastTabException ex) {
      logger.error(ex.getMessage(), ex);
      throw new ForbiddenException(ex.getMessage());
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      throw new InternalServerErrorException(ex.getMessage());
    }
  }
}
