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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.io.IOException;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.job.JobFilterItem;
import com.dremio.dac.model.job.JobFilterItems;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.User;
import com.dremio.service.users.UserService;

import io.opentelemetry.instrumentation.annotations.WithSpan;


/**
 * Resource for getting lists of spaces and user for job filtering
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/jobs/filters")
public class JobsFiltersResource {

  private final NamespaceService namespaceService;
  private final UserService userService;

  @Inject
  public JobsFiltersResource(NamespaceService namespaceService, UserService userService) {
    this.namespaceService = namespaceService;
    this.userService = userService;
  }

  @WithSpan
  @GET
  @Path("/spaces")
  @Produces(APPLICATION_JSON)
  public JobFilterItems searchSpaces(@QueryParam("filter") String query, @QueryParam("limit") Integer limit) throws NamespaceException {
    final JobFilterItems spaces = new JobFilterItems();
    for (SpaceConfig spaceConfig : namespaceService.getSpaces()) {
      if (query == null || spaceConfig.getName().contains(query)) {
        spaces.add(new JobFilterItem(spaceConfig.getName(), spaceConfig.getName()));
        if (limit != null && spaces.getItems().size() >= limit) {
          break;
        }
      }
    }
    return spaces;
  }

  @WithSpan
  @GET
  @Path("/users")
  @Produces(APPLICATION_JSON)
  public JobFilterItems searchUsers(@QueryParam("filter") String query, @QueryParam("limit") Integer limit) throws IOException {
    final JobFilterItems users = new JobFilterItems();
    for (final User userConfig : userService.searchUsers(query, null, null, limit)) {
      users.add(new JobFilterItem(userConfig.getUserName(), userConfig.getUserName()));
    }
    return users;
  }


}
