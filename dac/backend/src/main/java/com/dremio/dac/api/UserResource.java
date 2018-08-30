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
package com.dremio.dac.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;

/**
 * User API resource
 */
@APIResource
@Secured
@RolesAllowed({"admin"})
@Path("/user")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class UserResource {
  private final UserService userGroupService;

  @Inject
  public UserResource(UserService userGroupService) {
    this.userGroupService = userGroupService;
  }

  @GET
  @Path("/{id}")
  public User getUser(@PathParam("id") String id) throws UserNotFoundException {
    return User.fromUser(userGroupService.getUser(new UID(id)));
  }

  @GET
  @Path("/by-name/{name}")
  public User getUserByName(@PathParam("name") String name) throws UserNotFoundException {
    return User.fromUser(userGroupService.getUser(name));
  }
}
