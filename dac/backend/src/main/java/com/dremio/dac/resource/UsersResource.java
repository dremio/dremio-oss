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

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.model.usergroup.UserResourcePath;
import com.dremio.dac.model.usergroup.UserUI;
import com.dremio.dac.model.usergroup.UsersUI;
import com.dremio.service.users.User;
import com.dremio.service.users.UserService;
import java.io.IOException;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/** List/search users/admins. */
@RestResource
@Secured
@RolesAllowed("admin")
@Path("/users")
public class UsersResource {

  private final UserService userService;

  @Inject
  public UsersResource(UserService userService) {
    this.userService = userService;
  }

  @GET
  @Path("all")
  @Produces(MediaType.APPLICATION_JSON)
  public UsersUI getAllUsers() throws IOException {
    final UsersUI users = new UsersUI();
    for (final User userConfig : userService.getAllUsers(null)) {
      final UserName userName = new UserName(userConfig.getUserName());
      users.add(new UserUI(new UserResourcePath(userName), userName, userConfig));
    }
    return users;
  }

  @GET
  @Path("search")
  @Produces(MediaType.APPLICATION_JSON)
  public UsersUI getAllUsers(@QueryParam("filter") String query) throws IOException {
    final UsersUI users = new UsersUI();
    for (final User userConfig : userService.searchUsers(query, null, null, null)) {
      final UserName userName = new UserName(userConfig.getUserName());
      users.add(new UserUI(new UserResourcePath(userName), userName, userConfig));
    }
    return users;
  }
}
