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

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.Bootstrap;
import com.dremio.dac.model.common.DACUnauthorizedException;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.dac.model.usergroup.UserForm;
import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.model.usergroup.UserResourcePath;
import com.dremio.dac.model.usergroup.UserUI;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Bootstrap resource only available on master node when no user already exists
 *
 * <p>Note: should not be automatically registered!
 */
@Bootstrap
@Path("/bootstrap")
public class BootstrapResource {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BootstrapResource.class);

  public static final String ERROR_MSG =
      "First user can only be created when no user is already registered";
  private final SabotContext dContext;
  private final UserService userService;

  @Inject
  public BootstrapResource(SabotContext dContext, UserService userService) {
    this.dContext = dContext;
    this.userService = userService;
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("firstuser")
  public UserUI createUser(UserForm userForm)
      throws IOException,
          IllegalArgumentException,
          NamespaceException,
          DACUnauthorizedException,
          UserNotFoundException {

    synchronized (this) {
      if (dContext.getUserService().hasAnyUser()) {
        throw UserException.validationError().message(ERROR_MSG).build(logger);
      }

      final UserName userName = new UserName(userForm.getUserConfig().getUserName());
      User newUser =
          SimpleUser.newBuilder(userForm.getUserConfig())
              .setCreatedAt(System.currentTimeMillis())
              .setActive(true)
              .build();
      newUser = userService.createUser(newUser, userForm.getPassword());
      dContext
          .getNamespaceService(SystemUser.SYSTEM_USERNAME)
          .addOrUpdateHome(
              new HomePath(HomeName.getUserHomePath(userName.getName())).toNamespaceKey(),
              new HomeConfig().setCtime(System.currentTimeMillis()).setOwner(userName.getName()));
      return new UserUI(new UserResourcePath(userName), userName, newUser);
    }
  }
}
