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

import static java.lang.String.format;

import java.io.IOException;
import java.util.ConcurrentModificationException;

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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.model.common.DACUnauthorizedException;
import com.dremio.dac.model.usergroup.UserForm;
import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.model.usergroup.UserResourcePath;
import com.dremio.dac.model.usergroup.UserUI;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.users.UserServiceHelper;
import com.dremio.dac.util.ResourceUtil;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;

/**
 * Rest resource for users.
 */
@RestResource
@Secured
@Path("/user/{userName}")
public class UserResource {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserResource.class);

  private final UserService userService;
  private final NamespaceService namespaceService;
  private final UserServiceHelper userServiceHelper;
  private final SecurityContext securityContext;
  private final HomeFileTool fileStore;

  @Inject
  public UserResource(UserService userService, NamespaceService namespaceService, HomeFileTool fileStore,
                      UserServiceHelper userServiceHelper, @Context SecurityContext securityContext) {
    this.userService = userService;
    this.namespaceService = namespaceService;
    this.fileStore = fileStore;
    this.userServiceHelper = userServiceHelper;
    this.securityContext = securityContext;
  }

  private void checkUser(UserName userName, String action) throws DACUnauthorizedException {
    if (!securityContext.isUserInRole("admin") && !securityContext.getUserPrincipal().getName().equals(userName.getName())) {
      throw new DACUnauthorizedException(format("User %s is not allowed to %s user %s",
        securityContext.getUserPrincipal().getName(), action, userName.getName()));
    }
  }

  @RolesAllowed({"admin", "user"})
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public UserUI getUser(@PathParam("userName") UserName userName) throws UserNotFoundException, DACUnauthorizedException {
    return new UserUI(new UserResourcePath(userName), userName, userService.getUser(userName.getName()));
  }

  @RolesAllowed({"admin", "user"})
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Deprecated
  public UserUI updateUser(UserForm userForm, @PathParam("userName") UserName userName)
    throws IOException, IllegalArgumentException, NamespaceException, UserNotFoundException, DACUnauthorizedException {
    checkUser(userName, "update");

    User userConfig = userForm.getUserConfig();
    if (userConfig != null && userConfig.getUserName() != null && !userConfig.getUserName().equals(userName.getName())) {
      final UserName newUserName = new UserName(userForm.getUserConfig().getUserName());
      userConfig = userService.updateUserName(userName.getName(),
        newUserName.getName(),
        userConfig, userForm.getPassword());
      // TODO: rename home space and all uploaded files along with it
      // new username
      return new UserUI(new UserResourcePath(newUserName), newUserName, userConfig);
    } else {
      User newUser = SimpleUser.newBuilder(userForm.getUserConfig()).setUserName(userName.getName()).build();
      newUser = userService.updateUser(newUser, userForm.getPassword());
      return new UserUI(new UserResourcePath(userName), userName, newUser);
    }
  }

  @RolesAllowed("admin")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Deprecated
  public UserUI createUser(UserForm userForm, @PathParam("userName") UserName userName)
    throws IOException, IllegalArgumentException, NamespaceException, DACUnauthorizedException {
    checkUser(userName, "create");
    User newUser = com.dremio.dac.api.UserResource.addUser(userForm.getUserConfig(), userForm.getPassword(),
      userService, namespaceService);

    return new UserUI(new UserResourcePath(userName), userName, newUser);
  }

  @RolesAllowed("admin")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteUser(@PathParam("userName") UserName userName, @QueryParam("version") String version) throws IOException, UserNotFoundException {
    if (version == null) {
      throw new ClientErrorException("missing version parameter");
    }

    if (securityContext.getUserPrincipal().getName().equals(userName.getName())) {
      return Response.status(Status.FORBIDDEN).entity(
          new GenericErrorMessage("Deletion of the user account of currently logged in user is not allowed.")).build();
    }

    try {
      if (!userServiceHelper.deleteUser(userName.getName(), version)) {
        return Response.serverError().build();
      }
    } catch (ConcurrentModificationException e) {
      throw ResourceUtil.correctBadVersionErrorMessage(e, "user", userName.getName());
    }

    return Response.ok().build();
  }
}
