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

import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.config.DremioConfig;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.dac.model.usergroup.SessionPermissions;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.server.tokens.TokenDetails;
import com.dremio.dac.server.tokens.TokenManager;
import com.dremio.dac.server.tokens.TokenUtils;
import com.dremio.dac.support.SupportService;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserLoginException;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.google.common.base.Strings;

/**
 * API for user log in and log out.
 */
@RestResource
@Path("/login")
public class LogInLogOutResource {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogInLogOutResource.class);

  private final DremioConfig dremioConfig;
  private final SabotContext dContext;
  private final UserService userService;
  private final SupportService support;
  private final TokenManager tokenManager;

  @Inject
  public LogInLogOutResource(
      DremioConfig dremioConfig,
      SabotContext dContext,
      UserService userService,
      SupportService support,
      TokenManager tokenManager) {
    this.dremioConfig = dremioConfig;
    this.dContext = dContext;
    this.userService = userService;
    this.support = support;
    this.tokenManager = tokenManager;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response login(UserLogin userLogin, @Context HttpServletRequest request) {
    try {
      if (userLogin == null || Strings.isNullOrEmpty(userLogin.getUserName()) || Strings.isNullOrEmpty(userLogin.getPassword())) {
        throw new IllegalArgumentException("user name or password cannot be null or empty");
      }

      final UserName userName = new UserName(userLogin.getUserName());
      // Authenticate the user using the credentials provided
      userService.authenticate(userName.getName(), userLogin.getPassword());

      final User userConfig = userService.getUser(userName.getName());
      final String clientAddress = request.getRemoteAddr();

      // Get a token for this session
      final TokenDetails tokenDetails = tokenManager.createToken(userLogin.getUserName(), clientAddress);

      // Make sure the logged-in user has a home space. If not create one.
      try {
        final NamespaceService ns = dContext.getNamespaceService(SystemUser.SYSTEM_USERNAME);
        final NamespaceKey homeKey = new HomePath(HomeName.getUserHomePath(userName.getName())).toNamespaceKey();
        try {
          ns.getHome(homeKey);
        } catch (NamespaceNotFoundException nnfe) {
          // create home
          ns.addOrUpdateHome(homeKey,
              new HomeConfig().setCtime(System.currentTimeMillis()).setOwner(userName.getName())
          );
        }
      } catch (NamespaceException ex) {
        logger.error("Failed to make sure the user has home space setup.", ex);
        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new GenericErrorMessage(ex.getMessage())).build();
      }

      final OptionManager opt = dContext.getOptionManager();
      SessionPermissions perms = new SessionPermissions(
          opt.getOption(SupportService.USERS_UPLOAD),
          opt.getOption(SupportService.USERS_DOWNLOAD),
          opt.getOption(SupportService.USERS_EMAIL),
          opt.getOption(SupportService.USERS_CHAT)
          );

      return Response.ok(
          new UserLoginSession(
              tokenDetails.token,
              userLogin.getUserName(),
              userConfig.getFirstName(),
              userConfig.getLastName(),
              tokenDetails.expiresAt,
              userConfig.getEmail(),
              null,
              true,
              userConfig.getCreatedAt(),
              support.getClusterId().getIdentity(),
              support.getClusterId().getCreated(),
              "internal".equals(dremioConfig.getString(DremioConfig.WEB_AUTH_TYPE)),
              DremioVersionInfo.getVersion(),
              perms
              )
          ).build();
    } catch (IllegalArgumentException | UserLoginException | UserNotFoundException e) {
      return Response.status(UNAUTHORIZED).entity(new GenericErrorMessage(e.getMessage())).build();
    }
  }

  @DELETE
  public void logout(@HeaderParam(HttpHeaders.AUTHORIZATION) String authHeader) {
    tokenManager.invalidateToken(TokenUtils.getTokenFromAuthHeader(authHeader));
  }
}
