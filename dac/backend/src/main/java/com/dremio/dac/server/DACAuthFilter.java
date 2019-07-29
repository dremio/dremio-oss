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
package com.dremio.dac.server;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.server.tokens.TokenManager;
import com.dremio.dac.server.tokens.TokenUtils;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;

/**
 * Read cookie from request and validate it.
 */
@Secured
@Provider
@Priority(Priorities.AUTHENTICATION)
public class DACAuthFilter implements ContainerRequestFilter {

  @Inject private javax.inject.Provider<UserService> userService;
  @Inject private TokenManager tokenManager;

  public DACAuthFilter() {
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    try {
      final String token = TokenUtils.getTokenFromAuthHeaderOrQueryParameter(requestContext);
      final UserName userName;
      try {
        userName = new UserName(tokenManager.validateToken(token).username);
      } catch (final IllegalArgumentException e) {
        throw new NotAuthorizedException(e);
      }
      final User userConfig = userService.get().getUser(userName.getName());
      requestContext.setSecurityContext(new DACSecurityContext(userName, userConfig, requestContext));
    } catch (UserNotFoundException | NotAuthorizedException e) {
      requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
    }
  }
}
