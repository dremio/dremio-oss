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

import static com.dremio.dac.server.ContextualizedResourceMethodInvocationHandlerProvider.USER_CONTEXT_ATTRIBUTE;

import java.util.List;
import java.util.Map;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import com.dremio.common.collections.Tuple;
import com.dremio.context.UserContext;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.annotations.TemporaryAccess;
import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.server.tokens.TokenInfo;
import com.dremio.dac.server.tokens.TokenUtils;
import com.dremio.service.tokens.TokenDetails;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.google.common.base.Preconditions;

/**
 * Read cookie from request and validate it.
 */
@Secured
@Provider
@Priority(Priorities.AUTHENTICATION)
public class DACAuthFilter implements ContainerRequestFilter {

  @Inject private javax.inject.Provider<UserService> userService;
  @Inject private TokenManager tokenManager;
  @Inject private ResourceInfo resourceInfo;

  public DACAuthFilter() {
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    try {
      final UserName userName = getUserNameFromToken(requestContext);
      final User userConfig = userService.get().getUser(userName.getName());
      requestContext.setSecurityContext(new DACSecurityContext(userName, userConfig, requestContext));
      requestContext.setProperty(USER_CONTEXT_ATTRIBUTE, new UserContext(userConfig.getUID().getId()));
    } catch (UserNotFoundException | NotAuthorizedException e) {
      requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
    }
  }

  /**
   * If temporary access resource:
   *   1. get temp token from query param and validate temp token
   *   2. if not present fallback to header, then try validate temp token. If fails, fall back to validate token
   * If not a temporary resource:
   *   1. get token from auth header and validate token
   * @param requestContext request-specific information
   * @return UserName
   * @throws NotAuthorizedException if validation fails
   */
  protected UserName getUserNameFromToken(ContainerRequestContext requestContext) throws NotAuthorizedException {
    final UserName userName;
    try {
      TokenDetails tokenDetails;
      final String uriPath = requestContext.getUriInfo().getRequestUri().getPath();
      final Map<String, List<String>> queryParams = requestContext.getUriInfo().getQueryParameters();
      if (resourceInfo.getResourceMethod() != null && resourceInfo.getResourceMethod().isAnnotationPresent(TemporaryAccess.class)) {
        String temporaryToken = TokenUtils.getTemporaryToken(requestContext);
        if (temporaryToken != null) {
          tokenDetails = tokenManager.validateTemporaryToken(
            temporaryToken,
            uriPath,
            queryParams);
        } else {
          final Tuple<TokenUtils.TokenType, String> tokenTuple = TokenUtils.getAuthHeaderToken(requestContext);
          Preconditions.checkArgument(tokenTuple != null);
          temporaryToken = tokenTuple.second;
          try {
            tokenDetails = tokenManager.validateTemporaryToken(
              temporaryToken,
              uriPath,
              queryParams);
          } catch (IllegalArgumentException e) {
            tokenDetails = tokenManager.validateToken(temporaryToken);
          }
        }
      } else {
        final Tuple<TokenUtils.TokenType, String> tokenTuple = TokenUtils.getAuthHeaderToken(requestContext);
        Preconditions.checkArgument(tokenTuple != null);
        tokenDetails = tokenManager.validateToken(tokenTuple.second);
      }

      TokenInfo.setContext(requestContext, tokenDetails);
      userName = new UserName(tokenDetails.username);
      return userName;
    } catch (IllegalArgumentException e) {
      throw new NotAuthorizedException(e);
    }
  }

}
