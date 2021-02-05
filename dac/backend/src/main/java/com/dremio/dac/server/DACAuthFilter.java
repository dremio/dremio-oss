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
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

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

/**
 * Read cookie from request and validate it.
 */
@Secured
@Provider
@Priority(Priorities.AUTHENTICATION)
public class DACAuthFilter implements ContainerRequestFilter {

  @Inject
  private javax.inject.Provider<UserService> userService;
  @Inject
  private TokenManager tokenManager;
  @Inject
  private ResourceInfo resourceInfo;

  public DACAuthFilter() {
  }

  /**
   * Validates if the requester user is authorized and exists.
   * <p>
   * If the user is not found or is unauthorized, aborts the request
   * with an unauthorized response.
   *
   * @param requestContext the request context instance
   */
  @Override
  public void filter(ContainerRequestContext requestContext) {
    try {
      final UserName userName = getUserNameFromToken(requestContext);
      final User userConfig = userService.get().getUser(userName.getName());
      requestContext.setSecurityContext(new DACSecurityContext(userName, userConfig, requestContext));
    } catch (UserNotFoundException | NotAuthorizedException e) {
      requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
    }
  }

  /**
   * Gets the username from the request token.
   * <p>
   * If the token is a temporary access resource:
   * <ol>
   *   <li>Tries to retrieve the token from the query parameters and if it is not present
   *    tries to retrieve it from the authorization header;</li>
   *    <li>Validates the temporary token;</li>
   * </ol>
   * If the token is not a temporary resource or if the temporary token validation failed:
   * <ol>
   *   <li>Gets the token directly from the authorization header;</li>
   *   <li>Validates the token;</li>
   * </ol>
   * Then, if everything is validated properly, returns the token related username,
   * otherwise, a NotAuthorizedException will be thrown.
   *
   * @param requestContext the request context instance
   * @return UserName the token related username
   * @throws NotAuthorizedException If the token validation fails
   */
  protected UserName getUserNameFromToken(ContainerRequestContext requestContext) throws NotAuthorizedException {
    final UserName userName;

    try {
      TokenDetails tokenDetails;

      if (resourceInfo.getResourceMethod().isAnnotationPresent(TemporaryAccess.class)) {
        String temporaryToken = TokenUtils.getTemporaryToken(requestContext);
        if (temporaryToken != null) {
          tokenDetails = tokenManager.validateTemporaryToken(
            temporaryToken,
            requestContext.getUriInfo().getRequestUri().getPath(),
            requestContext.getUriInfo().getQueryParameters());
        } else {
          temporaryToken = TokenUtils.getToken(requestContext.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
          try {
            tokenDetails = tokenManager.validateTemporaryToken(
              temporaryToken,
              requestContext.getUriInfo().getRequestUri().getPath(),
              requestContext.getUriInfo().getQueryParameters());
          } catch (IllegalArgumentException e) {
            tokenDetails = tokenManager.validateToken(temporaryToken);
          }
        }
      } else {
        String token = TokenUtils.getToken(requestContext.getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
        tokenDetails = tokenManager.validateToken(token);
      }

      TokenInfo.setContext(requestContext, tokenDetails);
      userName = new UserName(tokenDetails.username);
      return userName;
    } catch (IllegalArgumentException e) {
      throw new NotAuthorizedException(e);
    }
  }

}
