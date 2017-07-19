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
package com.dremio.dac.server;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.dremio.service.users.UserService;

/**
 * Special filter that precedes all Jersey requests (except requests for bootstrap) and aborts if no user available
 * returning a 403 (FORBIDDEN) response status along with an entity with an errorMessage field as defined in
 * {@link GenericErrorMessage#NO_USER_MSG}.
 */
@Priority(Priorities.AUTHENTICATION - 1) //It's not the best way to do this, but we need to "force" this filter to run before the JerseyAuthFilter
public class NoUserFilter implements ContainerRequestFilter {

  private final AtomicBoolean hasAnyUser = new AtomicBoolean(false);
  private final UserService userService;

  @Inject
  public NoUserFilter(UserService userService) {
    this.userService = userService;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    // we cache in the local filter as we want to avoid expensive lookups. This means if we delete all data, we'll actually misreport has any user.
    if(hasAnyUser.get()){
      return;
    }

    if(userService.hasAnyUser()){
      hasAnyUser.set(true);
      return;
    }

    handle(requestContext);
  }

  public static void handle(ContainerRequestContext requestContext) {
    final UriInfo info = requestContext.getUriInfo();
    final String path = info.getPath();
    final boolean openUrl = path.startsWith("bootstrap/firstuser") || path.startsWith("test/create");

    if(openUrl){
      return;
    }

    requestContext.abortWith(Response.status(Response.Status.FORBIDDEN)
        .entity(new GenericErrorMessage(GenericErrorMessage.NO_USER_MSG))
        .build());
  }
}
