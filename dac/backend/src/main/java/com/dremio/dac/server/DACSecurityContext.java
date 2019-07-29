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

import java.security.Principal;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.model.usergroup.UserResourcePath;
import com.dremio.dac.model.usergroup.UserUI;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.User;

/**
 * Dac security context.
 */
public class DACSecurityContext implements SecurityContext {

  private final UserUI user;
  private final ContainerRequestContext requestContext;

  public DACSecurityContext(final UserName userName, final User user,
      ContainerRequestContext requestContext) {
    this.user = new UserUI(new UserResourcePath(userName), userName, user);
    this.requestContext = requestContext;
  }

  @Override
  public Principal getUserPrincipal() {
    return user;
  }

  @Override
  public boolean isUserInRole(String role) {
    return true;
  }

  @Override
  public boolean isSecure() {
    return requestContext.getSecurityContext().isSecure();
  }

  @Override
  public String getAuthenticationScheme() {
    return requestContext.getSecurityContext().getAuthenticationScheme();
  }

  public static SecurityContext system() {
    return new DACSecurityContext(new UserName(SystemUser.SYSTEM_USERNAME), SystemUser.SYSTEM_USER, null);
  }
}
