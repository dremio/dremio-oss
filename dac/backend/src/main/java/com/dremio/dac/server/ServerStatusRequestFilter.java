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

import com.dremio.dac.daemon.ServerHealthMonitor;
import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Response;

/** Throws 503 if server is not available */
@PreMatching
public class ServerStatusRequestFilter implements ContainerRequestFilter {
  @Inject private javax.inject.Provider<ServerHealthMonitor> serverHealthMonitor;

  public ServerStatusRequestFilter() {}

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    if (!serverHealthMonitor.get().isHealthy()) {
      requestContext.abortWith(
          Response.status(Response.Status.SERVICE_UNAVAILABLE)
              .entity(serverHealthMonitor.get().getStatus())
              .build());
    }
  }
}
