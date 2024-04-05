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

import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import java.lang.reflect.InvocationHandler;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import org.glassfish.jersey.server.model.Invocable;
import org.glassfish.jersey.server.spi.internal.ResourceMethodInvocationHandlerProvider;

/**
 * The ContextualizedResourceMethodInvocationHandlerProvider extracts the UserContext from
 * attributes within the HttpServletRequest which is set by the DACAuthFilter.
 */
public class ContextualizedResourceMethodInvocationHandlerProvider
    implements ResourceMethodInvocationHandlerProvider {
  public static final String USER_CONTEXT_ATTRIBUTE =
      ContextualizedResourceMethodInvocationHandlerProvider.class.getCanonicalName()
          + ".UserContext";

  @Context private HttpServletRequest httpServletRequest;

  @Override
  public InvocationHandler create(Invocable invocable) {
    return (proxy, method, args) ->
        RequestContext.current().with(getRequestContext()).call(() -> method.invoke(proxy, args));
  }

  private Map<RequestContext.Key<?>, Object> getRequestContext() {
    final Map<RequestContext.Key<?>, Object> contextMap = new HashMap<>();

    contextMap.put(UserContext.CTX_KEY, httpServletRequest.getAttribute(USER_CONTEXT_ATTRIBUTE));

    return contextMap;
  }
}
