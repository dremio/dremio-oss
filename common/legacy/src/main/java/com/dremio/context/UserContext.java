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
package com.dremio.context;

import com.dremio.common.UserConstants;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** User context. */
public class UserContext implements SerializableContext {
  public static final String DEFAULT_SERVICE_CONTEXT_ID = "77a89f85-c936-4f42-ab21-2ee90e9609b8";
  public static final String SYSTEM_USER_CONTEXT_ID = "678cc92c-01ed-4db3-9a28-d1f871042d9f";
  public static final RequestContext.Key<UserContext> CTX_KEY =
      RequestContext.newKey("user_ctx_key");
  public static final UserContext DEFAULT_SERVICE_CONTEXT =
      new UserContext(DEFAULT_SERVICE_CONTEXT_ID);
  // represents the Dremio System User ($dremio$)
  public static final UserContext SYSTEM_USER_CONTEXT = new UserContext(SYSTEM_USER_CONTEXT_ID);
  public static final String SYSTEM_USER_NAME = UserConstants.SYSTEM_USERNAME;

  // TODO(DX-63584): Change to private once the use in proxy handlers is removed.
  public static final String USER_HEADER_KEY = "x-dremio-user-key";

  private final String userId;

  public UserContext(String userId) {
    this.userId = userId;
  }

  public String getUserId() {
    return userId;
  }

  public static boolean isSystemUser() {
    return RequestContext.current().get(UserContext.CTX_KEY) != null
        && SYSTEM_USER_CONTEXT
            .getUserId()
            .equals(RequestContext.current().get(UserContext.CTX_KEY).getUserId());
  }

  @Override
  public void serialize(ImmutableMap.Builder<String, String> builder) {
    builder.put(USER_HEADER_KEY, userId);
  }

  public static class Transformer implements SerializableContextTransformer {
    @Override
    public RequestContext deserialize(final Map<String, String> headers, RequestContext builder) {
      if (headers.containsKey(USER_HEADER_KEY)) {
        return builder.with(UserContext.CTX_KEY, new UserContext(headers.get(USER_HEADER_KEY)));
      }

      return builder;
    }
  }
}
