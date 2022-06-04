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

/**
 * User context.
 */
public class UserContext {
  public static final RequestContext.Key<UserContext> CTX_KEY = RequestContext.newKey("user_ctx_key");
  public static final UserContext DEFAULT_SERVICE_CONTEXT = new UserContext("77a89f85-c936-4f42-ab21-2ee90e9609b8");
  // represents the Dremio System User ($dremio$)
  public static final UserContext SYSTEM_USER_CONTEXT = new UserContext("678cc92c-01ed-4db3-9a28-d1f871042d9f");

  private final String userId;

  public UserContext(String userId) {
    this.userId = userId;
  }

  public String getUserId() {
    return userId;
  }

  public String serialize() {
    return userId;
  }

  public static boolean isSystemUser() {
    return RequestContext.current().get(UserContext.CTX_KEY) != null
      && SYSTEM_USER_CONTEXT.getUserId().equals(RequestContext.current().get(UserContext.CTX_KEY).getUserId());
  }
}
