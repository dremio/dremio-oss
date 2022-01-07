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
 * Support context.
 */
public class SupportContext {
  public static final RequestContext.Key<SupportContext> CTX_KEY = RequestContext.newKey("support_ctx_key");

  // Note: This refers to the UserID field held within the UserContext,
  // but this constant only appears if the SupportContext is set.
  public static final String SUPPORT_USER_ID = "$Dremio-Support-Super-Admin-User$";

  public static final String SUPPORT_USER_NAME = "support@dremio.com";

  private final String ticket;
  private final String email;

  public SupportContext(String ticket, String email) {
    this.ticket = ticket;
    this.email = email;
  }

  public String getTicket() {
    return ticket;
  }

  public String getEmail() {
    return email;
  }

  /**
   * Checks if this user belongs to support role
   */
  public static boolean isSupportUser() {
    return RequestContext.current().get(SupportContext.CTX_KEY) != null
      && RequestContext.current().get(UserContext.CTX_KEY) != null
      && SupportContext.SUPPORT_USER_ID.equals(RequestContext.current().get(UserContext.CTX_KEY).getUserId());
  }

}
