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
 * The Username of the User.
 *
 * TODO:
 * Note that there is ongoing working to add the user's username to UserContext;
 * Refer to DX-51988: Introduce username to UserContext.
 * Once the ticket is completed, this class is unnecessary and should be removed in lieu of
 * simply using the username in the UserContext.
 * Refer to DX-59840: Remove UsernameContext once username is included in UserContext.
 */
public class UsernameContext {
  public static final RequestContext.Key<UsernameContext> CTX_KEY = RequestContext.newKey("user_name_ctx_key");

  private final String userName;

  public UsernameContext(String userName) {
    this.userName = userName;
  }

  public String getUserName() {
    return userName;
  }
}
