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
package com.dremio.exec.ops;

import com.dremio.service.users.SystemUser;

/**
 * Reflection Context
 */
public class ReflectionContext {
  public static final ReflectionContext SYSTEM_USER_CONTEXT = new ReflectionContext(SystemUser.SYSTEM_USERNAME, true);

  private final String userName;
  private final boolean isAdmin;

  public ReflectionContext(String userName, boolean isAdmin) {
    this.userName = userName;
    this.isAdmin = isAdmin;
  }

  public String getUserName() {
    return userName;
  }

  public boolean isAdmin() {
    return isAdmin;
  }


}
