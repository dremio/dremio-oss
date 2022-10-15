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
package com.dremio.service.users;

import com.dremio.common.UserConstants;
import com.dremio.service.users.proto.UID;

/**
 * Class with info of the system user running the Dremio service. You can't login using the system user
 * credentials, but user can be used to assign access controls for system storage plugins etc.
 */
public class SystemUser {
  /**
   * System user running the Dremio service.
   */
  public static final String SYSTEM_USERNAME = UserConstants.SYSTEM_USERNAME;
  public static final UID SYSTEM_UID = new UID(UserConstants.SYSTEM_ID);

  public static final User SYSTEM_USER =
      SimpleUser.newBuilder()
          .setUID(SYSTEM_UID) // Same UID always.
          .setUserName(SYSTEM_USERNAME)
          .setCreatedAt(System.currentTimeMillis())
          .setEmail("dremio@dremio.net")
          .build();

  /**
   * Determines if the username is the system username
   *
   * @param username the name to check
   * @return
   */
  public static boolean isSystemUserName(String username) {
    return SYSTEM_USERNAME.equals(username);
  }

  /**
   * Determines if the UID is the system UID
   *
   * @param uid the UID to check
   * @return
   */
  public static boolean isSystemUID(UID uid) {
    return SYSTEM_UID.equals(uid);
  }
}
