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
package com.dremio.sabot.rpc.user;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.options.OptionManager;

public class UserSessionUtils {
  public static UserSession systemSession(OptionManager options) {
    final UserBitShared.UserCredentials credentials =
        UserBitShared.UserCredentials.newBuilder().setUserName(SYSTEM_USERNAME).build();
    return UserSession.Builder.newBuilder()
        .withSessionOptionManager(
            new SessionOptionManagerImpl(options.getOptionValidatorListing()), options)
        .withCredentials(credentials)
        .exposeInternalSources(true)
        // Never check metadata validity and allow promotion only after coordinator startup
        .withCheckMetadataValidity(false)
        .withNeverPromote(false)
        .withErrorOnUnspecifiedVersion(true)
        .build();
  }
}
