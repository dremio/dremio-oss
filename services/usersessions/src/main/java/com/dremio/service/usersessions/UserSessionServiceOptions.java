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
package com.dremio.service.usersessions;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

/** Set of options for configuring the UserSessionService */
@Options
public interface UserSessionServiceOptions {
  /** Allows users to configure UserSession lifetime (in seconds). Default to 120 minutes. */
  TypeValidators.AdminPositiveLongValidator SESSION_TTL =
      new TypeValidators.AdminPositiveLongValidator(
          "usersessions.ttl.seconds", Integer.MAX_VALUE, 120 * 60);
}
