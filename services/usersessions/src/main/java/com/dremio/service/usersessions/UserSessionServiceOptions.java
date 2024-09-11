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
  /**
   * Session option managers get evicted from cache given TTL * multiplier. This is to account for
   * changes in the TTL, the assumption is that more than 10x increase from default will not happen.
   */
  int SESSION_TTL_BUFFER_MULTIPLIER = 10;

  /** Allows users to configure UserSession lifetime (in seconds). Default to 120 minutes. */
  TypeValidators.AdminPositiveLongValidator SESSION_TTL =
      new TypeValidators.AdminPositiveLongValidator(
          "usersessions.ttl.seconds", Integer.MAX_VALUE, 120 * 60);

  /** Allows users to configure maximum number of items in session option manager cache. */
  TypeValidators.AdminPositiveLongValidator MAX_SESSION_OPTION_MANAGERS =
      new TypeValidators.AdminPositiveLongValidator(
          "usersessions.max_option_managers", 500000L, 10000L);
}
