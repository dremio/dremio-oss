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

package com.dremio.service.userpreferences;

import java.util.UUID;

import com.dremio.service.userpreferences.proto.UserPreferenceProto.Preference;
import com.dremio.service.userpreferences.proto.UserPreferenceProto.PreferenceType;

/**
 * UserPreferenceService interface
 */
public interface UserPreferenceService {

  /**
   * get preference of specific preference type
   *
   * @param type
   * @return
   */
  Preference getPreferenceByType(PreferenceType type);

  /**
   * add an entity to preference list of specific preference type
   *
   * @param type
   * @param entityId
   * @return
   */
  Preference addEntityToPreference(PreferenceType type, UUID entityId)
    throws EntityAlreadyInPreferenceException, EntityThresholdReachedException;

  /**
   * remove an entity from specific preference type
   *
   * @param type
   * @param entityId
   * @return
   */
  Preference removeEntityFromPreference(PreferenceType type, UUID entityId)
    throws EntityNotFoundInPreferenceException;
}
