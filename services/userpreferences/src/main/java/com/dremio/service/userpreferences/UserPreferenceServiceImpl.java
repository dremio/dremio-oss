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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.core.SecurityContext;

import com.dremio.service.userpreferences.proto.UserPreferenceProto;
import com.dremio.service.userpreferences.proto.UserPreferenceProto.Preference;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.google.common.base.Preconditions;

/**
 * UserPreferenceService implementation
 */
public class UserPreferenceServiceImpl implements UserPreferenceService {

  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(UserPreferenceServiceImpl.class);

  private static final Long MAX_COUNT_OF_ENTITIES = 25L;

  private final UserPreferenceStore userPreferenceStore;
  private final UserService userService;
  private final SecurityContext securityContext;

  @Inject
  public UserPreferenceServiceImpl(Provider<UserPreferenceStore> userPreferenceStoreProvider,
                                   UserService userService,
                                   SecurityContext securityContext) {
    userPreferenceStore = userPreferenceStoreProvider.get();
    this.userService = userService;
    this.securityContext = securityContext;
  }

  @Override
  public UserPreferenceProto.Preference getPreferenceByType(UserPreferenceProto.PreferenceType type) {
    Optional<UserPreferenceProto.UserPreference> userPreference =
      userPreferenceStore.get(getCurrentUserId().toString());
    if (userPreference.isPresent()) {
      int index = getIndexOfPreferenceType(userPreference.get(), type);
      if (index != -1) {
        return userPreference.get().getPreferences(index);
      }
    }
    // if there is no items in preference, return empty list of preference.
    return newPreference(type, null);
  }

  @Override
  public UserPreferenceProto.Preference addEntityToPreference(UserPreferenceProto.PreferenceType type,
                                                              UUID entityId)
    throws EntityAlreadyInPreferenceException, EntityThresholdReachedException {
    String userId = getCurrentUserId().toString();
    Optional<UserPreferenceProto.UserPreference> existingUserPreference =
      userPreferenceStore.get(userId);
    if (!existingUserPreference.isPresent()) {
      // if no existing preference present
      Preference preference = newPreference(type, entityId);
      UserPreferenceProto.UserPreference createdPreference = userPreferenceStore.update(
        userId, newUserPreference(userId, preference));

      // return preference from create preference
      return createdPreference.getPreferences(getIndexOfPreferenceType(createdPreference, type));
    }

    int index = getIndexOfPreferenceType(existingUserPreference.get(), type);
    if (index == -1) {
      // if there is no preference of given type present in user preference
      UserPreferenceProto.UserPreference updatedUserPreference = userPreferenceStore.update(
        userId,
        existingUserPreference.get()
          .toBuilder()
          .addPreferences(newPreference(type, entityId))
          .build());
      return updatedUserPreference.getPreferences(getIndexOfPreferenceType(updatedUserPreference,
                                                                           type));
    }
    Preference existingPreferenceOfGivenType = existingUserPreference.get().getPreferences(index);


    Set<String> entityIds = new LinkedHashSet<>(existingPreferenceOfGivenType.getEntityIdsList());
    if (entityIds.contains(entityId.toString())) {
      // throw exception if entity already present in preference
      throw new EntityAlreadyInPreferenceException(
        entityId.toString(),
        type.name().toLowerCase(Locale.ROOT));
    }
    // add new entity to list of given preference type
    entityIds.add(entityId.toString());

    if (entityIds.size() > MAX_COUNT_OF_ENTITIES) {
      throw new EntityThresholdReachedException(MAX_COUNT_OF_ENTITIES,
                                                type.name().toLowerCase(Locale.ROOT));
    }

    Preference updatedPreference = Preference.newBuilder().addAllEntityIds(entityIds).build();

    UserPreferenceProto.UserPreference updatedUserPreference =
      existingUserPreference.get().toBuilder().setPreferences(index, updatedPreference).build();

    return userPreferenceStore.update(userId, updatedUserPreference).getPreferences(index);
  }

  private UserPreferenceProto.UserPreference newUserPreference(String userId,
                                                               Preference preference) {
    Preconditions.checkNotNull(userId, "userId must nobe null");
    UserPreferenceProto.UserPreference userPreference =
      UserPreferenceProto.UserPreference.newBuilder().setUserId(userId).build();
    if (preference != null) {
      userPreference = userPreference.toBuilder().addPreferences(preference).build();
    }
    return userPreference;
  }

  @Override
  public UserPreferenceProto.Preference removeEntityFromPreference(UserPreferenceProto.PreferenceType type,
                                                                   UUID entityId)
    throws EntityNotFoundInPreferenceException {
    String userId = getCurrentUserId().toString();
    Optional<UserPreferenceProto.UserPreference> existingUserPreference =
      userPreferenceStore.get(userId);
    if (existingUserPreference.isPresent()) {
      int index = getIndexOfPreferenceType(existingUserPreference.get(), type);
      if (index != -1) {
        Set<String> entityIds =
          new LinkedHashSet<>(existingUserPreference.get()
                                .getPreferences(index)
                                .getEntityIdsList());
        if (!entityIds.contains(entityId.toString())) {
          throw new EntityNotFoundInPreferenceException(
            entityId.toString(),
            type.name().toLowerCase(Locale.ROOT));
        }
        entityIds.remove(entityId.toString());
        UserPreferenceProto.UserPreference updatedUserPreference =
          existingUserPreference.get()
            .toBuilder()
            .setPreferences(
              index,
              Preference.newBuilder()
                .setType(type)
                .addAllEntityIds(entityIds)
                .build())
            .build();

        UserPreferenceProto.UserPreference updateUserPreference =
          userPreferenceStore.update(userId, updatedUserPreference);
        return updateUserPreference.getPreferences(index);
      }
    }
    throw new EntityNotFoundInPreferenceException(
      entityId.toString(),
      type.name().toLowerCase(Locale.ROOT));
  }

  private Preference newPreference(UserPreferenceProto.PreferenceType type, UUID entityId) {
    Preconditions.checkNotNull(type, "preference type must not be null");
    Preference preference = Preference.newBuilder().setType(type).build();
    if (entityId != null) {
      preference = preference.toBuilder().addEntityIds(entityId.toString()).build();
    }
    return preference;
  }

  private int getIndexOfPreferenceType(UserPreferenceProto.UserPreference userPreference,
                                       UserPreferenceProto.PreferenceType type) {
    List<Preference> preferenceList = userPreference.getPreferencesList();
    for (int i = 0; i < preferenceList.size(); i++) {
      if (preferenceList.get(i).getType() == type) {
        // return index of first matching preference type
        return i;
      }
    }
    // if preference of given type not present, return -1
    return -1;
  }

  private UUID getCurrentUserId() {
    try {
      return UUID.fromString(userService.getUser(securityContext.getUserPrincipal().getName())
                               .getUID()
                               .getId());
    } catch (UserNotFoundException exception) {
      logger.error("User : {} not found. Error : {}",
                   securityContext.getUserPrincipal().getName(),
                   exception.getMessage());
      return null;
    }
  }
}
