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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

import com.dremio.catalog.model.CatalogEntityId;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.factory.CatalogSupplier;
import com.dremio.service.userpreferences.proto.UserPreferenceProto;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;
import java.security.Principal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.core.SecurityContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests for UserPreference service */
public class TestUserPreferencesServiceImpl {
  private static final SabotConfig DEFAULT_SABOT_CONFIG = SabotConfig.forClient();
  private static final ScanResult CLASSPATH_SCAN_RESULT =
      ClassPathScanner.fromPrescan(DEFAULT_SABOT_CONFIG);
  private static final String USER_NAME_1 = "user1";
  private static final String USER_ID_1 = "87605f99-88da-45b1-8668-09bd0cfa8528";

  private UserPreferenceStore userPreferenceStore;
  private LocalKVStoreProvider kvStoreProvider;
  private UserPreferenceService userPreferenceService;
  private CatalogSupplier catalogSupplier;
  private Catalog catalog;
  private UserService userService;

  @Before
  public void setUp() throws Exception {

    kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();

    userPreferenceStore = new UserPreferenceStoreImpl(() -> kvStoreProvider);
    userPreferenceStore.start();

    catalog = Mockito.mock(Catalog.class);
    catalogSupplier = Mockito.mock(CatalogSupplier.class);
    Mockito.doReturn(catalog).when(catalogSupplier).get();
    userService = Mockito.mock(UserService.class);

    SecurityContext securityContext = Mockito.mock(SecurityContext.class);
    Principal principal = Mockito.mock(Principal.class);
    Mockito.doReturn(USER_NAME_1).when(principal).getName();
    Mockito.doReturn(principal).when(securityContext).getUserPrincipal();

    userPreferenceService =
        new UserPreferenceServiceImpl(
            () -> userPreferenceStore, catalogSupplier, userService, securityContext);
  }

  @After
  public void cleanUp() throws Exception {
    kvStoreProvider.close();
  }

  //  try to get starred entity when user hasn't starred any entity yet. and assert empty list is
  // returned.
  @Test
  public void testStarEntity()
      throws EntityThresholdReachedException,
          EntityAlreadyInPreferenceException,
          IllegalAccessException {
    try (MockedStatic<RequestContext> mocked = mockCurrentUser()) {
      mockUserService();
      mockValidEntity();

      UserPreferenceProto.Preference preference =
          userPreferenceService.getPreferenceByType(UserPreferenceProto.PreferenceType.STARRED);
      // assert when user doesn't have preference added, returns an empty preference list.
      Assert.assertEquals(0, preference.getEntitiesCount());

      UUID entityId1 = UUID.randomUUID();
      // when an entityId is added in preference list.
      preference =
          userPreferenceService.addEntityToPreference(
              UserPreferenceProto.PreferenceType.STARRED, entityId1);

      // assert added entry is present.
      Assert.assertTrue(
          getIdsFromEntities(preference.getEntitiesList()).contains(entityId1.toString()));
    } catch (UserNotFoundException exception) {
      Assert.fail("User not found exception thrown.");
    }
  }

  private List<String> getIdsFromEntities(List<UserPreferenceProto.Entity> entitiesList) {
    return entitiesList.stream()
        .map(UserPreferenceProto.Entity::getEntityId)
        .collect(Collectors.toList());
  }

  private MockedStatic<RequestContext> mockCurrentUser() {
    RequestContext requestContext =
        RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext);
    return mocked;
  }

  private void mockValidEntity() {
    Mockito.doReturn(true).when(catalog).existsById(any());
  }

  private void mockUserService() throws UserNotFoundException {
    Mockito.doReturn(
            SimpleUser.newBuilder().setUserName(USER_NAME_1).setUID(new UID(USER_ID_1)).build())
        .when(userService)
        .getUser(USER_NAME_1);
  }

  private void mockNoUser() throws UserNotFoundException {
    Mockito.doThrow(new UserNotFoundException("User Exception"))
        .when(userService)
        .getUser(USER_NAME_1);
  }

  @Test
  public void testStarTwoEntities()
      throws EntityThresholdReachedException,
          EntityAlreadyInPreferenceException,
          IllegalAccessException {
    // star two entites and assert both are present and
    // assert correct order i.e second entity comes after first
    try (MockedStatic<RequestContext> mocked = mockCurrentUser()) {
      mockUserService();
      mockValidEntity();

      // when two entites are added one after the other
      UUID entityId1 = UUID.randomUUID();
      userPreferenceService.addEntityToPreference(
          UserPreferenceProto.PreferenceType.STARRED, entityId1);
      UUID entityId2 = UUID.randomUUID();
      UserPreferenceProto.Preference preference =
          userPreferenceService.addEntityToPreference(
              UserPreferenceProto.PreferenceType.STARRED, entityId2);

      List<String> entityIds =
          preference.getEntitiesList().stream()
              .map(UserPreferenceProto.Entity::getEntityId)
              .collect(Collectors.toList());
      // assert both are present in preference list
      Assert.assertTrue(entityIds.contains(entityId1.toString()));
      Assert.assertTrue(entityIds.contains(entityId2.toString()));

      // and validate order of entity1 and entity2
      int index1 = -1;
      int index2 = -1;
      for (int i = 0; i < entityIds.size(); i++) {
        if (Objects.equals(entityIds.get(i), entityId1.toString())) {
          index1 = i;
        } else if (Objects.equals(entityIds.get(i), entityId2.toString())) {
          index2 = i;
        }
      }

      Assert.assertTrue(index1 < index2);
    } catch (UserNotFoundException e) {
      Assert.fail("UserNotFoundException thrown.");
    }
  }

  @Test
  public void testStarDuplicateEntity()
      throws EntityThresholdReachedException,
          EntityAlreadyInPreferenceException,
          IllegalAccessException {
    try (MockedStatic<RequestContext> mocked = mockCurrentUser()) {
      mockUserService();
      mockValidEntity();

      UUID entityId1 = UUID.randomUUID();
      userPreferenceService.addEntityToPreference(
          UserPreferenceProto.PreferenceType.STARRED, entityId1);
      // when an entity is starred twice
      assertThatThrownBy(
              () ->
                  userPreferenceService.addEntityToPreference(
                      UserPreferenceProto.PreferenceType.STARRED, entityId1))
          .isInstanceOf(EntityAlreadyInPreferenceException.class)
          .hasMessage(String.format("entityId : %s already exists in starred list.", entityId1));
    } catch (UserNotFoundException e) {
      Assert.fail("UserNotFoundException thrown.");
    }
  }

  @Test
  public void testStarAndUnstarEntity()
      throws EntityThresholdReachedException,
          EntityAlreadyInPreferenceException,
          EntityNotFoundInPreferenceException,
          IllegalAccessException {
    try (MockedStatic<RequestContext> mocked = mockCurrentUser()) {
      mockUserService();
      mockValidEntity();

      // when entity is starred and unstarred
      UUID entityId1 = UUID.randomUUID();
      userPreferenceService.addEntityToPreference(
          UserPreferenceProto.PreferenceType.STARRED, entityId1);
      UserPreferenceProto.Preference preference =
          userPreferenceService.removeEntityFromPreference(
              UserPreferenceProto.PreferenceType.STARRED, entityId1);
      // assert after unstaring entity is removed
      Assert.assertFalse(
          getIdsFromEntities(preference.getEntitiesList()).contains(entityId1.toString()));
    } catch (UserNotFoundException e) {
      Assert.fail("UserNotFoundException thrown.");
    }
  }

  @Test
  public void testUnstarNonexistingEntity() {
    try (MockedStatic<RequestContext> mocked = mockCurrentUser()) {
      mockUserService();
      mockValidEntity();

      UUID entityId1 = UUID.randomUUID();
      // when non-existing entity is starred
      assertThatThrownBy(
              () ->
                  userPreferenceService.removeEntityFromPreference(
                      UserPreferenceProto.PreferenceType.STARRED, entityId1))
          .isInstanceOf(EntityNotFoundInPreferenceException.class)
          .hasMessage(String.format("entityId : %s not found in starred list.", entityId1));
    } catch (UserNotFoundException e) {
      Assert.fail("UserNotFoundException thrown.");
    }
  }

  @Test
  public void testValidateEntity() {
    try (MockedStatic<RequestContext> mocked = mockCurrentUser()) {
      mockUserService();

      Mockito.doReturn(false).when(catalog).existsById(any());

      UUID entityId1 = UUID.randomUUID();
      // when non-existing entity is starred
      assertThatThrownBy(
              () ->
                  userPreferenceService.addEntityToPreference(
                      UserPreferenceProto.PreferenceType.STARRED, entityId1))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              String.format(
                  "entityId %s provided is not a valid catalog entity.", entityId1.toString()));
    } catch (UserNotFoundException e) {
      Assert.fail("UserNotFoundException thrown.");
    }
  }

  // when a catalog entity is deleted, it should be deleted from preference too.
  @Test
  public void testDeleteEntity()
      throws EntityAlreadyInPreferenceException,
          EntityThresholdReachedException,
          IllegalAccessException {
    try (MockedStatic<RequestContext> mocked = mockCurrentUser()) {
      mockUserService();
      mockValidEntity();

      // given a valid entity is in preference list
      UUID entityId1 = UUID.randomUUID();
      UUID entityId2 = UUID.randomUUID();
      userPreferenceService.addEntityToPreference(
          UserPreferenceProto.PreferenceType.STARRED, entityId1);
      UserPreferenceProto.Preference preference =
          userPreferenceService.addEntityToPreference(
              UserPreferenceProto.PreferenceType.STARRED, entityId2);
      Assert.assertTrue(
          getIdsFromEntities(preference.getEntitiesList()).contains(entityId1.toString()));
      Assert.assertTrue(
          getIdsFromEntities(preference.getEntitiesList()).contains(entityId2.toString()));

      // when the entity is deleted
      Mockito.doReturn(false)
          .when(catalog)
          .existsById(CatalogEntityId.fromString(entityId1.toString()));

      // assert when get is called the entity is removed from preference list.
      preference =
          userPreferenceService.getPreferenceByType(UserPreferenceProto.PreferenceType.STARRED);

      Assert.assertFalse(
          getIdsFromEntities(preference.getEntitiesList()).contains(entityId1.toString()));
      Assert.assertTrue(
          getIdsFromEntities(preference.getEntitiesList()).contains(entityId2.toString()));
    } catch (UserNotFoundException e) {
      Assert.fail("UserNotFoundException thrown.");
    }
  }

  // when a catalog entity is deleted, validate entity is deleted from KV store
  @Test
  public void testDeleteEntityFromStore()
      throws EntityAlreadyInPreferenceException,
          EntityThresholdReachedException,
          IllegalAccessException {
    try (MockedStatic<RequestContext> mocked = mockCurrentUser()) {
      mockUserService();
      mockValidEntity();

      // given 3 valid entity is added in preference list
      UUID entityId1 = UUID.randomUUID();
      UUID entityId2 = UUID.randomUUID();
      UUID entityId3 = UUID.randomUUID();

      userPreferenceService.addEntityToPreference(
          UserPreferenceProto.PreferenceType.STARRED, entityId1);
      userPreferenceService.addEntityToPreference(
          UserPreferenceProto.PreferenceType.STARRED, entityId2);
      UserPreferenceProto.Preference preference =
          userPreferenceService.addEntityToPreference(
              UserPreferenceProto.PreferenceType.STARRED, entityId3);

      List<String> entityIds = getIdsFromEntities(preference.getEntitiesList());

      Assert.assertTrue(entityIds.contains(entityId1.toString()));
      Assert.assertTrue(entityIds.contains(entityId2.toString()));
      Assert.assertTrue(entityIds.contains(entityId3.toString()));

      // when the entity is deleted and get call is made
      Mockito.doReturn(false)
          .when(catalog)
          .existsById(CatalogEntityId.fromString(entityId2.toString()));
      userPreferenceService.getPreferenceByType(UserPreferenceProto.PreferenceType.STARRED);

      // validate contents from store
      Optional<UserPreferenceProto.UserPreference> userPreference =
          userPreferenceStore.get(USER_ID_1);
      Assert.assertTrue(userPreference.isPresent());

      List<UserPreferenceProto.Preference> userPreferenceList =
          userPreference.get().getPreferencesList();

      UserPreferenceProto.Preference preferenceList = null;
      for (UserPreferenceProto.Preference preference1 : userPreferenceList) {
        if (preference1.getType() == UserPreferenceProto.PreferenceType.STARRED) {
          preferenceList = preference1;
          break;
        }
      }

      entityIds = getIdsFromEntities(preferenceList.getEntitiesList());
      // assert deleted entity is removed from store
      Assert.assertTrue(entityIds.contains(entityId1.toString()));
      Assert.assertFalse(entityIds.contains(entityId2.toString()));
      Assert.assertTrue(entityIds.contains(entityId3.toString()));
    } catch (UserNotFoundException e) {
      Assert.fail("UserNotFoundException thrown.");
    }
  }

  @Test
  public void testUserNotFoundExceptionThrown()
      throws EntityAlreadyInPreferenceException,
          EntityThresholdReachedException,
          IllegalAccessException {
    try {
      mockNoUser();
      mockValidEntity();

      UserPreferenceProto.Preference preference =
          userPreferenceService.getPreferenceByType(UserPreferenceProto.PreferenceType.STARRED);
      Assert.assertEquals(0, preference.getEntitiesCount());

      UUID entityId1 = UUID.randomUUID();
      preference =
          userPreferenceService.addEntityToPreference(
              UserPreferenceProto.PreferenceType.STARRED, entityId1);

      // assert added entry is present.
      Assert.assertTrue(
          getIdsFromEntities(preference.getEntitiesList()).contains(entityId1.toString()));
      Assert.fail("UserNotFoundException was not thrown.");
    } catch (UserNotFoundException exception) {
      // We want to fire off this exception if the user is not found.
    }
  }

  // TODO:
  //  8. star more than 25 entity
}
