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

package com.dremio.service.userpreference;

import java.security.Principal;
import java.util.Objects;
import java.util.UUID;

import javax.ws.rs.core.SecurityContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.userpreferences.EntityAlreadyInPreferenceException;
import com.dremio.service.userpreferences.EntityNotFoundInPreferenceException;
import com.dremio.service.userpreferences.EntityThresholdReachedException;
import com.dremio.service.userpreferences.UserPreferenceService;
import com.dremio.service.userpreferences.UserPreferenceServiceImpl;
import com.dremio.service.userpreferences.UserPreferenceStore;
import com.dremio.service.userpreferences.UserPreferenceStoreImpl;
import com.dremio.service.userpreferences.proto.UserPreferenceProto;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;

/**
 * Tests for UserPreference service
 */
public class TestUserPreferencesServiceImpl {
  private static final SabotConfig DEFAULT_SABOT_CONFIG = SabotConfig.forClient();
  private static final ScanResult
    CLASSPATH_SCAN_RESULT = ClassPathScanner.fromPrescan(DEFAULT_SABOT_CONFIG);
  private static final String USER_NAME_1 = "user1";
  private static final String USER_ID_1 = "87605f99-88da-45b1-8668-09bd0cfa8528";

  private UserPreferenceStore userPreferenceStore;
  private LocalKVStoreProvider kvStoreProvider;
  private UserPreferenceService userPreferenceService;

  @Before
  public void setUp() throws Exception {

    kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();

    userPreferenceStore = new UserPreferenceStoreImpl(() -> kvStoreProvider);
    userPreferenceStore.start();

    UserService userService = Mockito.mock(UserService.class);
    Mockito.doReturn(SimpleUser.newBuilder()
                       .setUserName(USER_NAME_1)
                       .setUID(new UID(USER_ID_1))
                       .build()).when(userService).getUser(USER_NAME_1);

    SecurityContext securityContext = Mockito.mock(SecurityContext.class);
    Principal principal = Mockito.mock(Principal.class);
    Mockito.doReturn(USER_NAME_1).when(principal).getName();
    Mockito.doReturn(principal).when(securityContext).getUserPrincipal();

    userPreferenceService = new UserPreferenceServiceImpl(() -> userPreferenceStore,
                                                          userService,
                                                          securityContext);
  }

  @After
  public void cleanUp() throws Exception {
    kvStoreProvider.close();
  }

  //  try to get starred entity when user hasn't starred any entity yet. and assert empty list is returned.
  @Test
  public void testStarEntity()
    throws EntityThresholdReachedException, EntityAlreadyInPreferenceException {
    //given
    RequestContext requestContext =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext);

    UserPreferenceProto.Preference preference = userPreferenceService.getPreferenceByType(
      UserPreferenceProto.PreferenceType.STARRED);
    // assert when user doesn't have preference added, returns an empty preference list.
    Assert.assertEquals(0, preference.getEntityIdsCount());

    UUID entityId1 = UUID.randomUUID();
    // when an entityId is added in preference list.
    preference =
      userPreferenceService.addEntityToPreference(UserPreferenceProto.PreferenceType.STARRED,
                                                  entityId1);

    // assert added entry is present.
    Assert.assertTrue(preference.getEntityIdsList().contains(entityId1.toString()));
    mocked.close();
  }


  @Test
  public void testStarTwoEntities()
    throws EntityThresholdReachedException, EntityAlreadyInPreferenceException {
    // star two entites and assert both are present and
    // assert correct order i.e second entity comes after first
    RequestContext requestContext =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext);

    // when two entites are added one after the other
    UUID entityId1 = UUID.randomUUID();
    userPreferenceService.addEntityToPreference(UserPreferenceProto.PreferenceType.STARRED,
                                                entityId1);
    UUID entityId2 = UUID.randomUUID();
    UserPreferenceProto.Preference preference =
      userPreferenceService.addEntityToPreference(UserPreferenceProto.PreferenceType.STARRED,
                                                  entityId2);

    // assert both are present in preference list
    Assert.assertTrue(preference.getEntityIdsList().contains(entityId1.toString()));
    Assert.assertTrue(preference.getEntityIdsList().contains(entityId2.toString()));

    // and validate order of entity1 and entity2
    int index1 = -1;
    int index2 = -1;
    for (int i = 0; i < preference.getEntityIdsList().size(); i++) {
      if (Objects.equals(preference.getEntityIdsList().get(i), entityId1.toString())) {
        index1 = i;
      } else if (Objects.equals(preference.getEntityIdsList().get(i), entityId2.toString())) {
        index2 = i;
      }
    }

    Assert.assertTrue(index1 < index2);
    mocked.close();
  }

  @Test
  public void testStarDuplicateEntity()
    throws EntityThresholdReachedException, EntityAlreadyInPreferenceException {
    //given
    RequestContext requestContext =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext);

    UUID entityId1 = UUID.randomUUID();
    userPreferenceService.addEntityToPreference(UserPreferenceProto.PreferenceType.STARRED,
                                                entityId1);
    // when an entity is starred twice
    Exception exception = Assert.assertThrows(EntityAlreadyInPreferenceException.class,
                                              () -> userPreferenceService.addEntityToPreference(
                                                UserPreferenceProto.PreferenceType.STARRED,
                                                entityId1));
    // assert an exception is thrown
    Assert.assertEquals(String.format("entityId : %s already exists in starred list.",
                                      entityId1), exception.getMessage());
    mocked.close();
  }

  @Test
  public void testStarAndUnstarEntity()
    throws EntityThresholdReachedException, EntityAlreadyInPreferenceException, EntityNotFoundInPreferenceException {
    //given
    RequestContext requestContext =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext);

    // when entity is starred and unstarred
    UUID entityId1 = UUID.randomUUID();
    userPreferenceService.addEntityToPreference(UserPreferenceProto.PreferenceType.STARRED,
                                                entityId1);
    UserPreferenceProto.Preference preference =
      userPreferenceService.removeEntityFromPreference(UserPreferenceProto.PreferenceType.STARRED,
                                                       entityId1);
    // assert after unstaring entity is removed
    Assert.assertFalse(preference.getEntityIdsList().contains(entityId1.toString()));
    mocked.close();
  }

  @Test
  public void testUnstarNonexistingEntity() {
    //given
    RequestContext requestContext =
      RequestContext.current().with(UserContext.CTX_KEY, new UserContext(USER_ID_1));
    MockedStatic<RequestContext> mocked = Mockito.mockStatic(RequestContext.class);
    mocked.when(RequestContext::current).thenReturn(requestContext);

    UUID entityId1 = UUID.randomUUID();
    // when non-existing entity is starred
    Exception exception = Assert.assertThrows(EntityNotFoundInPreferenceException.class,
                                              () -> userPreferenceService.removeEntityFromPreference(
                                                UserPreferenceProto.PreferenceType.STARRED,
                                                entityId1));
    // assert it throws exception.
    Assert.assertEquals(String.format("entityId : %s not found in starred list.",
                                      entityId1), exception.getMessage());
    mocked.close();
  }
  // TODO:
  //  8. star more than 25 entity


}
