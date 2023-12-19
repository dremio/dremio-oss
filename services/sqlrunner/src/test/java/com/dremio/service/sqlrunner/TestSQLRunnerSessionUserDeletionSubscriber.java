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
package com.dremio.service.sqlrunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.options.OptionManager;
import com.dremio.service.users.SimpleUserService;
import com.dremio.service.users.events.UserDeletionEvent;
import com.dremio.service.users.proto.UID;
import com.dremio.service.users.proto.UserConfig;
import com.dremio.service.users.proto.UserInfo;
import com.dremio.service.users.proto.UserType;
import com.dremio.test.DremioTest;

public class TestSQLRunnerSessionUserDeletionSubscriber {

  @Before
  public void setUp() throws Exception {
    OptionManager mockOptionManager = Mockito.mock(OptionManager.class);
    when(mockOptionManager.getOption(SQLRunnerOptions.SQLRUNNER_TABS)).thenReturn(true);
  }

  @Test
  public void testSQLRunnerSessionDeletedAfterDeleteUser() throws Exception {
    try (final LegacyKVStoreProvider kvstore =
           LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      SQLRunnerSessionService sqlRunnerSessionService = mock(SQLRunnerSessionService.class);

      kvstore.start();
      final SimpleUserService userService = new SimpleUserService(() -> kvstore);
      LegacyIndexedStore<UID, UserInfo> userGroupStore = kvstore.getStore(SimpleUserService.UserGroupStoreBuilder.class);
      // save an remote user
      final UID uid = new UID(UUID.randomUUID().toString());
      final UserConfig userConfig = new UserConfig()
        .setUid(uid)
        .setUserName("testuser")
        .setCreatedAt(System.currentTimeMillis())
        .setType(UserType.LOCAL)
        .setEmail("test.user@dremio.test")
        .setFirstName("Test")
        .setLastName("User");
      userGroupStore.put(uid, new UserInfo().setConfig(userConfig));

      SQLRunnerSessionUserDeletionSubscriber subscriber = new SQLRunnerSessionUserDeletionSubscriber(() -> sqlRunnerSessionService);
      userService.subscribe(UserDeletionEvent.getEventTopic(), subscriber);

      userService.deleteUser(uid, userConfig.getTag());
      verify(sqlRunnerSessionService, times(1)).deleteSession(uid.getId());
    }
  }

}
