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
package com.dremio.exec.store.hive;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import com.dremio.exec.server.SabotContext;
import com.dremio.service.users.SystemUser;

/**
 * Tests for Hive3StoragePlugin
 */
public class TestHive3StoragePlugin {
  private static final String TEST_USER_NAME = "testUser";

  @Test
  public void impersonationDisabledShouldReturnSystemUser() {
    final HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HIVE_SERVER2_ENABLE_DOAS, false);
    final SabotContext context = mock(SabotContext.class);

    final Hive3StoragePlugin plugin = createHiveStoragePlugin(hiveConf, context);

    final String userName = plugin.getUsername(TEST_USER_NAME);
    assertEquals(SystemUser.SYSTEM_USERNAME, userName);
  }

  @Test
  public void impersonationEnabledShouldReturnUser() {
    final HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HIVE_SERVER2_ENABLE_DOAS, true);
    final SabotContext context = mock(SabotContext.class);

    final Hive3StoragePlugin plugin = new Hive3StoragePlugin(hiveConf, context, "foo");

    final String userName = plugin.getUsername(TEST_USER_NAME);
    assertEquals(TEST_USER_NAME, userName);
  }

  protected Hive3StoragePlugin createHiveStoragePlugin(HiveConf hiveConf, SabotContext context) {
    return new Hive3StoragePlugin(hiveConf, context, "foo");
  }
}
