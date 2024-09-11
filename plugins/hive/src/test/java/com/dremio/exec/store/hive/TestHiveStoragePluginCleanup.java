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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Test that HiveStoragePlugin properly gets cleaned up with close().
 */
public class TestHiveStoragePluginCleanup {
  private MockHiveStoragePlugin plugin;

  @Before
  public void createStartedHiveStoragePlugin() {
    final HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HIVE_SERVER2_ENABLE_DOAS, true);
    final SabotContext context = mock(SabotContext.class);
    final OptionManager optionManager = mock(OptionManager.class);
    when(context.getDremioConfig()).thenReturn(DremioConfig.create());
    when(context.isCoordinator()).thenReturn(true);
    when(context.getOptionManager()).thenReturn(optionManager);

    plugin = new MockHiveStoragePlugin(hiveConf, context, "foo");
    plugin.start();
    assertEquals(1, plugin.getClientCount());
  }

  @After
  public void closePlugin() {
    if (plugin != null) {
      plugin.close();
      plugin = null;
    }
  }

  @Test
  public void testPluginCleanup() {
    plugin.hasAccessPermission("fakeUser", new NamespaceKey(ImmutableList.of("foo","bar")), null);
    assertEquals(2, plugin.getClientCount());

    plugin.close();
    plugin.verifyClosed();
  }

  @Test
  public void testHasAccessPermissionsAlreadyClosed() throws Exception {
    plugin.close();
    checkBadStateException( () ->
      plugin.hasAccessPermission("fakeUser", new NamespaceKey(ImmutableList.of("foo","bar")), null));
  }

  @Test
  public void testStoragePluginApiAlreadyClosed() throws Exception {
    plugin.close();
    checkBadStateException(
      () -> plugin.listDatasetHandles());
  }

  private static void checkBadStateException(Callable operation) throws Exception {
    try {
      operation.call();
      fail("UserException with SOURCE_BAD_STATE expected but no exception thrown.");
    } catch (UserException ex) {
      assertEquals(DremioPBError.ErrorType.SOURCE_BAD_STATE, ex.getErrorType());
    }
  }
}
