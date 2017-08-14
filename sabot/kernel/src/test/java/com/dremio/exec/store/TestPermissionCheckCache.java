/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.SchemaTreeProvider.MetadataStatsCollector;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.google.common.collect.Lists;

public class TestPermissionCheckCache {

  private static final PermissionCheckCache checks = new PermissionCheckCache(10);

  @Test
  public void ensureNotCached() throws Exception {
    final String username = "ensureNotCached";
    final StoragePlugin2 plugin = mock(StoragePlugin2.class);
    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any(DatasetConfig.class)))
        .thenReturn(true);
    assertTrue(checks.hasAccess(plugin, username, new NamespaceKey(Lists.newArrayList("what")), null, null, new MetadataStatsCollector()));
    assertNull(checks.getPermissionsCache()
        .getIfPresent(new PermissionCheckCache.Key(username,
            new NamespaceKey(Lists.newArrayList("what")))));
  }

  @Test
  public void ensureCached() throws Exception {
    final String username = "ensureCached";
    final StoragePlugin2 plugin = mock(StoragePlugin2.class);
    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any(DatasetConfig.class)))
        .thenReturn(true, false);
    final MetadataPolicy policy = new MetadataPolicy().setAuthTtlMs(10_000L);
    assertTrue(checks.hasAccess(plugin, username, new NamespaceKey(Lists.newArrayList("what")), null, policy, new MetadataStatsCollector()));
    assertNotNull(checks.getPermissionsCache()
        .getIfPresent(new PermissionCheckCache.Key(username,
            new NamespaceKey(Lists.newArrayList("what")))));
    assertTrue(checks.hasAccess(plugin, username, new NamespaceKey(Lists.newArrayList("what")), null, policy, new MetadataStatsCollector()));
  }

  @Test
  public void ensureReloaded() throws Exception {
    final String username = "ensureReloaded";
    final StoragePlugin2 plugin = mock(StoragePlugin2.class);
    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any(DatasetConfig.class)))
        .thenReturn(true, false);
    final MetadataPolicy policy = new MetadataPolicy().setAuthTtlMs(500L);
    assertTrue(checks.hasAccess(plugin, username, new NamespaceKey(Lists.newArrayList("what")), null, policy, new MetadataStatsCollector()));
    assertNotNull(checks.getPermissionsCache()
        .getIfPresent(new PermissionCheckCache.Key(username,
            new NamespaceKey(Lists.newArrayList("what")))));
    Thread.sleep(1000L);
    assertFalse(checks.hasAccess(plugin, username, new NamespaceKey(Lists.newArrayList("what")), null, policy, new MetadataStatsCollector()));
  }

  @Test
  public void throwsProperly() throws Exception {
    final String username = "throwsProperly";
    final StoragePlugin2 plugin = mock(StoragePlugin2.class);
    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any(DatasetConfig.class)))
        .thenThrow(new RuntimeException("you shall not pass"));
    final MetadataPolicy policy = new MetadataPolicy().setAuthTtlMs(1000L);
    try {
      checks.hasAccess(plugin, username, new NamespaceKey(Lists.newArrayList("what")), null, policy, new MetadataStatsCollector());
      fail();
    } catch (UserException e) {
      assertEquals(UserBitShared.DremioPBError.ErrorType.PERMISSION, e.getErrorType());
      assertEquals("Access denied reading dataset what. you shall not pass", e.getMessage());
    }
  }
}
