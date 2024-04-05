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
package com.dremio.exec.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.collect.Lists;
import org.junit.Test;

public class TestPermissionCheckCache {

  @Test
  public void ensureNotCached() throws Exception {
    final String username = "ensureNotCached";
    final StoragePlugin plugin = mock(StoragePlugin.class);
    final SourceConfig sourceConfig = new SourceConfig();
    final PermissionCheckCache checks =
        new PermissionCheckCache(DirectProvider.wrap(plugin), DirectProvider.wrap(0L), 1000);
    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any())).thenReturn(true);
    assertTrue(
        checks.hasAccess(
            username,
            new NamespaceKey(Lists.newArrayList("what")),
            null,
            new MetadataStatsCollector(),
            sourceConfig));
    assertNull(
        checks
            .getPermissionsCache()
            .getIfPresent(
                new PermissionCheckCache.Key(
                    username, new NamespaceKey(Lists.newArrayList("what")))));
  }

  @Test
  public void ensureCached() throws Exception {
    final String username = "ensureCached";
    final StoragePlugin plugin = mock(StoragePlugin.class);
    final SourceConfig sourceConfig = new SourceConfig();
    final PermissionCheckCache checks =
        new PermissionCheckCache(DirectProvider.wrap(plugin), DirectProvider.wrap(10_000L), 1000);
    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any()))
        .thenReturn(true, false);
    assertTrue(
        checks.hasAccess(
            username,
            new NamespaceKey(Lists.newArrayList("what")),
            null,
            new MetadataStatsCollector(),
            sourceConfig));
    assertNotNull(
        checks
            .getPermissionsCache()
            .getIfPresent(
                new PermissionCheckCache.Key(
                    username, new NamespaceKey(Lists.newArrayList("what")))));
    assertTrue(
        checks.hasAccess(
            username,
            new NamespaceKey(Lists.newArrayList("what")),
            null,
            new MetadataStatsCollector(),
            sourceConfig));
  }

  @Test
  public void ensureReloaded() throws Exception {
    final String username = "ensureReloaded";
    final StoragePlugin plugin = mock(StoragePlugin.class);
    final SourceConfig sourceConfig = new SourceConfig();
    final PermissionCheckCache checks =
        new PermissionCheckCache(DirectProvider.wrap(plugin), DirectProvider.wrap(500L), 1000);
    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any()))
        .thenReturn(true, false);
    assertTrue(
        checks.hasAccess(
            username,
            new NamespaceKey(Lists.newArrayList("what")),
            null,
            new MetadataStatsCollector(),
            sourceConfig));
    assertNotNull(
        checks
            .getPermissionsCache()
            .getIfPresent(
                new PermissionCheckCache.Key(
                    username, new NamespaceKey(Lists.newArrayList("what")))));
    Thread.sleep(1000L);
    assertFalse(
        checks.hasAccess(
            username,
            new NamespaceKey(Lists.newArrayList("what")),
            null,
            new MetadataStatsCollector(),
            sourceConfig));
  }

  @Test
  public void throwsProperly() throws Exception {
    final String username = "throwsProperly";
    final StoragePlugin plugin = mock(StoragePlugin.class);
    final SourceConfig sourceConfig = new SourceConfig();
    final PermissionCheckCache checks =
        new PermissionCheckCache(DirectProvider.wrap(plugin), DirectProvider.wrap(1000L), 1000);
    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any()))
        .thenThrow(new RuntimeException("you shall not pass"));
    try {
      checks.hasAccess(
          username,
          new NamespaceKey(Lists.newArrayList("what")),
          null,
          new MetadataStatsCollector(),
          sourceConfig);
      fail();
    } catch (UserException e) {
      assertEquals(UserBitShared.DremioPBError.ErrorType.PERMISSION, e.getErrorType());
      assertEquals("Access denied reading dataset what.", e.getMessage());
    }
  }

  @Test
  public void ensureNoPermissionIsNotCached() throws Exception {
    final String username = "ensureCached";
    final StoragePlugin plugin = mock(StoragePlugin.class);
    final SourceConfig sourceConfig = new SourceConfig();
    final PermissionCheckCache checks =
        new PermissionCheckCache(DirectProvider.wrap(plugin), DirectProvider.wrap(10_000L), 1000);
    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any()))
        .thenReturn(false, false);
    assertFalse(
        checks.hasAccess(
            username,
            new NamespaceKey(Lists.newArrayList("what")),
            null,
            new MetadataStatsCollector(),
            sourceConfig));

    assertEquals(0, checks.getPermissionsCache().size());

    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any()))
        .thenReturn(true, false);
    assertTrue(
        checks.hasAccess(
            username,
            new NamespaceKey(Lists.newArrayList("what")),
            null,
            new MetadataStatsCollector(),
            sourceConfig));

    assertEquals(1, checks.getPermissionsCache().size());
  }

  @Test
  public void testNeverCachePermissionsAnnotation() throws Exception {
    String username = "ensureNotCached";
    NamespaceKey key = new NamespaceKey(Lists.newArrayList("what"));

    // can't use mocks here since mockito will lose annotations
    StoragePlugin plugin = mock(StoragePlugin.class);
    when(plugin.hasAccessPermission(anyString(), any(NamespaceKey.class), any()))
        .thenReturn(true, false);

    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setType("ESYS");
    PermissionCheckCache checks = new PermissionCheckCache(() -> plugin, () -> 10_000L, 1000);
    assertTrue(checks.hasAccess(username, key, null, new MetadataStatsCollector(), sourceConfig));
    assertNull(
        checks.getPermissionsCache().getIfPresent(new PermissionCheckCache.Key(username, key)));
  }
}
