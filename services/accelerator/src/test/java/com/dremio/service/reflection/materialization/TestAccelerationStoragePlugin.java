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
package com.dremio.service.reflection.materialization;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationPlanId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshId;
import com.dremio.service.reflection.store.MaterializationPlanStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import javax.inject.Provider;
import org.junit.Assert;
import org.junit.Test;

/** tests for {@link AccelerationStoragePlugin} */
public class TestAccelerationStoragePlugin {

  @Test
  public void testIncrementalMaterializationDoesNotDuplicateDeletes() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");

    Materialization materialization =
        new Materialization()
            .setId(materializationId)
            .setReflectionId(reflectionId)
            .setArrowCachingEnabled(false)
            .setReflectionGoalVersion("test")
            .setState(MaterializationState.DONE);

    Refresh firstRefresh = new Refresh().setIsIcebergRefresh(false).setPath("refresh/test/path");
    Refresh secondRefresh = new Refresh().setIsIcebergRefresh(false).setPath("refresh/test/path");
    RefreshId firstRefreshId = new RefreshId("firstRefresh");
    RefreshId secondRefreshId = new RefreshId("secondRefresh");
    firstRefresh.setId(firstRefreshId);
    secondRefresh.setId(secondRefreshId);

    List<Refresh> refreshList = ImmutableList.of(firstRefresh, secondRefresh);

    SchemaConfig schemaConfig = mock(SchemaConfig.class);
    AccelerationStoragePluginConfig accelerationStoragePluginConfig =
        mock(AccelerationStoragePluginConfig.class);
    SabotContext sabotContext = mock(SabotContext.class);
    Provider<StoragePluginId> storagePluginIdProvider = mock(Provider.class);
    MaterializationStore materializationStore = mock(MaterializationStore.class);
    MaterializationPlanStore materializationPlanStore = mock(MaterializationPlanStore.class);
    NamespaceKey tableSchemaPath = mock(NamespaceKey.class);
    TableMutationOptions tableMutationOptions = mock(TableMutationOptions.class);
    AccelerationStoragePlugin accelerationStoragePlugin =
        spy(
            new AccelerationStoragePlugin(
                accelerationStoragePluginConfig,
                sabotContext,
                "testAccelerationStoragePlugin",
                storagePluginIdProvider,
                materializationStore,
                materializationPlanStore));

    when(tableSchemaPath.getPathComponents())
        .thenReturn(
            ImmutableList.of(
                ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME,
                reflectionId.getId(),
                materializationId.getId()));
    when(materializationStore.get(materializationId)).thenReturn(materialization);
    when(materializationStore.getRefreshesExclusivelyOwnedBy(materialization))
        .thenReturn(refreshList);
    doNothing()
        .when(accelerationStoragePlugin)
        .fileSystemPluginDropTable(
            any(NamespaceKey.class), any(SchemaConfig.class), any(TableMutationOptions.class));

    // TEST
    // Verify we only dropped the filesystem path once, but deleted both refreshes from the
    // materialization store.
    accelerationStoragePlugin.dropTable(tableSchemaPath, schemaConfig, tableMutationOptions);
    verify(accelerationStoragePlugin, times(1))
        .fileSystemPluginDropTable(
            any(NamespaceKey.class), any(SchemaConfig.class), any(TableMutationOptions.class));
    verify(materializationStore, times(2)).delete(any(RefreshId.class));
    verify(materializationPlanStore, times(1)).delete(any(MaterializationPlanId.class));
  }

  @Test
  public void testGetPath() {
    ReflectionId reflectionId = new ReflectionId("r_id");
    MaterializationId materializationId = new MaterializationId("m_id");

    Materialization materialization =
        new Materialization()
            .setId(materializationId)
            .setSeriesId(5L)
            .setReflectionId(reflectionId);

    Refresh refresh =
        new Refresh().setIsIcebergRefresh(true).setPath("r_id/m_id_0").setBasePath("m_id_0");

    AccelerationStoragePluginConfig accelerationStoragePluginConfig =
        mock(AccelerationStoragePluginConfig.class);
    when(accelerationStoragePluginConfig.getPath())
        .thenReturn(Path.of(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME));
    SabotContext sabotContext = mock(SabotContext.class);
    Provider<StoragePluginId> storagePluginIdProvider = mock(Provider.class);
    MaterializationStore materializationStore = mock(MaterializationStore.class);
    MaterializationPlanStore materializationPlanStore = mock(MaterializationPlanStore.class);
    AccelerationStoragePlugin accelerationStoragePlugin =
        spy(
            new AccelerationStoragePlugin(
                accelerationStoragePluginConfig,
                sabotContext,
                "testAccelerationStoragePlugin",
                storagePluginIdProvider,
                materializationStore,
                materializationPlanStore));

    when(materializationStore.get(materializationId)).thenReturn(materialization);
    when(materializationStore.getMostRecentRefresh(reflectionId, 5L)).thenReturn(refresh);

    // TEST
    NamespaceKey namespaceKey =
        new NamespaceKey(
            Lists.newArrayList(
                ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME,
                reflectionId.getId(),
                materializationId.getId()));
    Path path = accelerationStoragePlugin.getPath(namespaceKey, "dremio");
    Assert.assertEquals(
        ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME + "/r_id/m_id_0", path.toString());
  }
}
