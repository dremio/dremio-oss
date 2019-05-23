/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import javax.inject.Provider;

import org.junit.Test;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.ImpersonationConf;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.proto.EntityId;

/**
 * Tests for DatasetManager
 */
public class TestDatasetManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDatasetManager.class);

  @Test
  public void testAccessUsernameOverride() throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey("test");

    final ViewExpansionContext viewExpansionContext = mock(ViewExpansionContext.class);
    when(viewExpansionContext.getQueryUser()).thenReturn("newaccessuser");

    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    when(schemaConfig.getViewExpansionContext()).thenReturn(viewExpansionContext);

    final MetadataStatsCollector statsCollector = mock(MetadataStatsCollector.class);

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);
    when(metadataRequestOptions.getStatsCollector()).thenReturn(statsCollector);

    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(0L);

    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    datasetConfig.setId(new EntityId("test"));
    datasetConfig.setFullPathList(Collections.singletonList("test"));
    datasetConfig.setReadDefinition(readDefinition);
    datasetConfig.setTotalNumSplits(0);

    class FakeSource extends ConnectionConf<FakeSource, StoragePlugin> implements ImpersonationConf {
      @Override
      public StoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
        return null;
      }

      @Override
      public String getAccessUserName(String delegatedUser, String queryUserName) {
        return queryUserName;
      }
    }

    final FakeSource fakeSource = new FakeSource();

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    doReturn(fakeSource).when(managedStoragePlugin).getConnectionConf();
    when(managedStoragePlugin.isValid(any(), any())).thenReturn(true);
    // newaccessuser should be used and not username
    doThrow(new RuntimeException("Wrong username"))
      .when(managedStoragePlugin).checkAccess(namespaceKey, datasetConfig, "username", metadataRequestOptions);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager = new DatasetManager(pluginRetriever, namespaceService, optionManager);
    datasetManager.getTable(namespaceKey, metadataRequestOptions);
  }
}
