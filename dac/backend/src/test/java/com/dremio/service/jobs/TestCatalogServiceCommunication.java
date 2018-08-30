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
package com.dremio.service.jobs;

import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.inject.Provider;

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TestCatalogServiceImpl;
import com.dremio.exec.catalog.TestCatalogServiceImpl.MockUpPlugin;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.collect.ImmutableList;

/**
 * Tests for catalog service communication.
 */
public class TestCatalogServiceCommunication extends BaseTestServer {

  private static MockUpPlugin mockUpPlugin;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();

    mockUpPlugin = new MockUpPlugin();
  }

  @Test
  public void changeConfigAndExpectMessage() throws Exception {
    if (!isMultinode()) {
      return;
    }

    CatalogServiceImpl catalogServiceImpl = (CatalogServiceImpl) l(CatalogService.class);

    {
      final SourceConfig mockUpConfig = new SourceConfig()
          .setName(MOCK_UP)
          .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY)
          .setId(new EntityId("source-id"))
          .setCtime(100L)
          .setConnectionConf(new MockUpConfig());

      doMockDatasets(mockUpPlugin, ImmutableList.<SourceTableDefinition>of());

      catalogServiceImpl.getSystemUserCatalog().createSource(mockUpConfig);

      assertTrue((
          (CatalogServiceImpl) getExecutorDaemon()
              .getBindingProvider()
              .lookup(CatalogService.class))
          .getManagedSource(MOCK_UP)
          .matches(mockUpConfig));
    }

    {
      final SourceConfig mockUpConfig = new SourceConfig()
          .setName(MOCK_UP)
          .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
          .setCtime(100L)
          .setId(new EntityId("source-id"))
          .setVersion(l(NamespaceService.class)
              .getSource(new NamespaceKey(MOCK_UP))
              .getVersion())
          .setConnectionConf(new MockUpConfig());

      catalogServiceImpl.getSystemUserCatalog().updateSource(mockUpConfig);

      assertTrue((
          (CatalogServiceImpl) getExecutorDaemon()
              .getBindingProvider()
              .lookup(CatalogService.class))
          .getManagedSource(MOCK_UP)
          .matches(mockUpConfig));

      catalogServiceImpl.getSystemUserCatalog().deleteSource(mockUpConfig);
    }
  }

  public static final String MOCK_UP = "mockup2";

  /**
   * Mock source config.
   */
  @SourceType(value = MOCK_UP, configurable = false)
  public static class MockUpConfig extends ConnectionConf<TestCatalogServiceImpl.MockUpConfig, MockUpPlugin> {

    @Override
    public MockUpPlugin newPlugin(SabotContext context, String name,
                                  Provider<StoragePluginId> pluginIdProvider) {
      return mockUpPlugin;
    }
  }

  private void doMockDatasets(StoragePlugin plugin, final List<SourceTableDefinition> datasets) throws Exception {
    ((MockUpPlugin) plugin).setDatasets(datasets);
  }
}
