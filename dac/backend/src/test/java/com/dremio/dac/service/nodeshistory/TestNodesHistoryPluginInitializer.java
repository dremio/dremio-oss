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
package com.dremio.dac.service.nodeshistory;

import com.dremio.BaseTestQuery;
import com.dremio.BaseTestQueryJunit5;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.plugins.nodeshistory.NodesHistoryStoreConfig;
import com.dremio.plugins.nodeshistory.NodesHistoryTable;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.services.nodemetrics.NodeMetrics;
import com.dremio.services.nodemetrics.persistence.NodeMetricsTestUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestNodesHistoryPluginInitializer extends BaseTestQueryJunit5 {
  private static NodesHistoryPluginInitializer nodesHistoryPluginInitializer;
  private static NodesHistoryStoreConfig nodesHistoryStoreConfig;

  @BeforeAll
  public static void setup() throws Exception {
    BaseTestQuery.setupDefaultTestCluster();
    nodesHistoryPluginInitializer =
        new NodesHistoryPluginInitializer(
            BaseTestQueryJunit5::getCatalogService, RunSynchronouslySchedulerService::new);
    nodesHistoryStoreConfig = getNodesHistoryStoreConfig();
  }

  private static NodesHistoryStoreConfig getNodesHistoryStoreConfig() {
    final FileSystemPlugin<?> plugin =
        BaseTestQueryJunit5.getCatalogService()
            .getSource(NodesHistoryStoreConfig.STORAGE_PLUGIN_NAME);
    return new NodesHistoryStoreConfig(plugin.getConfig().getPath(), plugin.getSystemUserFS());
  }

  @Test
  public void testInitialize() throws IOException {
    // Validate precondition that table doesn't already exist before the test
    NamespaceKey key = new NamespaceKey(NodesHistoryTable.METRICS.getPath());
    Catalog catalog = getCatalogService().getSystemUserCatalog();
    Preconditions.checkState(catalog.getTable(key) == null);
    catalog.clearDatasetCache(key, null);

    // Run the plugin initializer to create the metrics table
    nodesHistoryPluginInitializer.initialize();

    // The plugin initializer will have created the table as shallow, so for getTable to return it
    // we need to ensure the backing folder exists so a dataset handle can be retrieved for it to
    // be promoted on query
    NodeMetrics testNodeMetrics =
        NodeMetricsTestUtils.generateRandomNodeMetrics(
            NodeMetricsTestUtils.NodeType.MASTER_COORDINATOR);
    NodeMetricsTestUtils.writeNodeMetrics(List.of(testNodeMetrics), () -> nodesHistoryStoreConfig);

    // Validate the metrics table was created successfully and the dataset is complete and valid
    DremioTable metricsTable = catalog.getTable(key);
    Assertions.assertNotNull(metricsTable);
    Assertions.assertTrue(metricsTable.getDatasetMetadataState().isComplete());
    Assertions.assertFalse(metricsTable.getDatasetMetadataState().isExpired());
    Assertions.assertEquals(
        NodesHistoryTable.METRICS.getName(), metricsTable.getDatasetConfig().getName());
    Assertions.assertEquals(
        DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER, metricsTable.getDatasetConfig().getType());
    Assertions.assertTrue(metricsTable.getDatasetConfig().getCreatedAt() > 0);
    Assertions.assertTrue(metricsTable.getDatasetConfig().getLastModified() > 0);
    Assertions.assertNotNull(metricsTable.getDatasetConfig().getRecordSchema());
    Assertions.assertNotNull(metricsTable.getDatasetConfig().getReadDefinition());
    Assertions.assertEquals(
        FileType.TEXT,
        metricsTable.getDatasetConfig().getPhysicalDataset().getFormatSettings().getType());
    Assertions.assertEquals(
        nodesHistoryStoreConfig
            .getStoragePath()
            .resolve(NodesHistoryTable.METRICS.getName())
            .toString(),
        metricsTable.getDatasetConfig().getPhysicalDataset().getFormatSettings().getLocation());
  }

  private static final class RunSynchronouslySchedulerService implements SchedulerService {
    @Override
    public void start() {}

    @Override
    public Cancellable schedule(Schedule schedule, Runnable task) {
      task.run();
      return new Cancellable() {
        @Override
        public void cancel(boolean mayInterruptIfRunning) {
          throw new UnsupportedOperationException("Cancellation not supported");
        }

        @Override
        public boolean isCancelled() {
          return false;
        }

        @Override
        public boolean isDone() {
          return true;
        }
      };
    }

    @Override
    public void close() {}
  }
}
