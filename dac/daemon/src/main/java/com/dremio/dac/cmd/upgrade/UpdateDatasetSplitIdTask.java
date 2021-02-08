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
package com.dremio.dac.cmd.upgrade;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;

import com.dremio.common.Version;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.server.DACConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStore.LegacyFindByRange;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.collect.ImmutableList;

/**
 * UpdateDatasetSplitIdTask implements a Scan for datasets whose ID may cause unsafe dataset split IDs
 * as they are using reserved characters
 */
public class UpdateDatasetSplitIdTask extends UpgradeTask implements LegacyUpgradeTask {


  //DO NOT MODIFY
  static final String taskUUID = "d7cb2438-bc97-4c76-8a7a-ff5493e48e5e";

  public UpdateDatasetSplitIdTask() {
    super("Fix dataset split ids with invalid id",
      ImmutableList.of(ReIndexAllStores.taskUUID, "ff9f6514-d7e6-44c7-b628-865cd3ce7368"));
  }

  /**
   * Gets the upgrade task UUID.
   *
   * @return the UUID from the current task
   */
  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_300;
  }

  /**
   * Executes a verification in store providers and update the store partitions.
   *
   * @param context an instance that contains the stores required by the current upgrade
   * @throws Exception If any exception or errors occurs
   */
  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final LegacyKVStoreProvider storeProvider = context.getKVStoreProvider();
    final LegacyKVStore<String, NameSpaceContainer> namespace = storeProvider.getStore(NamespaceServiceImpl.NamespaceStoreCreator.class);
    final LegacyKVStore<PartitionChunkId, PartitionChunk> partitionChunksStore = storeProvider.getStore(NamespaceServiceImpl.PartitionChunkCreator.class);

    int fixedSplitIds = 0;
    // namespace#find() returns entries ordered by depth, so sources will
    // be processed before folders, which will be processed before datasets
    for(Map.Entry<String, NameSpaceContainer> entry: namespace.find()) {
      final NameSpaceContainer container = entry.getValue();

      if (container.getType() != NameSpaceContainer.Type.DATASET) {
        continue;
      }

      DatasetConfig config = entry.getValue().getDataset();
      if (config.getType() == DatasetType.VIRTUAL_DATASET) {
        continue;
      }

      if (config.getReadDefinition() == null || config.getReadDefinition().getSplitVersion() == null) {
        continue;
      }

      if (!PartitionChunkId.mayRequireNewDatasetId(config)) {
        // Datasets which do not contain reserved characters are fine
        continue;
      }

      fixSplits(partitionChunksStore, config);
    }

    AdminLogger.log("  Updated {} dataset splits with new ids.", fixedSplitIds);
  }

  /**
   * Generates new ID, add a new partition from a given store partition and deletes the partition with the old ID.
   *
   * @param partitionChunksStore a piece of a partition of a dataset
   * @param config               a DatasetConfig instance
   */
  private void fixSplits(final LegacyKVStore<PartitionChunkId, PartitionChunk> partitionChunksStore,
      DatasetConfig config) {
    final long version = config.getReadDefinition().getSplitVersion();

    // Get old splits
    final LegacyFindByRange<PartitionChunkId> query = PartitionChunkId.unsafeGetSplitsRange(config);
    for (Entry<PartitionChunkId, PartitionChunk> entry : partitionChunksStore.find(query)) {
      final PartitionChunkId oldId = entry.getKey();
      final PartitionChunk split = entry.getValue();

      // Generate new Id and compare with old id
      final PartitionChunkId newId = PartitionChunkId.of(config, split, version);
      if (oldId.equals(newId)) {
        continue;
      }

      // Delete the previous entry and add a new one
      partitionChunksStore.delete(oldId);
      partitionChunksStore.put(newId, split);
    }
  }


  /**
   * Run the task against a directory.
   *
   * @param args one single argument, the path to the database
   * @throws Exception If any exception or errors occurs
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      AdminLogger.log("Require one argument: path to the database");
    }

    final String dbPath = args[0];

    if (!Files.isDirectory(Paths.get(dbPath))) {
      AdminLogger.log("No database found. Skipping splits check");
      return;
    }

    final SabotConfig sabotConfig = DACConfig.newConfig().getConfig().getSabotConfig();
    final ScanResult classpathScan = ClassPathScanner.fromPrescan(sabotConfig);
    try (final LocalKVStoreProvider storeProvider =
      new LocalKVStoreProvider(classpathScan, args[0], false, true)) {
      storeProvider.start();

      final UpgradeContext context = new UpgradeContext(storeProvider.asLegacy(), null, null, null);
      final UpdateDatasetSplitIdTask task = new UpdateDatasetSplitIdTask();
      task.upgrade(context);
    }
  }

  /**
   * Gets a string representation of the current task description.
   *
   * @return the task description represented by a string format
   */
  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
