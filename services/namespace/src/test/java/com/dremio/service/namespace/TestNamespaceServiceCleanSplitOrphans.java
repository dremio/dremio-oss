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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.PartitionChunkId.SplitOrphansRetentionPolicy.KEEP_CURRENT_VERSION_ONLY;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.namespace.PartitionChunkId.SplitOrphansRetentionPolicy;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.Affinity;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionValue;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionValueType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.test.DremioTest;
import com.google.protobuf.ByteString;

/**
 * Test for {@code NamespaceServiceImpl#deleteSplitOrphans(com.dremio.service.namespace.PartitionChunkId.SplitOrphansRetentionPolicy)}
 */
public class TestNamespaceServiceCleanSplitOrphans {
  private static final long REFRESH_PERIOD_MS = TimeUnit.HOURS.toMillis(24);
  private static final long GRACE_PERIOD_MS = TimeUnit.HOURS.toMillis(48);

  private LegacyKVStoreProvider kvstore;
  private NamespaceService namespaceService;
  private LegacyIndexedStore<String, NameSpaceContainer> namespaceStore;
  private LegacyIndexedStore<PartitionChunkId, PartitionChunk> partitionChunksStore;
  private LegacyKVStore<PartitionChunkId, PartitionProtobuf.MultiSplit> multiSplitStore;

  private long now;

  @Before
  public void populate() throws Exception {
    kvstore =
        LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    kvstore.start();

    namespaceService = new NamespaceServiceImpl(kvstore, new CatalogStatusEventsImpl());
    namespaceStore = kvstore.getStore(NamespaceServiceImpl.NamespaceStoreCreator.class);
    partitionChunksStore = kvstore.getStore(NamespaceServiceImpl.PartitionChunkCreator.class);
    multiSplitStore = kvstore.getStore(NamespaceServiceImpl.MultiSplitStoreCreator.class);

    now = System.currentTimeMillis();

    addHome("foo");
    savePhysicalDataset(Arrays.asList("@foo", "uploadfile"), DatasetType.PHYSICAL_DATASET_HOME_FILE, now - TimeUnit.HOURS.toMillis(48), 10);
    savePhysicalDataset(Arrays.asList("@foo", "uploadfolder"), DatasetType.PHYSICAL_DATASET_HOME_FOLDER, now - TimeUnit.DAYS.toMillis(365), 20);
    saveVirtualDataset(Arrays.asList("@foo", "bar"));

    addSource("test");
    // Add a dataset which refresh every hour...
    for(int i = 28; i >= 0; i--) {
      savePhysicalDataset(Arrays.asList("test", "dataset1"), DatasetType.PHYSICAL_DATASET, now - TimeUnit.HOURS.toMillis(i), 100);
    }
    // Add a second dataset, refreshing once per day
    savePhysicalDataset(Arrays.asList("test", "dataset2"), DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER, now, 1000);
    // Add a dataset which had splits but has been removed since
    int j = 0;
    for(int i = 16; i >= 5; i--) {
      j++;
      savePhysicalDataset(Arrays.asList("test", "dataset3"), DatasetType.PHYSICAL_DATASET_SOURCE_FILE, now - TimeUnit.HOURS.toMillis(i), 25);
    }
    assertThat(j).isEqualTo(12);
    deleteDataset(Arrays.asList("test", "dataset3"));

    addSource("OTHER");
    // Add a dataset which refresh every hour but source capitalization do not match
    for(int i = 28; i >= 0; i--) {
      savePhysicalDataset(Arrays.asList("other", "dataset4"), DatasetType.PHYSICAL_DATASET, now - TimeUnit.HOURS.toMillis(i), 100);
    }

    // Assert the total number of splits
    assertThat(
        StreamSupport.stream(partitionChunksStore.find().spliterator(), false).count())
      .isEqualTo((long) 10 + 20 + 29 * 100 + 1000 + 12 * 25 + 29 * 100);
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(kvstore);
  }

  @Test
  public void testKeepCurrentVersion() throws Exception {
    namespaceService.deleteSplitOrphans(KEEP_CURRENT_VERSION_ONLY, true);
    assertThat(
        namespaceService.getPartitionChunkCount(new LegacyFindByCondition().setCondition(SearchQueryUtils.newMatchAllQuery())))
      .isEqualTo(10 + 20 + 100 + 1000 + 100);


    DatasetConfig dataset1 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("test", "dataset1")));
    generatePartitionChunks(now, 100).forEach(split -> {
      PartitionChunkId id = PartitionChunkId.of(dataset1, split, now);
      assertThat(partitionChunksStore.get(id)).isEqualTo(split);
    });

    DatasetConfig dataset2 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("test", "dataset2")));
    generatePartitionChunks(now, 1000).forEach(split -> {
      PartitionChunkId id = PartitionChunkId.of(dataset2, split, now);
      assertThat(partitionChunksStore.get(id)).isEqualTo(split);
    });

    DatasetConfig dataset4 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("other", "dataset4")));
    generatePartitionChunks(now, 100).forEach(split -> {
      PartitionChunkId id = PartitionChunkId.of(dataset4, split, now);
      assertThat(partitionChunksStore.get(id)).isEqualTo(split);
    });
  }

  @Test
  public void testKeepValidSplits() throws Exception {
    namespaceService.deleteSplitOrphans(SplitOrphansRetentionPolicy.KEEP_VALID_SPLITS, true);
    assertThat(
        namespaceService.getPartitionChunkCount(new LegacyFindByCondition().setCondition(SearchQueryUtils.newMatchAllQuery())))
      .isEqualTo(10 + 20 + 24 * 100 + 1000 + 24 * 100);
    DatasetConfig dataset1 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("test", "dataset1")));
    for(int i = 23; i >= 0; i--) {
      final long splitVersion = now - TimeUnit.HOURS.toMillis(i);
      generatePartitionChunks(splitVersion, 100).forEach(split -> {
        PartitionChunkId id = PartitionChunkId.of(dataset1, split, splitVersion);
        assertThat(partitionChunksStore.get(id)).isEqualTo(split);
      });
    }

    DatasetConfig dataset2 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("test", "dataset2")));
    generatePartitionChunks(now, 1000).forEach(split -> {
      PartitionChunkId id = PartitionChunkId.of(dataset2, split, now);
      assertThat(partitionChunksStore.get(id)).isEqualTo(split);
    });

    DatasetConfig dataset4 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("other", "dataset4")));
    for(int i = 23; i >= 0; i--) {
      final long splitVersion = now - TimeUnit.HOURS.toMillis(i);
      generatePartitionChunks(splitVersion, 100).forEach(split -> {
        PartitionChunkId id = PartitionChunkId.of(dataset4, split, splitVersion);
        assertThat(partitionChunksStore.get(id)).isEqualTo(split);
      });
    }
  }

  private void addHome(String name) throws Exception {
    final HomeConfig home = new HomeConfig()
      .setId(new EntityId().setId(UUID.randomUUID().toString()))
      .setOwner(name);

    final NameSpaceContainer container = new NameSpaceContainer()
        .setType(NameSpaceContainer.Type.HOME)
        .setFullPathList(Arrays.asList(name))
        .setHome(home);

    namespaceStore.put(NamespaceServiceImpl.getKey(new NamespaceKey("@" + name)), container);
  }

  private void addSource(String name) throws Exception {
    final SourceConfig source = new SourceConfig()
      .setId(new EntityId().setId(UUID.randomUUID().toString()))
      .setName(name)
      .setType("test")
      .setCtime(100L)
      .setAccelerationRefreshPeriod(REFRESH_PERIOD_MS)
      .setAccelerationGracePeriod(GRACE_PERIOD_MS)
      .setMetadataPolicy(new MetadataPolicy().setDatasetDefinitionExpireAfterMs(TimeUnit.DAYS.toMillis(1)));

    final NameSpaceContainer container = new NameSpaceContainer()
        .setType(NameSpaceContainer.Type.SOURCE)
        .setFullPathList(Arrays.asList(name))
        .setSource(source);
    namespaceStore.put(NamespaceServiceImpl.getKey(new NamespaceKey(name)), container);
  }

  public void savePhysicalDataset(List<String> path, DatasetType type, long splitVersion, int count) throws NamespaceException {
    final ReadDefinition readDefinition = new ReadDefinition()
        .setSplitVersion(splitVersion);

    final DatasetConfig datasetConfig = saveDataset(path, type, config -> config.setReadDefinition(readDefinition));
    generatePartitionChunks(splitVersion, count)
        .forEach(split -> { partitionChunksStore.put(PartitionChunkId.of(datasetConfig, split, splitVersion), split);
          multiSplitStore.put(PartitionChunkId.of(datasetConfig, split, splitVersion), createMultiSplit(split.getSplitKey())); });
  }

  private PartitionProtobuf.MultiSplit createMultiSplit(String splitKey) {
    return PartitionProtobuf.MultiSplit.newBuilder()
      .setMultiSplitKey(splitKey)
      .build();
  }

  public void saveVirtualDataset(List<String> path) throws NamespaceException {
    final VirtualDataset virtualDataset = new VirtualDataset().setSql("SELECT 1");
    saveDataset(path, DatasetType.VIRTUAL_DATASET, config -> config.setVirtualDataset(virtualDataset));
  }

  private DatasetConfig saveDataset(List<String> path, DatasetType type, Function<DatasetConfig, DatasetConfig> transformer) throws NamespaceException {
    final NamespaceKey key = new NamespaceKey(path);
    final String stringKey = NamespaceServiceImpl.getKey(key);

    final Optional<DatasetConfig> oldDataset = Optional.ofNullable(namespaceStore.get(stringKey)).map(NameSpaceContainer::getDataset);

    final DatasetConfig datasetConfig = transformer.apply(new DatasetConfig()
        .setId(oldDataset.map(DatasetConfig::getId).orElse(new EntityId().setId(UUID.randomUUID().toString())))
        .setName(path.get(path.size() - 1))
        .setFullPathList(path)
        .setType(type)
        .setTag(oldDataset.map(DatasetConfig::getTag).orElse(null))
        .setOwner("dremio"));

    final NameSpaceContainer container = new NameSpaceContainer()
        .setType(NameSpaceContainer.Type.DATASET)
        .setFullPathList(path)
        .setDataset(datasetConfig);
    namespaceStore.put(stringKey, container);

    return datasetConfig;
  }

  private Stream<PartitionChunk> generatePartitionChunks(long splitVersion, int count) {
    return IntStream.range(0, count)
        .mapToObj(i ->
          PartitionChunk.newBuilder()
          .setRowCount(i)
          .setSize(i)
          .addAffinities(Affinity.newBuilder().setHost("node" + i))
          .addPartitionValues(PartitionValue.newBuilder().setColumn("column" + i).setIntValue(i).setType(PartitionValueType.IMPLICIT))
          .setPartitionExtendedProperty(ByteString.copyFromUtf8(String.valueOf(i)))
          .setSplitKey(String.valueOf(i))
          .build()
        );
  }

  private void deleteDataset(List<String> path) throws NamespaceException {
    final NamespaceKey key = new NamespaceKey(path);
    try {
      DatasetConfig oldDataset = namespaceService.getDataset(key);
      namespaceService.deleteDataset(key, oldDataset.getTag());
    } catch (NamespaceNotFoundException e) {
      // Ignore
    }
  }
}
