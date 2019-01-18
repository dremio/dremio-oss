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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.DatasetSplitId.SplitOrphansRetentionPolicy.KEEP_CURRENT_VERSION_ONLY;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.AutoCloseables;
import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.service.namespace.DatasetSplitId.SplitOrphansRetentionPolicy;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionValue;
import com.dremio.service.namespace.dataset.proto.PartitionValueType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.test.DremioTest;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

/**
 * Test for {@code NamespaceServiceImpl#deleteSplitOrphans(com.dremio.service.namespace.DatasetSplitId.SplitOrphansRetentionPolicy)}
 */
public class TestNamespaceServiceCleanSplitOrphans {
  private static final long REFRESH_PERIOD_MS = TimeUnit.HOURS.toMillis(24);
  private static final long GRACE_PERIOD_MS = TimeUnit.HOURS.toMillis(48);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private LocalKVStoreProvider kvstore;
  private NamespaceService namespaceService;
  private IndexedStore<byte[], NameSpaceContainer> namespaceStore;
  private IndexedStore<DatasetSplitId, DatasetSplit> splitsStore;

  private long now;

  @Before
  public void populate() throws Exception {
    kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvstore.start();

    namespaceService = new NamespaceServiceImpl(kvstore);
    namespaceStore = kvstore.getStore(NamespaceServiceImpl.NamespaceStoreCreator.class);
    splitsStore = kvstore.getStore(NamespaceServiceImpl.DatasetSplitCreator.class);

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
    assertThat(j, is(12));
    deleteDataset(Arrays.asList("test", "dataset3"));

    addSource("OTHER");
    // Add a dataset which refresh every hour but source capitalization do not match
    for(int i = 28; i >= 0; i--) {
      savePhysicalDataset(Arrays.asList("other", "dataset4"), DatasetType.PHYSICAL_DATASET, now - TimeUnit.HOURS.toMillis(i), 100);
    }

    // Assert the total number of splits
    assertThat(
        StreamSupport.stream(splitsStore.find().spliterator(), false).count(),
        is((long) 10 + 20 + 29 * 100 + 1000 + 12 * 25 + 29 * 100));
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(kvstore);
  }

  @Test
  public void testKeepCurrentVersion() throws Exception {
    namespaceService.deleteSplitOrphans(KEEP_CURRENT_VERSION_ONLY);
    assertThat(
        namespaceService.getSplitCount(new FindByCondition().setCondition(SearchQueryUtils.newMatchAllQuery())),
        is(10 + 20 + 100 + 1000 + 100));


    DatasetConfig dataset1 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("test", "dataset1")));
    generateSplits(now, 100).forEach(split -> {
      DatasetSplitId id = DatasetSplitId.of(dataset1, split, now);
      assertThat(splitsStore.get(id), is(split));
    });

    DatasetConfig dataset2 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("test", "dataset2")));
    generateSplits(now, 1000).forEach(split -> {
      DatasetSplitId id = DatasetSplitId.of(dataset2, split, now);
      assertThat(splitsStore.get(id), is(split));
    });

    DatasetConfig dataset4 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("other", "dataset4")));
    generateSplits(now, 100).forEach(split -> {
      DatasetSplitId id = DatasetSplitId.of(dataset4, split, now);
      assertThat(splitsStore.get(id), is(split));
    });
  }

  @Test
  public void testKeepValidSplits() throws Exception {
    namespaceService.deleteSplitOrphans(SplitOrphansRetentionPolicy.KEEP_VALID_SPLITS);
    assertThat(
        namespaceService.getSplitCount(new FindByCondition().setCondition(SearchQueryUtils.newMatchAllQuery())),
        is(10 + 20 + 24 * 100 + 1000 + 24 * 100));
    DatasetConfig dataset1 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("test", "dataset1")));
    for(int i = 23; i >= 0; i--) {
      final long splitVersion = now - TimeUnit.HOURS.toMillis(i);
      generateSplits(splitVersion, 100).forEach(split -> {
        DatasetSplitId id = DatasetSplitId.of(dataset1, split, splitVersion);
        assertThat(splitsStore.get(id), is(split));
      });
    }

    DatasetConfig dataset2 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("test", "dataset2")));
    generateSplits(now, 1000).forEach(split -> {
      DatasetSplitId id = DatasetSplitId.of(dataset2, split, now);
      assertThat(splitsStore.get(id), is(split));
    });

    DatasetConfig dataset4 = namespaceService.getDataset(new NamespaceKey(Arrays.asList("other", "dataset4")));
    for(int i = 23; i >= 0; i--) {
      final long splitVersion = now - TimeUnit.HOURS.toMillis(i);
      generateSplits(splitVersion, 100).forEach(split -> {
        DatasetSplitId id = DatasetSplitId.of(dataset4, split, splitVersion);
        assertThat(splitsStore.get(id), is(split));
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
    generateSplits(splitVersion, count)
        .forEach(split -> splitsStore.put(DatasetSplitId.of(datasetConfig, split, splitVersion), split));
  }

  public void saveVirtualDataset(List<String> path) throws NamespaceException {
    final VirtualDataset virtualDataset = new VirtualDataset().setSql("SELECT 1");
    saveDataset(path, DatasetType.VIRTUAL_DATASET, config -> config.setVirtualDataset(virtualDataset));
  }

  private DatasetConfig saveDataset(List<String> path, DatasetType type, Function<DatasetConfig, DatasetConfig> transformer) throws NamespaceException {
    final NamespaceKey key = new NamespaceKey(path);
    final byte[] binaryKey = NamespaceServiceImpl.getKey(key);

    final Optional<DatasetConfig> oldDataset = Optional.ofNullable(namespaceStore.get(binaryKey)).map(NameSpaceContainer::getDataset);

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
    namespaceStore.put(binaryKey, container);

    return datasetConfig;
  }

  private Stream<DatasetSplit> generateSplits(long splitVersion, int count) {
    return IntStream.range(0, count)
        .mapToObj(i ->
          new DatasetSplit()
          .setRowCount((long) i)
          .setTag("0")
          .setSize((long) i)
          .setAffinitiesList(Arrays.asList(new Affinity().setHost("node" + i)))
          .setPartitionValuesList(Lists.newArrayList(new PartitionValue().setColumn("column" + i).setIntValue(i).setType(PartitionValueType.IMPLICIT)))
          .setExtendedProperty(ByteString.copyFrom(String.valueOf(i).getBytes()))
          .setSplitKey(String.valueOf(i))
          .setSplitVersion(splitVersion)
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
