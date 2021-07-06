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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.sample.SampleSourceMetadata;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

/**
 * Unit test of the dataset metadata saver, and the subsequent fetching of splits
 */
public class TestDatasetMetadataSaver {
  private LegacyKVStoreProvider kvStoreProvider;
  private NamespaceService namespaceService;
  private NamespaceService.SplitCompression currentCompression;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    kvStoreProvider =
        LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    kvStoreProvider.start();
    namespaceService = new NamespaceServiceImpl(kvStoreProvider);
  }

  @After
  public void cleanup() throws Exception {
    kvStoreProvider.close();
  }

  // TODO: use MetadataObjectsUtils.newShallowConfig() once saving logic is moved to the catalog
  DatasetConfig convert(DatasetHandle handle, long splitVersion) {
    final DatasetConfig shallowConfig = new DatasetConfig();

    shallowConfig.setId(new EntityId()
      .setId(UUID.randomUUID().toString()));
    shallowConfig.setCreatedAt(System.currentTimeMillis());
    shallowConfig.setName(handle.getDatasetPath().getName());
    shallowConfig.setFullPathList(handle.getDatasetPath().getComponents());
    shallowConfig.setType(DatasetType.PHYSICAL_DATASET);

    // To make it work with save, we need a read definition that contains a split version
    ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(splitVersion);
    shallowConfig.setReadDefinition(readDefinition);
    return shallowConfig;
  }

  void checkSplits(DatasetConfig dataset, int numPartitionChunksPerDataset, int numSplitsPerPartitionChunk, boolean quitBeforeSaving, boolean legacySplits) throws NamespaceException, IOException {
    List<PartitionChunkMetadata> result =
      ImmutableList.copyOf(namespaceService.findSplits(new LegacyIndexedStore.LegacyFindByCondition().setCondition(PartitionChunkId.getSplitsQuery(dataset))));
    assertEquals(numPartitionChunksPerDataset, result.size());
    Set<String> expectedSplits = new HashSet<>();
    for (int p = 0; p < numPartitionChunksPerDataset; p++) {
      for (int s = 0; s < numSplitsPerPartitionChunk; s++) {
        expectedSplits.add(String.format("p%d_s%d", p, s));
      }
    }
    if (quitBeforeSaving || legacySplits) {
      assertNull(null, dataset.getTotalNumSplits());
    } else {
      assertEquals(numPartitionChunksPerDataset * numSplitsPerPartitionChunk, dataset.getTotalNumSplits().longValue()); // NB unbox to avoid ambiguous assertEquals
    }
    for (PartitionChunkMetadata pcm : result) {
      if (legacySplits) {
        assertEquals(1, pcm.getSplitCount());
      } else {
        assertEquals(numSplitsPerPartitionChunk, pcm.getSplitCount());
      }
      List<DatasetSplit> splits = ImmutableList.copyOf(pcm.getDatasetSplits());
      assertEquals(numSplitsPerPartitionChunk, splits.size());
      for (DatasetSplit split : splits) {
        String splitName = new String(split.getSplitExtendedProperty().toByteArray());
        assertTrue(expectedSplits.contains(splitName));
        expectedSplits.remove(splitName);
      }
    }
    assertTrue(expectedSplits.isEmpty());
  }

  private void saveDataset(SampleSourceMetadata connector, DatasetHandle ds, DatasetConfig dsConfig, NamespaceKey dsPath,
                           boolean quitBeforeSaving, boolean opportunisticSave, long maxSingleSplitPartitionchunks, boolean validateConsistency) throws NamespaceException, IOException {
    try (DatasetMetadataSaver metadataSaver = namespaceService.newDatasetMetadataSaver(dsPath, dsConfig.getId(), currentCompression, maxSingleSplitPartitionchunks, validateConsistency)) {
      final PartitionChunkListing chunks = connector.listPartitionChunks(ds);
      final Iterator<? extends PartitionChunk> chunkIterator = chunks.iterator();
      while (chunkIterator.hasNext()) {
        final PartitionChunk chunk = chunkIterator.next();

        final Iterator<? extends com.dremio.connector.metadata.DatasetSplit> splits = chunk.getSplits().iterator();
        while (splits.hasNext()) {
          final com.dremio.connector.metadata.DatasetSplit split = splits.next();
          metadataSaver.saveDatasetSplit(split);
        }
        metadataSaver.savePartitionChunk(chunk);
      }
      if (quitBeforeSaving) {
        return;
      }
      metadataSaver.saveDataset(dsConfig, opportunisticSave);
    }
  }


  private void testSaveHelperWithCompression(int numPartitionChunksPerDataset, int numSplitsPerPartitionChunk,
                                             NamespaceKey dsPath, boolean quitBeforeSaving,
                                             NamespaceService.SplitCompression compression, long maxSingleSplitPartitionChunks,
                                             boolean validateConsistency) throws NamespaceException, IOException {
    currentCompression = compression;

    final SampleSourceMetadata s1 = new SampleSourceMetadata();
    s1.addNDatasets(1, numPartitionChunksPerDataset, numSplitsPerPartitionChunk);

    final List<DatasetHandle> dsHandles = ImmutableList.copyOf(s1.listDatasetHandles().iterator());
    assertEquals(1, dsHandles.size());
    final DatasetHandle ds = dsHandles.get(0);
    final DatasetConfig dsConfig = convert(ds, 1);

    saveDataset(s1, ds, dsConfig, dsPath, quitBeforeSaving, false, maxSingleSplitPartitionChunks, validateConsistency);

    if (quitBeforeSaving) {
      checkSplits(dsConfig, 0, 0, quitBeforeSaving, false);
    } else {
      checkSplits(dsConfig, numPartitionChunksPerDataset, numSplitsPerPartitionChunk, quitBeforeSaving, false);
      DatasetConfig datasetConfig = namespaceService.getDataset(dsPath);
      namespaceService.deleteDataset(dsPath, datasetConfig.getTag());
    }
  }

  private void testSaveHelper(int numPartitionChunksPerDataset, int numSplitsPerPartitionChunk, NamespaceKey dsPath, boolean quitBeforeSaving, long maxSingleSplitPartitionChunks, boolean validateConsistency) throws NamespaceException, IOException {
    testSaveHelperWithCompression(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, quitBeforeSaving,
      NamespaceService.SplitCompression.UNCOMPRESSED, maxSingleSplitPartitionChunks, validateConsistency);
    testSaveHelperWithCompression(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, quitBeforeSaving,
      NamespaceService.SplitCompression.SNAPPY, maxSingleSplitPartitionChunks, validateConsistency);
  }

  // Single partition, single split
  @Test
  public void testSinglePartitionSingleSplit() throws Exception {
    final int numPartitionChunksPerDataset = 1;
    final int numSplitsPerPartitionChunk = 1;
    final NamespaceKey dsPath = new NamespaceKey(asList("dataset_1_1"));
    testSaveHelper(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, false, Long.MAX_VALUE, false);
  }

  // Single partition, multiple splits
  @Test
  public void testSinglePartitionMultiSplit() throws Exception {
    final int numPartitionChunksPerDataset = 1;
    final int numSplitsPerPartitionChunk = 5;
    final NamespaceKey dsPath = new NamespaceKey(asList("dataset_1_n"));
    testSaveHelper(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, false, Long.MAX_VALUE, false);
  }

  // Multiple partitions, single split
  @Test
  public void testMultiPartitionSingleSplit() throws Exception {
    final int numPartitionChunksPerDataset = 3;
    final int numSplitsPerPartitionChunk = 1;
    final NamespaceKey dsPath = new NamespaceKey(asList("dataset_n_1"));
    testSaveHelper(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, false, Long.MAX_VALUE, false);
  }

  // Multiple partitions, single split with consistency validation
  @Test
  public void testMultiPartitionSingleSplitWithConsistency() throws Exception {
    final int numPartitionChunksPerDataset = 3;
    final int numSplitsPerPartitionChunk = 1;
    final NamespaceKey dsPath = new NamespaceKey(asList("dataset_n_1"));
    testSaveHelper(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, false, Long.MAX_VALUE, true);
  }

  // Multiple partitions, multiple splits
  @Test
  public void testMultiPartitionMultiSplit() throws Exception {
    final int numPartitionChunksPerDataset = 3;
    final int numSplitsPerPartitionChunk = 7;
    final NamespaceKey dsPath = new NamespaceKey(asList("dataset_m_n"));
    testSaveHelper(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, false, Long.MAX_VALUE, false);
  }

  // Multiple partitions, multiple splits with consistency validation
  @Test
  public void testMultiPartitionMultiSplitWithConsistency() throws Exception {
    final int numPartitionChunksPerDataset = 3;
    final int numSplitsPerPartitionChunk = 7;
    final NamespaceKey dsPath = new NamespaceKey(asList("dataset_m_n"));
    testSaveHelper(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, false, Long.MAX_VALUE, true);
  }

  // Single partition, single split, fail to save
  @Test
  public void testSinglePartitionSingleSplitFail() throws Exception {
    final int numPartitionChunksPerDataset = 1;
    final int numSplitsPerPartitionChunk = 1;
    final NamespaceKey dsPath = new NamespaceKey(asList("fail_1_1"));
    testSaveHelper(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, true, Long.MAX_VALUE, false);
  }

  // Single partition, multiple splits, fail to save
  @Test
  public void testSinglePartitionMultiSplitFail() throws Exception {
    final int numPartitionChunksPerDataset = 1;
    final int numSplitsPerPartitionChunk = 5;
    final NamespaceKey dsPath = new NamespaceKey(asList("fail_1_n"));
    testSaveHelper(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, true, Long.MAX_VALUE, false);
  }

  // Multiple partitions, single split, fail to save
  @Test
  public void testMultiPartitionSingleSplitFail() throws Exception {
    final int numPartitionChunksPerDataset = 3;
    final int numSplitsPerPartitionChunk = 1;
    final NamespaceKey dsPath = new NamespaceKey(asList("fail_n_1"));
    testSaveHelper(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, true, Long.MAX_VALUE, false);
  }

  // Multiple partitions, multiple splits
  @Test
  public void testMultiPartitionMultiSplitFail() throws Exception {
    final int numPartitionChunksPerDataset = 3;
    final int numSplitsPerPartitionChunk = 7;
    final NamespaceKey dsPath = new NamespaceKey(asList("fail_m_n"));
    testSaveHelper(numPartitionChunksPerDataset, numSplitsPerPartitionChunk, dsPath, true, Long.MAX_VALUE, false);
  }

  // Test behavior when we have a legacy single split
  @Test
  public void testLegacySingleSplit() throws Exception {
    EntityPath entityPath = new EntityPath(ImmutableList.of("legacy", "one"));
    long splitVersion = 17;
    final DatasetConfig dsConfig = new DatasetConfig();

    dsConfig.setId(new EntityId()
      .setId(UUID.randomUUID().toString()));
    dsConfig.setCreatedAt(System.currentTimeMillis());
    dsConfig.setName(entityPath.getName());
    dsConfig.setFullPathList(entityPath.getComponents());
    dsConfig.setType(DatasetType.PHYSICAL_DATASET);

    // To make it work with save, we need a read definition that contains a split version
    ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(splitVersion);
    dsConfig.setReadDefinition(readDefinition);

    PartitionProtobuf.PartitionChunk split = PartitionProtobuf.PartitionChunk.newBuilder()
      .setSize(1)
      .setRowCount(10)
      .setSplitKey("legacy_1")
      .setPartitionExtendedProperty(ByteString.copyFromUtf8("p0_s0"))
      .addAffinities(PartitionProtobuf.Affinity
        .newBuilder()
        .setFactor(2.0)
        .setHost("host")
        .build())
      .build();

    ((NamespaceServiceImpl)namespaceService).directInsertLegacySplit(dsConfig, split, splitVersion);
    checkSplits(dsConfig, 1, 1, false, true);
  }

  // Test behavior when we have a multiple legacy split
  @Test
  public void testLegacyMultiSplit() throws Exception {
    final int numPartitionChunksPerDataset = 3;
    EntityPath entityPath = new EntityPath(ImmutableList.of("legacy", "one"));
    long splitVersion = 17;
    final DatasetConfig dsConfig = new DatasetConfig();

    dsConfig.setId(new EntityId()
      .setId(UUID.randomUUID().toString()));
    dsConfig.setCreatedAt(System.currentTimeMillis());
    dsConfig.setName(entityPath.getName());
    dsConfig.setFullPathList(entityPath.getComponents());
    dsConfig.setType(DatasetType.PHYSICAL_DATASET);

    // To make it work with save, we need a read definition that contains a split version
    ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(splitVersion);
    dsConfig.setReadDefinition(readDefinition);

    for (int p = 0; p < numPartitionChunksPerDataset; p++) {
      PartitionProtobuf.PartitionChunk split = PartitionProtobuf.PartitionChunk.newBuilder()
        .setSize(1)
        .setRowCount(10)
        .setSplitKey(String.format("legacy_%d", p))
        .setPartitionExtendedProperty(ByteString.copyFromUtf8(String.format("p%d_s0", p)))
        .addAffinities(PartitionProtobuf.Affinity
          .newBuilder()
          .setFactor(2.0)
          .setHost("host")
          .build())
        .build();
      ((NamespaceServiceImpl) namespaceService).directInsertLegacySplit(dsConfig, split, splitVersion);
    }
    checkSplits(dsConfig, numPartitionChunksPerDataset, 1, false, true);
  }

  @Test
  public void testOpportunisticSave() throws Exception {
    final SampleSourceMetadata s = new SampleSourceMetadata();
    s.addNDatasets(1, 1, 1);

    final List<DatasetHandle> dsHandles = ImmutableList.copyOf(s.listDatasetHandles().iterator());
    assertEquals(1, dsHandles.size());
    final DatasetHandle ds = dsHandles.get(0);
    final DatasetConfig dsConfig = convert(ds, 1);

    final NamespaceKey dsPath = new NamespaceKey(asList("opp_save_1"));
    saveDataset(s, ds, dsConfig, dsPath, false, false, Long.MAX_VALUE, false);

    dsConfig.setId(new EntityId().setId("Bogus"));
    expectedException.expect(new ExceptionMatcher<>("There already exists an entity", UserException.class));
    saveDataset(s, ds, dsConfig, dsPath, false, true, Long.MAX_VALUE, false);
  }

  private static class ExceptionMatcher<T extends Throwable> extends TypeSafeMatcher<T> {
    private final Class<T> exceptionClazz;
    private final String expectedMessage;
    ExceptionMatcher(String expectedMessage, Class<T> clazz) {
      this.expectedMessage = expectedMessage;
      exceptionClazz = clazz;
    }
    @Override
    protected boolean matchesSafely(T e) {
      return exceptionClazz.isInstance(e) && e.getMessage().contains(expectedMessage);
    }
    @Override
    public void describeTo(final Description description) {
      description.appendText(exceptionClazz.getName());
      description.appendText(" containing the message: ");
      description.appendText(expectedMessage);
    }
  }

  @Test
  public void testSingleSplitPartitionChunks() throws Exception {
    final SampleSourceMetadata s1 = new SampleSourceMetadata();
    // save a dataset with 10 single split partition chunks by setting direct saving limit to 5
    final NamespaceKey dsPath = new NamespaceKey(asList("single_split_partitions_10_1"));
    final SampleSourceMetadata s = new SampleSourceMetadata();
    s.addNDatasets(1, 10, 1);
    final List<DatasetHandle> dsHandles = ImmutableList.copyOf(s.listDatasetHandles().iterator());
    assertEquals(1, dsHandles.size());
    final DatasetHandle ds = dsHandles.get(0);
    final DatasetConfig dsConfig = convert(ds, 1);
    currentCompression = NamespaceService.SplitCompression.UNCOMPRESSED;
    saveDataset(s, ds, dsConfig, dsPath, false, false, 5, false);

    List<PartitionChunkMetadata> partitionChunks =
      ImmutableList.copyOf(namespaceService.findSplits(new LegacyIndexedStore.LegacyFindByCondition().setCondition(PartitionChunkId.getSplitsQuery(dsConfig))));

    // ensure only 5 partition chunks are saved with split
    assertEquals(partitionChunks.size(), 10);
    int numDirectSplit = 0;
    for (PartitionChunkMetadata partitionChunkMetadata : partitionChunks) {
      assertEquals(partitionChunkMetadata.getSplitCount(), 1);
      if (((AbstractPartitionChunkMetadata) partitionChunkMetadata).hasDatasetSplit()) {
        numDirectSplit++;
      }
    }
    assertEquals(numDirectSplit, 5);
  }
}
