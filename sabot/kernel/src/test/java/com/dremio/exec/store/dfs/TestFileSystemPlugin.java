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
package com.dremio.exec.store.dfs;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.AccessMode;
import java.util.List;
import java.util.UUID;

import javax.inject.Provider;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.PartitionChunkMetadataImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.protostuff.ByteString;

public class TestFileSystemPlugin {
  private FileSystemPlugin<?> fileSystemPlugin;

  private SabotContext sabotContext;

  private FileSystem fileSystem;

  private static final String TEST_PARQUET_FILE_PATH = "/user/test/data/file.parquet";

  private static final String TEST_CSV_FILE_PATH = "/user/test/data/file.csv";

  private static final String TEST_USER = "TEST_USER";

  private static final NamespaceKey TEST_KEY = new NamespaceKey(ImmutableList.of("SPACE_NAME", "SOURCE_NAME", "DATASET_NAME"));

  @Before
  public void setup() throws Exception {
    sabotContext = mock(SabotContext.class);
    final FileSystemWrapper fileSystemWrapper = mock(FileSystemWrapper.class);
    fileSystem = mock(FileSystem.class);
    final MockFileSystemConf fileSystemConf = new MockFileSystemConf();
    final String name = "TEST";
    when(fileSystemWrapper.wrap(
      any(FileSystem.class),
      eq(name),
      eq(fileSystemConf),
      eq(null),
      eq(false),
      eq(false)))
      .thenReturn(fileSystem);
    when(sabotContext.getFileSystemWrapper())
      .thenReturn(fileSystemWrapper);
    Provider<StoragePluginId> idProvider = () -> null;
    fileSystemPlugin = new FileSystemPlugin<>(fileSystemConf, sabotContext, name, idProvider);
  }

  private PartitionProtobuf.PartitionChunk getEasySplitsPartition(String path) {
    final EasyProtobuf.EasyDatasetSplitXAttr splitExtended = EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
      .setPath(path)
      .setStart(0)
      .setLength(0)
      .setUpdateKey(com.dremio.exec.store.file.proto.FileProtobuf.FileSystemCachedEntity.newBuilder()
        .setPath(path)
        .setLastModificationTime(0))
      .build();
    final PartitionProtobuf.DatasetSplit datasetSplit = PartitionProtobuf.DatasetSplit.newBuilder()
      .setRecordCount(1)
      .setSplitExtendedProperty(splitExtended.toByteString()).build();
    return PartitionProtobuf.PartitionChunk.newBuilder()
      .setDatasetSplit(datasetSplit).setSplitCount(1).build();
  }

  private PartitionProtobuf.PartitionChunk getParquetSplitsPartition(String path) {
    final ParquetProtobuf.ParquetDatasetSplitXAttr splitExtended =
      ParquetProtobuf.ParquetDatasetSplitXAttr.newBuilder()
        .setPath(path)
        .setStart(0)
        .setLength(0)
        .setUpdateKey(
          FileProtobuf.FileSystemCachedEntity.newBuilder()
            .setPath(path)
            .setLastModificationTime(0)
            .setLength(0))
        .build();
    final PartitionProtobuf.DatasetSplit datasetSplit = PartitionProtobuf.DatasetSplit.newBuilder()
      .setRecordCount(1)
      .setSplitExtendedProperty(splitExtended.toByteString()).build();
    return PartitionProtobuf.PartitionChunk.newBuilder()
      .setDatasetSplit(datasetSplit).setSplitCount(1).build();
  }

  @Test
  public void testHasAccessPermissionParquetFile() throws IOException {
    DatasetConfig datasetConfig = new DatasetConfig()
      .setId(new EntityId().setId(UUID.randomUUID().toString()))
      .setName(TEST_KEY.toString())
      .setFullPathList(TEST_KEY.getPathComponents())
      .setPhysicalDataset(new PhysicalDataset().setFormatSettings(
        new ParquetFileConfig().asFileConfig()).setIcebergMetadataEnabled(false))
      .setLastModified(System.currentTimeMillis())
      .setReadDefinition(new ReadDefinition().setReadSignature(ByteString.EMPTY).setSplitVersion(1L))
      .setTotalNumSplits(1);
    Mockito.doNothing().when(fileSystem).access(Path.of(TEST_PARQUET_FILE_PATH), ImmutableSet.of(AccessMode.READ)); // no throw = access granted
    NamespaceService namespaceService = mock(NamespaceService.class);
    when(sabotContext.getNamespaceService(anyString())).thenReturn(namespaceService);

    final PartitionChunkMetadata partitionChunkMetadata = new PartitionChunkMetadataImpl(
      getParquetSplitsPartition(TEST_PARQUET_FILE_PATH), null, () -> {}, () -> null);
    Iterable<PartitionChunkMetadata> partitionsIterable = ImmutableList.of(partitionChunkMetadata);
    when(namespaceService.findSplits(any(LegacyKVStore.LegacyFindByRange.class))).thenReturn(partitionsIterable);
    boolean hasAccess = fileSystemPlugin.hasAccessPermission(TEST_USER, TEST_KEY, datasetConfig);
    Assert.assertTrue(hasAccess);
  }

  /* With unlimited splits enabled (iceberg metadata), the dataset split extended property data for parquet files is
   * serialized as EasyDatasetSplitXAttr, not as ParquetDatasetSplitXAttr
   * Regression test for: DX-51166, DX-37600
   */
  @Test
  public void testHasAccessPermissionParquetFileUnlimitedSplits() throws IOException {
    DatasetConfig datasetConfig = new DatasetConfig()
      .setId(new EntityId().setId(UUID.randomUUID().toString()))
      .setName(TEST_KEY.toString())
      .setFullPathList(TEST_KEY.getPathComponents())
      .setPhysicalDataset(new PhysicalDataset().setFormatSettings(
        new ParquetFileConfig().asFileConfig()).setIcebergMetadataEnabled(true).setIcebergMetadata(new IcebergMetadata()))
      .setLastModified(System.currentTimeMillis())
      .setReadDefinition(new ReadDefinition().setReadSignature(ByteString.EMPTY).setSplitVersion(1L))
      .setTotalNumSplits(1);
    Mockito.doNothing().when(fileSystem).access(Path.of(TEST_PARQUET_FILE_PATH), ImmutableSet.of(AccessMode.READ)); // no throw = access granted
    NamespaceService namespaceService = mock(NamespaceService.class);
    when(sabotContext.getNamespaceService(anyString())).thenReturn(namespaceService);

    final PartitionChunkMetadata partitionChunkMetadata = new PartitionChunkMetadataImpl(
      getEasySplitsPartition(TEST_PARQUET_FILE_PATH), null, () -> {}, () -> null);
    Iterable<PartitionChunkMetadata> partitionsIterable = ImmutableList.of(partitionChunkMetadata);
    when(namespaceService.findSplits(any(LegacyKVStore.LegacyFindByRange.class))).thenReturn(partitionsIterable);
    boolean hasAccess = fileSystemPlugin.hasAccessPermission(TEST_USER, TEST_KEY, datasetConfig);
    Assert.assertTrue(hasAccess);
  }

  @Test
  public void testHasAccessPermissionEasyFile() throws IOException {
    DatasetConfig datasetConfig = new DatasetConfig()
      .setId(new EntityId().setId(UUID.randomUUID().toString()))
      .setName(TEST_KEY.toString())
      .setFullPathList(TEST_KEY.getPathComponents())
      .setPhysicalDataset(new PhysicalDataset().setFormatSettings(new TextFileConfig().asFileConfig()))
      .setLastModified(System.currentTimeMillis())
      .setReadDefinition(new ReadDefinition().setReadSignature(ByteString.EMPTY).setSplitVersion(1L))
      .setTotalNumSplits(1);
    Mockito.doNothing().when(fileSystem).access(Path.of(TEST_CSV_FILE_PATH), ImmutableSet.of(AccessMode.READ)); // no throw = access granted
    NamespaceService namespaceService = mock(NamespaceService.class);
    when(sabotContext.getNamespaceService(anyString())).thenReturn(namespaceService);

    final PartitionChunkMetadata partitionChunkMetadata = new PartitionChunkMetadataImpl(
      getEasySplitsPartition(TEST_CSV_FILE_PATH), null, () -> {}, () -> null);
    Iterable<PartitionChunkMetadata> partitionsIterable = ImmutableList.of(partitionChunkMetadata);
    when(namespaceService.findSplits(any(LegacyKVStore.LegacyFindByRange.class))).thenReturn(partitionsIterable);
    boolean hasAccess = fileSystemPlugin.hasAccessPermission(TEST_USER, TEST_KEY, datasetConfig);
    Assert.assertTrue(hasAccess);
  }

  class MockFileSystemConf extends FileSystemConf<MockFileSystemConf, MockFileSystemPlugin> {

    @Override
    public MockFileSystemPlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      return null;
    }

    @Override
    public Path getPath() {
      return null;
    }

    @Override
    public boolean isImpersonationEnabled() {
      return true;
    }

    @Override
    public List<Property> getProperties() {
      return null;
    }

    @Override
    public String getConnection() {
      return null;
    }

    @Override
    public boolean isPartitionInferenceEnabled() {
      return false;
    }

    @Override
    public SchemaMutability getSchemaMutability() {
      return null;
    }
  }

  class MockFileSystemPlugin extends FileSystemPlugin<MockFileSystemConf> {

    public MockFileSystemPlugin(MockFileSystemConf config, SabotContext context, String name, Provider<StoragePluginId> idProvider) {
      super(config, context, name, idProvider);
    }
  }
}
