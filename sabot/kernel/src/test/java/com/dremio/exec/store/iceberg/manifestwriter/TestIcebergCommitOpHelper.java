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
package com.dremio.exec.store.iceberg.manifestwriter;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dremio.exec.physical.base.TableFormatWriterOptions;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.metadatarefresh.committer.ReadSignatureProvider;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

public class TestIcebergCommitOpHelper extends BaseTestOperator {

  private static final int BATCH_SIZE = 3;
  private static final int WORK_BUF_SIZE = 32768;

  private static final String SOURCE_TABLE_ROOT = "/mock/path/to/table";
  private static final String METADATA_ROOT = "/mock/path/to/metadata";

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "col1", Types.IntegerType.get()),
          Types.NestedField.optional(2, "col2", Types.IntegerType.get()),
          Types.NestedField.optional(3, "part1", Types.IntegerType.get()),
          Types.NestedField.optional(4, "part2", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("part1").identity("part2").build();

  private static final Schema UNPARTITIONED_SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "col1", Types.IntegerType.get()),
          Types.NestedField.optional(2, "col2", Types.IntegerType.get()));

  private static final PartitionSpec UNPARTITIONED_SPEC = PartitionSpec.builderFor(SCHEMA).build();

  private static final PartitionKey PARTITION_1A = createPartitionKey(SCHEMA, SPEC, 1, "a");
  private static final PartitionKey PARTITION_1B = createPartitionKey(SCHEMA, SPEC, 1, "b");
  private static final PartitionKey PARTITION_1C = createPartitionKey(SCHEMA, SPEC, 1, "c");
  private static final PartitionKey PARTITION_2A = createPartitionKey(SCHEMA, SPEC, 2, "a");
  private static final PartitionKey PARTITION_2B = createPartitionKey(SCHEMA, SPEC, 2, "b");

  private static final SchemaConverter SCHEMA_CONVERTER = SchemaConverter.getBuilder().build();

  private static FileSystemPlugin<?> metadataPlugin;
  private static StoragePlugin sourceTablePlugin;
  private static FileSystem metadataFileSystem;
  private static FileSystem sourceTableFileSystem;

  private ArrowBuf workBuf;

  @Before
  public void beforeTest() throws Exception {
    workBuf = getTestAllocator().buffer(WORK_BUF_SIZE);
    testCloseables.add(workBuf);

    metadataPlugin = mock(FileSystemPlugin.class, RETURNS_DEEP_STUBS);
    metadataFileSystem = mock(FileSystem.class);
    sourceTablePlugin =
        mock(
            StoragePlugin.class,
            withSettings()
                .defaultAnswer(RETURNS_DEEP_STUBS)
                .extraInterfaces(SupportsInternalIcebergTable.class));
    sourceTableFileSystem = mock(FileSystem.class);
    SupportsInternalIcebergTable supportsInternalIcebergTable =
        (SupportsInternalIcebergTable) sourceTablePlugin;
    when(supportsInternalIcebergTable.createFS(any(), any(), any()))
        .thenReturn(sourceTableFileSystem);
    when(supportsInternalIcebergTable.createReadSignatureProvider(
            any(), any(), anyLong(), any(), any(), anyBoolean(), anyBoolean()))
        .thenAnswer(
            invocation ->
                new MockReadSignatureProvider(
                    invocation.getArgument(3), invocation.getArgument(4)));
  }

  @Test
  public void testIncrementalRefreshPartitionPathExistenceChecks() throws Exception {
    List<String> partitionPaths =
        ImmutableList.of(
            partitionPath(PARTITION_1A),
            partitionPath(PARTITION_1B),
            partitionPath(PARTITION_1C),
            partitionPath(PARTITION_2A));
    IcebergCommitOpHelper helper =
        createCommitOpHelper(
            IcebergCommandType.INCREMENTAL_METADATA_REFRESH, partitionPaths, SCHEMA, SPEC);

    VectorContainer input = createInputContainer();
    addInputRow(
        input,
        createDataFile("delete1", SPEC, PARTITION_1C),
        OperationType.DELETE_DATAFILE,
        SPEC,
        ImmutableList.of(PARTITION_1C));
    addInputRow(
        input,
        createDataFile("delete2", SPEC, PARTITION_1C),
        OperationType.DELETE_DATAFILE,
        SPEC,
        ImmutableList.of(PARTITION_1C));
    addInputRow(
        input,
        createDataFile("subdir/delete3", SPEC, PARTITION_2A),
        OperationType.DELETE_DATAFILE,
        SPEC,
        ImmutableList.of(PARTITION_2A));
    addInputRow(
        input,
        createDataFile("no_partition_path_match", SPEC, PARTITION_2B),
        OperationType.DELETE_DATAFILE,
        SPEC,
        ImmutableList.of(PARTITION_2B));
    addInputRow(
        input,
        createDeleteFile("posDelete1", SPEC, PARTITION_1C),
        OperationType.DELETE_DELETEFILE,
        SPEC,
        ImmutableList.of(PARTITION_1C));
    addInputRow(
        input,
        createDeleteFile("subdir/posDelete2", SPEC, PARTITION_2A),
        OperationType.DELETE_DELETEFILE,
        SPEC,
        ImmutableList.of(PARTITION_2A));
    addInputRow(
        input,
        createDeleteFile("no_partition_path_match_pos_delete", SPEC, PARTITION_2B),
        OperationType.DELETE_DELETEFILE,
        SPEC,
        ImmutableList.of(PARTITION_2B));

    helper.setup(input);
    helper.consumeData(input.getRecordCount());
    helper.commit(null);

    // Verify that partition existence checks are only done for PARTITION_1C and PARTITION_2A.  The
    // 4th file delete
    // with PARTITION_2B should not trigger an existence check either as it does not exist in the
    // partition path
    // list passed to IcebergCommitOpHelper.
    verify(sourceTableFileSystem, times(2)).exists(any());
    verify(sourceTableFileSystem, never()).exists(Path.of(partitionPath(PARTITION_1A)));
    verify(sourceTableFileSystem, never()).exists(Path.of(partitionPath(PARTITION_1B)));
    verify(sourceTableFileSystem, times(1)).exists(Path.of(partitionPath(PARTITION_1C)));
    verify(sourceTableFileSystem, times(1)).exists(Path.of(partitionPath(PARTITION_2A)));
    verify(sourceTableFileSystem, never()).exists(Path.of(partitionPath(PARTITION_2B)));
  }

  @Test
  public void testFullRefreshPartitionPathExistenceChecks() throws Exception {
    List<String> partitionPaths =
        ImmutableList.of(
            partitionPath(PARTITION_1A),
            partitionPath(PARTITION_1B),
            partitionPath(PARTITION_1C),
            partitionPath(PARTITION_2A));
    IcebergCommitOpHelper helper =
        createCommitOpHelper(
            IcebergCommandType.FULL_METADATA_REFRESH, partitionPaths, SCHEMA, SPEC);

    // existence checks are independent of added manifests - skip adding any for simplicity
    VectorContainer input = createInputContainer();

    helper.setup(input);
    helper.consumeData(input.getRecordCount());
    helper.commit(null);

    // Verify that partition existence checks are done for all 4 partitions in the partitionsPath
    // list.
    verify(sourceTableFileSystem, times(4)).exists(any());
    verify(sourceTableFileSystem, times(1)).exists(Path.of(partitionPath(PARTITION_1A)));
    verify(sourceTableFileSystem, times(1)).exists(Path.of(partitionPath(PARTITION_1B)));
    verify(sourceTableFileSystem, times(1)).exists(Path.of(partitionPath(PARTITION_1C)));
    verify(sourceTableFileSystem, times(1)).exists(Path.of(partitionPath(PARTITION_2A)));
  }

  @Test
  public void testIncrementalRefreshUnpartitionedExistenceChecks() throws Exception {
    List<String> partitionPaths = ImmutableList.of();
    IcebergCommitOpHelper helper =
        createCommitOpHelper(
            IcebergCommandType.INCREMENTAL_METADATA_REFRESH,
            partitionPaths,
            UNPARTITIONED_SCHEMA,
            UNPARTITIONED_SPEC);

    VectorContainer input = createInputContainer();
    addInputRow(
        input,
        createDataFile("delete1", UNPARTITIONED_SPEC),
        OperationType.DELETE_DATAFILE,
        UNPARTITIONED_SPEC,
        ImmutableList.of());
    addInputRow(
        input,
        createDataFile("delete2", UNPARTITIONED_SPEC),
        OperationType.DELETE_DATAFILE,
        UNPARTITIONED_SPEC,
        ImmutableList.of());
    addInputRow(
        input,
        createDeleteFile("posDelete1", UNPARTITIONED_SPEC),
        OperationType.DELETE_DELETEFILE,
        UNPARTITIONED_SPEC,
        ImmutableList.of());
    addInputRow(
        input,
        createDeleteFile("posDelete2", UNPARTITIONED_SPEC),
        OperationType.DELETE_DELETEFILE,
        UNPARTITIONED_SPEC,
        ImmutableList.of());

    helper.setup(input);
    helper.consumeData(input.getRecordCount());
    helper.commit(null);

    // Verify that existence checks are only done for the table root one time.
    verify(sourceTableFileSystem, times(1)).exists(any());
    verify(sourceTableFileSystem, times(1)).exists(Path.of(SOURCE_TABLE_ROOT));
  }

  @Test
  public void testRevertCalledOnCommitException() throws Exception {
    IcebergCommitOpHelper commitOpHelper =
        createCommitOpHelper(
            IcebergCommandType.INSERT,
            ImmutableList.of(),
            UNPARTITIONED_SCHEMA,
            UNPARTITIONED_SPEC);

    VectorContainer input = createInputContainer();
    addInputRow(
        input,
        createDataFile("df1", UNPARTITIONED_SPEC),
        OperationType.ADD_DATAFILE,
        UNPARTITIONED_SPEC,
        ImmutableList.of());

    commitOpHelper.setup(input);
    commitOpHelper.consumeData(input.getRecordCount());
    when(commitOpHelper.icebergOpCommitter.commit(null)).thenThrow(new RuntimeException(""));

    IcebergCommitOpHelper finalCommitOpHelper = spy(commitOpHelper);
    assertThrows(RuntimeException.class, () -> finalCommitOpHelper.commit(null));
    verify(finalCommitOpHelper, times(1)).revertHistoryEvents(any());
  }

  private IcebergCommitOpHelper createCommitOpHelper(
      IcebergCommandType type, List<String> partitionPaths, Schema schema, PartitionSpec spec)
      throws Exception {
    WriterCommitterPOP pop = getPop(type, schema, spec, partitionPaths);
    BufferAllocator allocator =
        getTestAllocator()
            .newChildAllocator(
                "operatorContext",
                getTestAllocator().getInitReservation(),
                getTestAllocator().getLimit());
    final OperatorContextImpl context =
        testContext.getNewOperatorContext(allocator, pop, BATCH_SIZE, null);
    testCloseables.add(context);

    return new IcebergCommitOpHelper(context, pop, metadataFileSystem);
  }

  private VectorContainer createInputContainer() {
    VectorContainer container = new VectorContainer(getTestAllocator());
    testCloseables.add(container);
    container.addSchema(RecordWriter.SCHEMA);
    container.buildSchema();
    container.setRecordCount(0);

    return container;
  }

  private void addInputRow(
      VectorContainer container,
      Object icebergFile,
      OperationType operationType,
      PartitionSpec spec,
      List<PartitionKey> partitionKeys)
      throws Exception {
    VarBinaryVector icebergMetadataVector = container.addOrGet(RecordWriter.ICEBERG_METADATA);
    IntVector operationTypeVector = container.addOrGet(RecordWriter.OPERATION_TYPE);
    ListVector partitionDataVector = container.addOrGet(RecordWriter.PARTITION_DATA);
    int index = container.getRecordCount();

    IcebergMetadataInformation metaInfo =
        new IcebergMetadataInformation(IcebergSerDe.serializeToByteArray(icebergFile));
    byte[] metaInfoBytes = IcebergSerDe.serializeToByteArray(metaInfo);
    icebergMetadataVector.setSafe(container.getRecordCount(), metaInfoBytes);

    operationTypeVector.setSafe(container.getRecordCount(), operationType.value);

    UnionListWriter writer = partitionDataVector.getWriter();
    writer.setPosition(index);
    writer.startList();
    for (PartitionKey key : partitionKeys) {
      IcebergPartitionData partData = IcebergPartitionData.fromStructLike(spec, key);
      byte[] partDataBytes = IcebergSerDe.serializeToByteArray(partData);
      Preconditions.checkState(partDataBytes.length < WORK_BUF_SIZE);
      workBuf.setBytes(0, partDataBytes);
      writer.writeVarBinary(0, partDataBytes.length, workBuf);
    }
    writer.endList();

    container.setAllCount(container.getRecordCount() + 1);
  }

  private WriterCommitterPOP getPop(
      IcebergCommandType type, Schema schema, PartitionSpec spec, List<String> partitionPaths) {

    DatasetConfig datasetConfig =
        new DatasetConfig()
            .setReadDefinition(
                new ReadDefinition().setReadSignature(io.protostuff.ByteString.EMPTY));
    return new WriterCommitterPOP(
        PROPS,
        null,
        METADATA_ROOT,
        getIcebergTableProps(type, schema, spec, partitionPaths),
        new NamespaceKey(ImmutableList.of("test", "test_table")),
        Optional.of(datasetConfig),
        null,
        metadataPlugin,
        sourceTablePlugin,
        false,
        true,
        mock(TableFormatWriterOptions.class),
        null);
  }

  private IcebergTableProps getIcebergTableProps(
      IcebergCommandType type, Schema schema, PartitionSpec spec, List<String> partitionPaths) {
    IcebergTableProps props =
        new IcebergTableProps(
            "/path/to/metadata",
            UUID.randomUUID().toString(),
            SCHEMA_CONVERTER.fromIceberg(schema),
            spec.fields().stream().map(PartitionField::name).collect(Collectors.toList()),
            type,
            "test_db",
            "test_table",
            SOURCE_TABLE_ROOT,
            null,
            io.protostuff.ByteString.copyFrom(IcebergSerDe.serializePartitionSpec(spec)),
            IcebergSerDe.serializedSchemaAsJson(schema),
            null,
            null,
            Collections.emptyMap(),
            null);
    props.setPartitionPaths(partitionPaths);

    return props;
  }

  private static PartitionKey createPartitionKey(
      Schema schema, PartitionSpec spec, Object... values) {
    Preconditions.checkArgument(values.length == spec.fields().size());
    PartitionKey key = new PartitionKey(spec, schema);
    for (int i = 0; i < values.length; i++) {
      key.set(i, values[i]);
    }

    return key;
  }

  private static String partitionPath(PartitionKey partitionKey) {
    return SOURCE_TABLE_ROOT + "/" + partitionKey.toPath();
  }

  private static DataFile createDataFile(
      String name, PartitionSpec spec, PartitionKey partitionKey) {
    return DataFiles.builder(spec)
        .withPath(partitionPath(partitionKey) + "/" + name)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(1)
        .withRecordCount(1)
        .withPartition(partitionKey)
        .build();
  }

  private static DataFile createDataFile(String name, PartitionSpec spec) {
    return DataFiles.builder(spec)
        .withPath(SOURCE_TABLE_ROOT + "/" + name)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(1)
        .withRecordCount(1)
        .build();
  }

  private static DeleteFile createDeleteFile(
      String name, PartitionSpec spec, PartitionKey partitionKey) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPath(partitionPath(partitionKey) + "/" + name)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(1)
        .withRecordCount(1)
        .withPartition(partitionKey)
        .build();
  }

  private static DeleteFile createDeleteFile(String name, PartitionSpec spec) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPath(SOURCE_TABLE_ROOT + "/" + name)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(1)
        .withRecordCount(1)
        .build();
  }

  /**
   * A mock ReadSignatureProvider implementation which simply will call the partitionExists
   * predicate for each partition directory.
   */
  private static class MockReadSignatureProvider implements ReadSignatureProvider {

    private final List<String> partitionPaths;
    private final Predicate<String> partitionExists;

    MockReadSignatureProvider(List<String> partitionPaths, Predicate<String> partitionExists) {
      this.partitionPaths = partitionPaths;
      this.partitionExists = partitionExists;
    }

    @Override
    public ByteString compute(
        Set<IcebergPartitionData> addedPartitions, Set<IcebergPartitionData> deletedPartitions) {

      if (partitionPaths.isEmpty()) {
        boolean ignore = partitionExists.test(SOURCE_TABLE_ROOT);
      } else {
        for (String path : partitionPaths) {
          boolean ignore = partitionExists.test(path);
        }
      }

      return ByteString.EMPTY;
    }
  }
}
