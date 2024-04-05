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
package com.dremio.exec.store;

import static com.dremio.exec.store.SystemSchemas.PATH;
import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH;
import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_SIZE;
import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.MODIFICATION_TIME;
import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.PARTITION_INFO;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.config.DirListingTableFunctionContext;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingRecordReader;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;

public class TestDirListingTableFunction extends BaseTestTableFunction {

  private FileSystem fs;
  private static BufferAllocator testAllocator;

  @Mock(
      extraInterfaces = {
        SupportsIcebergRootPointer.class,
        SupportsInternalIcebergTable.class,
        MutablePlugin.class
      })
  private FileSystemPlugin plugin;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void prepareMocks() throws Exception {
    testAllocator = allocatorRule.newAllocator("test-DirListTF-allocator", 0, Long.MAX_VALUE);

    fs = mock(HadoopFileSystem.class);
    when(fec.getStoragePlugin(any())).thenReturn(plugin);
    when(plugin.createFSWithoutHDFSCache(anyString(), anyString(), any())).thenReturn(fs);
  }

  @After
  public void cleanup() throws Exception {
    testAllocator.close();
  }

  private OperatorContext getCtx() {
    OperatorContext operatorContext = mock(OperatorContext.class, RETURNS_DEEP_STUBS);
    when(operatorContext.getAllocator()).thenReturn(testAllocator);
    when(operatorContext.getTargetBatchSize()).thenReturn(4000);
    when(operatorContext.getMinorFragmentEndpoints())
        .thenReturn(ImmutableList.of(new MinorFragmentEndpoint(0, null)));
    when(operatorContext.getFunctionContext().getContextInformation().getQueryStartTime())
        .thenReturn(TestDirListingRecordReader.START_TIME);
    when(operatorContext.getOptions().getOption(ExecConstants.DIR_LISTING_EXCLUDE_FUTURE_MOD_TIMES))
        .thenReturn(true);
    return operatorContext;
  }

  private static final TableFunctionPOP TABLE_FUNCTION_POP =
      new TableFunctionPOP(
          PROPS,
          null,
          new TableFunctionConfig(
              TableFunctionConfig.FunctionType.DIR_LISTING,
              true,
              new DirListingTableFunctionContext(
                  MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.BATCH_SCHEMA,
                  null,
                  null,
                  MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA
                      .BATCH_SCHEMA
                      .getFields()
                      .stream()
                      .map(f -> SchemaPath.getSimplePath(f.getName()))
                      .collect(ImmutableList.toImmutableList()),
                  true,
                  true)));

  @Test
  public void testRecursive() throws Exception {
    when(plugin.createDirListRecordReader(any(), any(), any(), anyBoolean(), any(), any()))
        .thenAnswer(
            i ->
                new DirListingRecordReader(
                    getCtx(), fs, i.getArgument(2), true, null, null, true, false));
    Path inputPath = Path.of("/path1/");
    TestDirListingRecordReader.setupFsListIteratorMock((HadoopFileSystem) fs, inputPath);

    Schema icebergSchema1 =
        new Schema(optional(1000, "dir0", org.apache.iceberg.types.Types.StringType.get()));
    Schema icebergSchema2 =
        new Schema(
            optional(1000, "dir0", org.apache.iceberg.types.Types.StringType.get()),
            optional(1001, "dir1", org.apache.iceberg.types.Types.StringType.get()));

    IcebergPartitionData partitionData1 =
        new IcebergPartitionData(PartitionSpec.unpartitioned().partitionType());
    IcebergPartitionData partitionData2 =
        new IcebergPartitionData(
            PartitionSpec.builderFor(icebergSchema1).identity("dir0").build().partitionType());
    partitionData2.set(0, "bar");
    IcebergPartitionData partitionData3 =
        new IcebergPartitionData(
            PartitionSpec.builderFor(icebergSchema2)
                .identity("dir0")
                .identity("dir1")
                .build()
                .partitionType());
    partitionData3.set(0, "bar");
    partitionData3.set(1, "subBar1");

    Fixtures.Table input = t(th(PATH), tr(inputPath.toString()));
    Fixtures.Table output =
        t(
            th(FILE_PATH, FILE_SIZE, MODIFICATION_TIME, PARTITION_INFO),
            tr(
                "/path1/foo.parquet?version=1",
                20L,
                1L,
                IcebergSerDe.serializeToByteArray(partitionData1)),
            tr(
                "/path1/bar/file1.parquet?version=31",
                70L,
                31L,
                IcebergSerDe.serializeToByteArray(partitionData2)),
            tr(
                "/path1/bar/subBar1/file2.parquet?version=32",
                1200L,
                32L,
                IcebergSerDe.serializeToByteArray(partitionData3)),
            tr(
                "/path1/bar/subBar1/file3.parquet?version=312",
                1200L,
                312L,
                IcebergSerDe.serializeToByteArray(partitionData3)),
            tr(
                "/path1/bar/subBar1/file4.parquet?version=331",
                1400L,
                331L,
                IcebergSerDe.serializeToByteArray(partitionData3)));
    validateSingle(TABLE_FUNCTION_POP, TableFunctionOperator.class, input, output, 10);
  }

  @Test
  public void testHiveRecursive() throws Exception {
    BatchSchema tableSchema =
        BatchSchema.of(
            CompleteType.INT.toField("integerCol"),
            CompleteType.DOUBLE.toField("doubleCol"),
            CompleteType.BIT.toField("bitField"),
            CompleteType.VARCHAR.toField("varCharField"));

    List<PartitionProtobuf.PartitionValue> partitionValues = new ArrayList<>();
    partitionValues.add(
        PartitionProtobuf.PartitionValue.newBuilder()
            .setColumn("integerCol")
            .setIntValue(20)
            .build());
    partitionValues.add(
        PartitionProtobuf.PartitionValue.newBuilder()
            .setColumn("doubleCol")
            .setDoubleValue(20.0D)
            .build());
    partitionValues.add(
        PartitionProtobuf.PartitionValue.newBuilder()
            .setColumn("bitField")
            .setBitValue(true)
            .build());
    partitionValues.add(
        PartitionProtobuf.PartitionValue.newBuilder()
            .setColumn("varCharField")
            .setStringValue("tempVarCharValue")
            .build());

    Schema icebergSchema =
        new Schema(
            optional(1000, "integerCol", org.apache.iceberg.types.Types.IntegerType.get()),
            optional(1001, "doubleCol", org.apache.iceberg.types.Types.DoubleType.get()),
            optional(1002, "bitField", org.apache.iceberg.types.Types.BooleanType.get()),
            optional(1003, "varCharField", org.apache.iceberg.types.Types.StringType.get()));
    IcebergPartitionData partitionData =
        new IcebergPartitionData(
            PartitionSpec.builderFor(icebergSchema)
                .identity("integerCol")
                .identity("doubleCol")
                .identity("bitField")
                .identity("varCharField")
                .build()
                .partitionType());
    partitionData.set(0, 20);
    partitionData.set(1, 20.0D);
    partitionData.set(2, true);
    partitionData.set(3, "tempVarCharValue");

    when(plugin.createDirListRecordReader(any(), any(), any(), anyBoolean(), any(), any()))
        .thenAnswer(
            i ->
                new DirListingRecordReader(
                    getCtx(),
                    fs,
                    i.getArgument(2),
                    true,
                    tableSchema,
                    partitionValues,
                    false,
                    false));
    Path inputPath = Path.of("/path2/");
    TestDirListingRecordReader.setupFsListIteratorMock((HadoopFileSystem) fs, inputPath);

    Fixtures.Table input = t(th(PATH), tr(inputPath.toString()));
    Fixtures.Table output =
        t(
            th(FILE_PATH, FILE_SIZE, MODIFICATION_TIME, PARTITION_INFO),
            tr(
                "/path2/foo.parquet?version=1",
                20L,
                1L,
                IcebergSerDe.serializeToByteArray(partitionData)),
            tr(
                "/path2/bar/file1.parquet?version=31",
                70L,
                31L,
                IcebergSerDe.serializeToByteArray(partitionData)),
            tr(
                "/path2/bar/subBar1/file2.parquet?version=32",
                1200L,
                32L,
                IcebergSerDe.serializeToByteArray(partitionData)),
            tr(
                "/path2/bar/subBar1/file3.parquet?version=312",
                1200L,
                312L,
                IcebergSerDe.serializeToByteArray(partitionData)),
            tr(
                "/path2/bar/subBar1/file4.parquet?version=331",
                1400L,
                331L,
                IcebergSerDe.serializeToByteArray(partitionData)));
    validateSingle(TABLE_FUNCTION_POP, TableFunctionOperator.class, input, output, 10);
  }

  @Test
  public void testInferPartitions() throws Exception {
    when(plugin.createDirListRecordReader(any(), any(), any(), anyBoolean(), any(), any()))
        .thenAnswer(
            i ->
                new DirListingRecordReader(
                    getCtx(), fs, i.getArgument(2), true, null, null, true, true));
    Path inputPath = Path.of("/path3/");
    TestDirListingRecordReader.setupFsListIteratorMockWithPartitions(
        (HadoopFileSystem) fs, inputPath);

    Schema icebergSchema =
        new Schema(
            optional(1000, "id", org.apache.iceberg.types.Types.StringType.get()),
            optional(1001, "data", org.apache.iceberg.types.Types.StringType.get()),
            optional(1002, "dir0", org.apache.iceberg.types.Types.StringType.get()),
            optional(1003, "dir1", org.apache.iceberg.types.Types.StringType.get()));
    IcebergPartitionData partitionData =
        new IcebergPartitionData(
            PartitionSpec.builderFor(icebergSchema)
                .identity("id")
                .identity("data")
                .identity("dir0")
                .identity("dir1")
                .build()
                .partitionType());
    partitionData.set(0, "1");
    partitionData.set(1, "name");
    partitionData.set(2, "id=1");
    partitionData.set(3, "data=name");
    IcebergPartitionData partitionData2 = partitionData.copy();
    partitionData2.set(0, "2");
    partitionData2.set(1, "value");
    partitionData2.set(2, "id=2");
    partitionData2.set(3, "data=value");
    IcebergPartitionData partitionData3 = partitionData2.copy();
    partitionData3.set(1, "1234");
    partitionData3.set(3, "data=1234");

    Fixtures.Table input = t(th(PATH), tr(inputPath.toString()));
    Fixtures.Table output =
        t(
            th(FILE_PATH, FILE_SIZE, MODIFICATION_TIME, PARTITION_INFO),
            tr(
                "/path3/id=1/data=name/file1.parquet?version=1",
                20L,
                1L,
                IcebergSerDe.serializeToByteArray(partitionData)),
            tr(
                "/path3/id=1/data=name/file2.parquet?version=2",
                40L,
                2L,
                IcebergSerDe.serializeToByteArray(partitionData)),
            tr(
                "/path3/id=1/data=name/file3.parquet?version=3",
                70L,
                3L,
                IcebergSerDe.serializeToByteArray(partitionData)),
            tr(
                "/path3/id=2/data=value/file4.parquet?version=4",
                1010L,
                4L,
                IcebergSerDe.serializeToByteArray(partitionData2)),
            tr(
                "/path3/id=2/data=value/file5.parquet?version=5",
                1200L,
                5L,
                IcebergSerDe.serializeToByteArray(partitionData2)),
            tr(
                "/path3/id=2/data=value/file6.parquet?version=6",
                40L,
                6L,
                IcebergSerDe.serializeToByteArray(partitionData2)),
            tr(
                "/path3/id=2/data=1234/file7.parquet?version=7",
                70L,
                7L,
                IcebergSerDe.serializeToByteArray(partitionData3)));
    validateSingle(TABLE_FUNCTION_POP, TableFunctionOperator.class, input, output, 10);
  }
}
