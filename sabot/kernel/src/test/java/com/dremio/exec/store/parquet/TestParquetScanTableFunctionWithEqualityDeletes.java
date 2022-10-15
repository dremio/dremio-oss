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
package com.dremio.exec.store.parquet;

import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rs;
import static com.dremio.sabot.RecordSet.st;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileContent;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;

import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.iceberg.deletes.RowLevelDeleteFilterFactory.DeleteFileInfo;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;
import io.protostuff.ByteStringUtil;

public class TestParquetScanTableFunctionWithEqualityDeletes extends BaseTestTableFunction {

  private static final List<SchemaPath> COLUMNS = ImmutableList.of(
      SchemaPath.getSimplePath("product_id"),
      SchemaPath.getSimplePath("category"),
      SchemaPath.getSimplePath("color"));
  private static final BatchSchema OUTPUT_SCHEMA = IcebergTestTables.PRODUCTS_SCHEMA.maskAndReorder(COLUMNS);
  private static final List<String> PARTITION_COLUMNS = ImmutableList.of("category");
  private static final ByteString EXTENDED_PROPS = getExtendedProperties();
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_WIDGET =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(PartitionProtobuf.PartitionValue.newBuilder()
              .setColumn("category")
              .setStringValue("widget")
              .build())
          .build();
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_GADGET =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(PartitionProtobuf.PartitionValue.newBuilder()
              .setColumn("category")
              .setStringValue("gadget")
              .build())
          .build();
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_GIZMO =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(PartitionProtobuf.PartitionValue.newBuilder()
              .setColumn("category")
              .setStringValue("gizmo")
              .build())
          .build();

  // each data file has 2 row groups of 100 rows each
  // contains product_id 0..199
  private static final String DATA_WIDGET_00 = "widget/widget-00.parquet";
  // contains product_id 400..599
  private static final String DATA_WIDGET_01 = "widget/widget-01.parquet";

  // delete rows where product_id >= 0 && product_id < 30
  private static final DeleteFileInfo EQ_DELETE_WIDGET_00 = new DeleteFileInfo(
      "widget/eqdelete-widget-00.parquet", FileContent.EQUALITY_DELETES, 30, ImmutableList.of(1));
  // delete rows where color == "green" (product_id >= 0 && product_id < 200 && product_id % 10 == 5)
  private static final DeleteFileInfo EQ_DELETE_WIDGET_01 = new DeleteFileInfo(
      "widget/eqdelete-widget-01.parquet", FileContent.EQUALITY_DELETES, 20, ImmutableList.of(4));
  // delete rows where (product_id >= 100 && product_id < 200) || (product_id >= 500 && product_id < 600)
  private static final DeleteFileInfo EQ_DELETE_WIDGET_02 = new DeleteFileInfo(
      "widget/eqdelete-widget-02.parquet", FileContent.EQUALITY_DELETES, 200, ImmutableList.of(1));
  // delete rows (positional) where product_id >= 50 && product_id < 53
  private static final DeleteFileInfo DELETE_WIDGET_00 = new DeleteFileInfo(
      "widget/delete-widget-00.parquet", FileContent.POSITION_DELETES, 0, null);

  private static final Predicate<Integer> FILTER_EQ_DELETE_WIDGET_00 = i -> !(i >= 0 && i < 30);
  private static final Predicate<Integer> FILTER_EQ_DELETE_WIDGET_01 = i -> !(i >= 0 && i < 200 && i % 10 == 5);
  private static final Predicate<Integer> FILTER_EQ_DELETE_WIDGET_02 =
      i -> !((i >= 100 && i < 200) || (i >= 500 && i < 600));
  private static final Predicate<Integer> FILTER_DELETE_WIDGET_00 = i -> !(i >= 50 && i < 53);

  private static final List<String> COLORS = ImmutableList.of(
      "black",
      "white",
      "red",
      "orange",
      "yellow",
      "green",
      "blue",
      "purple",
      "brown",
      "gray");

  private static final int BATCH_SIZE = 67;

  private static IcebergTestTables.Table table;

  private FileSystem fs;
  @Mock
  private StoragePluginId pluginId;
  @Mock(extraInterfaces = {SupportsIcebergRootPointer.class, SupportsInternalIcebergTable.class})
  private MutablePlugin plugin;

  @BeforeClass
  public static void initTables() {
    table = IcebergTestTables.PRODUCTS_WITH_EQ_DELETES.get();
  }

  @AfterClass
  public static void closeTables() throws Exception {
    table.close();
  }

  @Before
  public void prepareMocks() throws Exception {
    fs = HadoopFileSystem.get(Path.of("/"), new Configuration());
    when(fec.getStoragePlugin(pluginId)).thenReturn(plugin);
    SupportsIcebergRootPointer sirp = (SupportsIcebergRootPointer) plugin;
    when(sirp.createFSWithAsyncOptions(anyString(), anyString(), any())).thenReturn(fs);
    SupportsInternalIcebergTable siit = (SupportsInternalIcebergTable) plugin;
    when(siit.createScanTableFunction(any(), any(), any(), any())).thenAnswer(i ->
        new ParquetScanTableFunction(i.getArgument(0), i.getArgument(1), i.getArgument(2), i.getArgument(3)));
    when(pluginId.getName()).thenReturn("testplugin");
  }

  @Test
  public void testDataFileWithMultipleRowGroups() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        inputRow(DATA_WIDGET_00, ImmutableList.of(EQ_DELETE_WIDGET_00, EQ_DELETE_WIDGET_01, EQ_DELETE_WIDGET_02)));

    Iterator<Integer> productIds = Stream.iterate(0, i -> i + 1)
        .limit(200)
        .filter(FILTER_EQ_DELETE_WIDGET_00)
        .filter(FILTER_EQ_DELETE_WIDGET_01)
        .filter(FILTER_EQ_DELETE_WIDGET_02)
        .iterator();
    RecordSet output = outputRecordSet(productIds);

    validate(input, output);
  }

  @Test
  public void testWithMultipleDataFilesSharingDeleteFiles() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        inputRow(DATA_WIDGET_00, ImmutableList.of(EQ_DELETE_WIDGET_02)),
        inputRow(DATA_WIDGET_01, ImmutableList.of(EQ_DELETE_WIDGET_02)));

    Iterator<Integer> productIds =
        Stream.concat(
            Stream.iterate(0, i -> i + 1).limit(200),
            Stream.iterate(400, i -> i + 1).limit(200))
        .filter(FILTER_EQ_DELETE_WIDGET_02)
        .iterator();
    RecordSet output = outputRecordSet(productIds);

    validate(input, output);
  }

  @Test
  public void testWithEmptyBlockSplitExpansion() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        // this split should expand to only the 1st rowgroup which ends around offset 2365
        inputRow(DATA_WIDGET_00, 0, 2000,
            ImmutableList.of(EQ_DELETE_WIDGET_00, EQ_DELETE_WIDGET_01, EQ_DELETE_WIDGET_02)),
        // this should expand to nothing
        inputRow(DATA_WIDGET_00, 4000, 2000,
            ImmutableList.of(EQ_DELETE_WIDGET_00, EQ_DELETE_WIDGET_01, EQ_DELETE_WIDGET_02)));

    Iterator<Integer> productIds = Stream.iterate(0, i -> i + 1)
        .limit(100)
        .filter(FILTER_EQ_DELETE_WIDGET_00)
        .filter(FILTER_EQ_DELETE_WIDGET_01)
        .filter(FILTER_EQ_DELETE_WIDGET_02)
        .iterator();
    RecordSet output = outputRecordSet(productIds);

    validate(input, output);
  }

  @Test
  public void testWithNoDeleteFiles() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        inputRow(DATA_WIDGET_00),
        inputRow(DATA_WIDGET_01));

    Iterator<Integer> productIds =
        Stream.concat(
            Stream.iterate(0, i -> i + 1).limit(200),
            Stream.iterate(400, i -> i + 1).limit(200))
        .iterator();
    RecordSet output = outputRecordSet(productIds);

    validate(input, output);
  }

  @Test
  public void testWithEqualityAndPositionDeleteFiles() throws Exception {
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        inputRow(DATA_WIDGET_00,
            ImmutableList.of(EQ_DELETE_WIDGET_00, EQ_DELETE_WIDGET_01, EQ_DELETE_WIDGET_02, DELETE_WIDGET_00)));

    Iterator<Integer> productIds = Stream.iterate(0, i -> i + 1)
        .limit(200)
        .filter(FILTER_EQ_DELETE_WIDGET_00)
        .filter(FILTER_EQ_DELETE_WIDGET_01)
        .filter(FILTER_EQ_DELETE_WIDGET_02)
        .filter(FILTER_DELETE_WIDGET_00)
        .iterator();
    RecordSet output = outputRecordSet(productIds);

    validate(input, output);
  }

  @Test
  public void testWithEqualityFieldsNotInProjectedColumns() throws Exception {
    // project only product_id, with an equality delete file that requires color
    List<SchemaPath> columns = ImmutableList.of(SchemaPath.getSimplePath("product_id"));
    RecordSet input = rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
        inputRow(DATA_WIDGET_00, ImmutableList.of(EQ_DELETE_WIDGET_01)));

    RecordSet.Record[] outputRecords = Stream.iterate(0, i -> i + 1)
        .limit(200)
        .filter(FILTER_EQ_DELETE_WIDGET_01)
        .map(RecordSet::r)
        .toArray(RecordSet.Record[]::new);
    RecordSet output = rs(IcebergTestTables.PRODUCTS_SCHEMA.maskAndReorder(columns), outputRecords);

    validate(input, output, columns);
  }

  private RecordSet.Record inputRow(String relativePath) throws Exception {
    return inputRow(relativePath, 0, -1, ImmutableList.of());
  }

  private RecordSet.Record inputRow(String relativePath, List<DeleteFileInfo> deleteFiles) throws Exception {
    return inputRow(relativePath, 0, -1, deleteFiles);
  }

  private RecordSet.Record inputRow(String relativePath, long offset, long length, List<DeleteFileInfo> deleteFiles) throws Exception {
    // need to prefix with "file:" as that's the format contained in the positional delete file
    Path path = Path.of("file:" + table.getLocation() + "/data/" + relativePath);
    long fileSize = fs.getFileAttributes(path).size();
    if (length == -1) {
      length = fileSize;
    }
    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = getPartitionInfoForDataFile(relativePath);
    return r(
        st(path.toString(), 0L, fileSize, fileSize),
        createSplitInformation(path.toString(), offset, length, fileSize, 0, partitionInfo),
        EXTENDED_PROPS.toByteArray(),
        deleteFiles.stream()
            .map(info -> st(
                table.getLocation() + "/data/" + info.getPath(),
                info.getContent().id(),
                info.getRecordCount(),
                info.getEqualityIds()))
            .collect(Collectors.toList()));
  }

  private RecordSet outputRecordSet(Iterator<Integer> expectedProductIds) {
    List<RecordSet.Record> rows = new ArrayList<>();
    while (expectedProductIds.hasNext()) {
      int productId = expectedProductIds.next();
      String category;
      if ((productId >= 0 && productId < 200) || (productId >= 400 && productId < 600)) {
        category = "widget";
      } else if ((productId >= 200 && productId < 400) || (productId >= 800 && productId < 1000)) {
        category = "gizmo";
      } else {
        category = "gadget";
      }
      String color = COLORS.get(productId % COLORS.size());

      rows.add(r(productId, category, color));
    }
    return rs(OUTPUT_SCHEMA,
        rows.toArray(new RecordSet.Record[0]));
  }



  private void validate(RecordSet input, RecordSet output) throws Exception {
    validate(input, output, COLUMNS);
  }

  private void validate(RecordSet input, RecordSet output, List<SchemaPath> projectedColumns) throws Exception {
    TableFunctionPOP pop = getPop(table, IcebergTestTables.PRODUCTS_SCHEMA, projectedColumns, PARTITION_COLUMNS,
        EXTENDED_PROPS);
    try (AutoCloseable closeable = with(ExecConstants.PARQUET_READER_VECTORIZE, false)) {
      validateSingle(pop, TableFunctionOperator.class, input, output, BATCH_SIZE);
    }
  }

  private TableFunctionPOP getPop(
      IcebergTestTables.Table table,
      BatchSchema fullSchema,
      List<SchemaPath> columns,
      List<String> partitionColumns,
      ByteString extendedProps) {
    BatchSchema projectedSchema = fullSchema.maskAndReorder(columns);
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.DATA_FILE_SCAN,
            false,
            new TableFunctionContext(
                getFileConfig(table),
                fullSchema,
                projectedSchema,
                ImmutableList.of(Arrays.asList(table.getTableName().split("\\."))),
                null,
                pluginId,
                null,
                columns,
                partitionColumns,
                null,
                extendedProps,
                false,
                false,
                false,
                null)));
  }

  private PartitionProtobuf.NormalizedPartitionInfo getPartitionInfoForDataFile(String relativePath) {
    if (relativePath.startsWith("widget")) {
      return PARTITION_INFO_WIDGET;
    } else if (relativePath.startsWith("gizmo")) {
      return PARTITION_INFO_GIZMO;
    } else {
      return PARTITION_INFO_GADGET;
    }
  }

  private FileConfig getFileConfig(IcebergTestTables.Table table) {
    FileConfig config = new FileConfig();
    config.setLocation(table.getLocation());
    config.setType(FileType.ICEBERG);
    return config;
  }

  private static ByteString getExtendedProperties() {
    IcebergProtobuf.IcebergDatasetXAttr.Builder builder = IcebergProtobuf.IcebergDatasetXAttr.newBuilder();
    for (int i = 0; i < IcebergTestTables.PRODUCTS_SCHEMA.getFields().size(); i++) {
      builder.addColumnIds(IcebergProtobuf.IcebergSchemaField.newBuilder()
          .setSchemaPath(IcebergTestTables.PRODUCTS_SCHEMA.getFields().get(i).getName())
          .setId(i + 1));
    }
    return toProtostuff(builder.build()::writeTo);
  }

  private static ByteString toProtostuff(BytesOutput out) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      out.writeTo(output);
      return ByteStringUtil.wrap(output.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static byte[] createSplitInformation(String path, long offset, long length, long fileSize, long mtime,
      PartitionProtobuf.NormalizedPartitionInfo partitionInfo) throws Exception {
    ParquetProtobuf.ParquetBlockBasedSplitXAttr splitExtended = ParquetProtobuf.ParquetBlockBasedSplitXAttr.newBuilder()
        .setPath(path)
        .setStart(offset)
        .setLength(length)
        .setFileLength(fileSize)
        .setLastModificationTime(mtime)
        .build();

    PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
        .setPartitionId(partitionInfo.getId())
        .setExtendedProperty(splitExtended.toByteString());

    return IcebergSerDe.serializeToByteArray(new SplitAndPartitionInfo(partitionInfo, splitInfo.build()));
  }
}
