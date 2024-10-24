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

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.iceberg.deletes.RowLevelDeleteFilterFactory.DeleteFileInfo;
import com.dremio.sabot.RecordBatchValidatorDefaultImpl;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.RecordSet.RsRecord;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.FileContent;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestParquetScanTableFunctionWithEqualityDeletes
    extends BaseTestParquetScanTableFunction {

  private static final List<SchemaPath> COLUMNS =
      ImmutableList.of(
          SchemaPath.getSimplePath("product_id"),
          SchemaPath.getSimplePath("category"),
          SchemaPath.getSimplePath("color"));
  private static final BatchSchema OUTPUT_SCHEMA =
      IcebergTestTables.PRODUCTS_SCHEMA.maskAndReorder(COLUMNS);
  private static final List<String> PARTITION_COLUMNS = ImmutableList.of("category");
  private static final ByteString EXTENDED_PROPS =
      getExtendedProperties(IcebergTestTables.PRODUCTS_SCHEMA);
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_WIDGET =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(
              PartitionProtobuf.PartitionValue.newBuilder()
                  .setColumn("category")
                  .setStringValue("widget")
                  .build())
          .build();
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_GADGET =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(
              PartitionProtobuf.PartitionValue.newBuilder()
                  .setColumn("category")
                  .setStringValue("gadget")
                  .build())
          .build();
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_GIZMO =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(
              PartitionProtobuf.PartitionValue.newBuilder()
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
  private static final DeleteFileInfo EQ_DELETE_WIDGET_00 =
      new DeleteFileInfo(
          "widget/eqdelete-widget-00.parquet",
          FileContent.EQUALITY_DELETES,
          30,
          ImmutableList.of(1));
  // delete rows where color == "green" (product_id >= 0 && product_id < 200 && product_id % 10 ==
  // 5)
  private static final DeleteFileInfo EQ_DELETE_WIDGET_01 =
      new DeleteFileInfo(
          "widget/eqdelete-widget-01.parquet",
          FileContent.EQUALITY_DELETES,
          20,
          ImmutableList.of(4));
  // delete rows where (product_id >= 100 && product_id < 200) || (product_id >= 500 && product_id <
  // 600)
  private static final DeleteFileInfo EQ_DELETE_WIDGET_02 =
      new DeleteFileInfo(
          "widget/eqdelete-widget-02.parquet",
          FileContent.EQUALITY_DELETES,
          200,
          ImmutableList.of(1));
  // delete rows (positional) where product_id >= 50 && product_id < 53
  private static final DeleteFileInfo DELETE_WIDGET_00 =
      new DeleteFileInfo("widget/delete-widget-00.parquet", FileContent.POSITION_DELETES, 0, null);

  private static final Predicate<Integer> FILTER_EQ_DELETE_WIDGET_00 = i -> !(i >= 0 && i < 30);
  private static final Predicate<Integer> FILTER_EQ_DELETE_WIDGET_01 =
      i -> !(i >= 0 && i < 200 && i % 10 == 5);
  private static final Predicate<Integer> FILTER_EQ_DELETE_WIDGET_02 =
      i -> !((i >= 100 && i < 200) || (i >= 500 && i < 600));
  private static final Predicate<Integer> FILTER_DELETE_WIDGET_00 = i -> !(i >= 50 && i < 53);

  private static final List<String> COLORS =
      ImmutableList.of(
          "black", "white", "red", "orange", "yellow", "green", "blue", "purple", "brown", "gray");

  private static final int BATCH_SIZE = 67;

  private static IcebergTestTables.Table table;

  @BeforeClass
  public static void initTables() {
    table = IcebergTestTables.PRODUCTS_WITH_EQ_DELETES.get();
  }

  @AfterClass
  public static void closeTables() throws Exception {
    AutoCloseables.close(table);
  }

  @Test
  public void testDataFileWithMultipleRowGroups() throws Exception {
    RecordSet input =
        rs(
            SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
            inputRow(
                DATA_WIDGET_00,
                ImmutableList.of(EQ_DELETE_WIDGET_00, EQ_DELETE_WIDGET_01, EQ_DELETE_WIDGET_02)));

    Iterator<Integer> productIds =
        Stream.iterate(0, i -> i + 1)
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
    RecordSet input =
        rs(
            SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
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
    RecordSet input =
        rs(
            SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
            // this split should expand to only the 1st rowgroup which ends around offset 2365
            inputRow(
                DATA_WIDGET_00,
                0,
                2000,
                ImmutableList.of(EQ_DELETE_WIDGET_00, EQ_DELETE_WIDGET_01, EQ_DELETE_WIDGET_02)),
            // this should expand to nothing
            inputRow(
                DATA_WIDGET_00,
                4000,
                2000,
                ImmutableList.of(EQ_DELETE_WIDGET_00, EQ_DELETE_WIDGET_01, EQ_DELETE_WIDGET_02)));

    Iterator<Integer> productIds =
        Stream.iterate(0, i -> i + 1)
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
    RecordSet input =
        rs(
            SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
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
    RecordSet input =
        rs(
            SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
            inputRow(
                DATA_WIDGET_00,
                ImmutableList.of(
                    EQ_DELETE_WIDGET_00,
                    EQ_DELETE_WIDGET_01,
                    EQ_DELETE_WIDGET_02,
                    DELETE_WIDGET_00)));

    Iterator<Integer> productIds =
        Stream.iterate(0, i -> i + 1)
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
    RecordSet input =
        rs(
            SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
            inputRow(DATA_WIDGET_00, ImmutableList.of(EQ_DELETE_WIDGET_01)));

    RsRecord[] outputRecords =
        Stream.iterate(0, i -> i + 1)
            .limit(200)
            .filter(FILTER_EQ_DELETE_WIDGET_01)
            .map(RecordSet::r)
            .toArray(RsRecord[]::new);
    RecordSet output = rs(IcebergTestTables.PRODUCTS_SCHEMA.maskAndReorder(columns), outputRecords);

    validate(input, output, columns);
  }

  private RsRecord inputRow(String relativePath) throws Exception {
    return inputRow(relativePath, 0, -1, ImmutableList.of());
  }

  private RsRecord inputRow(String relativePath, List<DeleteFileInfo> deleteFiles)
      throws Exception {
    return inputRow(relativePath, 0, -1, deleteFiles);
  }

  private RsRecord inputRow(
      String relativePath, long offset, long length, List<DeleteFileInfo> deleteFiles)
      throws Exception {
    String fullPath = "file:" + table.getLocation() + "/data/" + relativePath;
    PartitionProtobuf.NormalizedPartitionInfo partitionInfo =
        getPartitionInfoForDataFile(relativePath);
    List<DeleteFileInfo> deleteFilesWithFullPaths =
        deleteFiles.stream()
            .map(
                info ->
                    new DeleteFileInfo(
                        table.getLocation() + "/data/" + info.getPath(),
                        info.getContent(),
                        info.getRecordCount(),
                        info.getEqualityIds()))
            .collect(Collectors.toList());
    return inputRow(
        fullPath, offset, length, partitionInfo, deleteFilesWithFullPaths, EXTENDED_PROPS);
  }

  private RecordSet outputRecordSet(Iterator<Integer> expectedProductIds) {
    List<RsRecord> rows = new ArrayList<>();
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
    return rs(OUTPUT_SCHEMA, rows.toArray(new RsRecord[0]));
  }

  private void validate(RecordSet input, RecordSet output) throws Exception {
    validate(input, output, COLUMNS);
  }

  private void validate(RecordSet input, RecordSet output, List<SchemaPath> projectedColumns)
      throws Exception {
    TableFunctionPOP pop =
        getPopForIceberg(
            table,
            IcebergTestTables.PRODUCTS_SCHEMA,
            projectedColumns,
            PARTITION_COLUMNS,
            EXTENDED_PROPS);
    try (AutoCloseable closeable = with(ExecConstants.PARQUET_READER_VECTORIZE, false)) {
      validateSingle(
          pop,
          TableFunctionOperator.class,
          input,
          new RecordBatchValidatorDefaultImpl(output),
          BATCH_SIZE);
    }
  }

  private PartitionProtobuf.NormalizedPartitionInfo getPartitionInfoForDataFile(
      String relativePath) {
    if (relativePath.startsWith("widget")) {
      return PARTITION_INFO_WIDGET;
    } else if (relativePath.startsWith("gizmo")) {
      return PARTITION_INFO_GIZMO;
    } else {
      return PARTITION_INFO_GADGET;
    }
  }
}
