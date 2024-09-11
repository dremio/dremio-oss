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
package com.dremio.exec.store.dfs.copyinto;

import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rs;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.iceberg.deletes.RowLevelDeleteFilterFactory;
import com.dremio.exec.store.parquet.BaseTestParquetScanTableFunction;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.RecordSet.RsRecord;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCopyIntoSkipParquetCoercionReader extends BaseTestParquetScanTableFunction {
  private static final int BATCH_SIZE = 670;
  private static final String QUERY_ID = UUID.randomUUID().toString();
  private static final String TEST_USER = "test-user";
  private static final String SOURCE = "/source";
  private static final String DATA_2021_00 = "2021/2021-00.parquet";
  private static final String INCOMPATIBLE_FILE =
      "/00000-1-a9e8d979-a183-40c5-af3d-a338ab62be8b-00000.parquet";
  private static final List<SchemaPath> PROJECTED_ICEBERG_COLUMNS =
      ImmutableList.of(
          SchemaPath.getSimplePath("order_id"), SchemaPath.getSimplePath("order_year"));

  private static final List<SchemaPath> ALL_PROJECTED_COLUMNS =
      Stream.concat(
              PROJECTED_ICEBERG_COLUMNS.stream(),
              Stream.of(SchemaPath.getSimplePath(ColumnUtils.COPY_HISTORY_COLUMN_NAME)))
          .collect(Collectors.toList());

  private static final BatchSchema ICEBERG_SCHEMA =
      IcebergTestTables.V2_ORDERS_SCHEMA.maskAndReorder(PROJECTED_ICEBERG_COLUMNS);
  private static final BatchSchema OUTPUT_SCHEMA =
      ICEBERG_SCHEMA
          .addColumn(
              Field.nullable(ColumnUtils.COPY_HISTORY_COLUMN_NAME, CompleteType.VARCHAR.getType()))
          .maskAndReorder(ALL_PROJECTED_COLUMNS);
  private static final List<String> PARTITION_COLUMNS = ImmutableList.of("order_year");
  private static final ByteString EXTENDED_PROPS = getExtendedProperties(ICEBERG_SCHEMA);
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_2019 =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(
              PartitionProtobuf.PartitionValue.newBuilder()
                  .setColumn("order_year")
                  .setIntValue(2019)
                  .build())
          .build();
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_2020 =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(
              PartitionProtobuf.PartitionValue.newBuilder()
                  .setColumn("order_year")
                  .setIntValue(2020)
                  .build())
          .build();
  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO_2021 =
      PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
          .setId("1")
          .addValues(
              PartitionProtobuf.PartitionValue.newBuilder()
                  .setColumn("order_year")
                  .setIntValue(2021)
                  .build())
          .build();

  private static IcebergTestTables.Table table;
  private static IcebergTestTables.Table incomaptibleTable;

  @BeforeClass
  public static void initTables() {
    table = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
    incomaptibleTable = IcebergTestTables.NATION.get();
  }

  @AfterClass
  public static void closeTables() throws Exception {
    table.close();
    incomaptibleTable.close();
  }

  @Test
  public void testDataFileWithMultipleRowGroupsWithHistory() throws Exception {
    RecordSet input =
        rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA, inputRow(table, DATA_2021_00));
    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1).limit(1000).iterator();
    RecordSet output = outputRecordSet(true, orderIds);
    validate(input, output, true);
  }

  @Test
  public void testDataFileWithMultipleRowGroupsNoHistory() throws Exception {
    RecordSet input =
        rs(SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA, inputRow(table, DATA_2021_00));
    Iterator<Integer> orderIds = Stream.iterate(6000, i -> i + 1).limit(1000).iterator();
    RecordSet output = outputRecordSet(false, orderIds);
    validate(input, output, false);
  }

  @Test
  public void testSkippedFileNoSuccessEvent() throws Exception {
    RecordSet input =
        rs(
            SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA,
            inputRow(incomaptibleTable, INCOMPATIBLE_FILE));
    List<RsRecord> rows = new ArrayList<>();
    rows.add(r(null, 2021, getExpectedSkippedJson())); // only skipped event expected
    RecordSet output = rs(OUTPUT_SCHEMA, rows.toArray(new RsRecord[0]));

    // Fully loaded event is not recorded irrespective of the recordHistory flag
    validate(input, output, true);
    validate(input, output, false);
  }

  private String getExpectedSkippedJson() {
    return FileLoadInfo.Util.getJson(
        new CopyIntoFileLoadInfo.Builder(
                QUERY_ID,
                TEST_USER,
                table.getTableName(),
                SOURCE,
                incomaptibleTable.getLocation() + "/data" + INCOMPATIBLE_FILE,
                new ExtendedFormatOptions(),
                FileType.PARQUET.name(),
                CopyIntoFileLoadInfo.CopyIntoFileState.SKIPPED)
            .setRecordsLoadedCount(0)
            .setRecordsRejectedCount(1)
            .setFileSize(2305)
            .setFirstErrorMessage("Failed to cast the string ALGERIA to int32_t")
            .build());
  }

  private String getExpectedInfoJson() {
    return FileLoadInfo.Util.getJson(
        new CopyIntoFileLoadInfo.Builder(
                QUERY_ID,
                TEST_USER,
                table.getTableName(),
                SOURCE,
                table.getLocation() + "/data/" + DATA_2021_00,
                new ExtendedFormatOptions(),
                FileType.PARQUET.name(),
                CopyIntoFileLoadInfo.CopyIntoFileState.FULLY_LOADED)
            .setRecordsLoadedCount(448)
            .setFileSize(21393)
            .build());
  }

  private void validate(RecordSet input, RecordSet output, boolean recordHistory) throws Exception {
    TableFunctionPOP pop =
        getPopForIceberg(
            table,
            OUTPUT_SCHEMA,
            ALL_PROJECTED_COLUMNS,
            PARTITION_COLUMNS,
            getExtendedProps(recordHistory));
    try (AutoCloseable closeable = with(ExecConstants.PARQUET_READER_VECTORIZE, false)) {
      validateSingle(
          pop,
          TableFunctionOperator.class,
          input,
          new CopyIntoRecordBatchValidator(output),
          BATCH_SIZE);
    }
  }

  private ByteString getExtendedProps(boolean shouldRecordHistory) {
    SimpleQueryContext copyQueryContext =
        new SimpleQueryContext(TEST_USER, QUERY_ID, table.getTableName());
    CopyIntoQueryProperties copyIntoQueryProperties = new CopyIntoQueryProperties();
    copyIntoQueryProperties.setOnErrorOption(CopyIntoQueryProperties.OnErrorOption.SKIP_FILE);
    copyIntoQueryProperties.setStorageLocation(SOURCE);
    if (shouldRecordHistory) {
      copyIntoQueryProperties.addEventHistoryRecordsForState(
          CopyIntoFileLoadInfo.CopyIntoFileState.FULLY_LOADED);
    }

    CopyIntoExtendedProperties copyIntoExtendedProperties = new CopyIntoExtendedProperties();
    copyIntoExtendedProperties.setProperty(
        CopyIntoExtendedProperties.PropertyKey.COPY_INTO_QUERY_PROPERTIES, copyIntoQueryProperties);
    copyIntoExtendedProperties.setProperty(
        CopyIntoExtendedProperties.PropertyKey.QUERY_CONTEXT, copyQueryContext);

    return CopyIntoExtendedProperties.Util.getByteString(copyIntoExtendedProperties);
  }

  private RecordSet outputRecordSet(boolean hasHistory, Iterator<Integer> expectedOrderIds) {
    List<RsRecord> rows = new ArrayList<>();

    if (hasHistory) {
      // Partition column is written at a later stage
      rows.add(r(null, 2021, getExpectedInfoJson()));
    }
    while (expectedOrderIds.hasNext()) {
      int orderId = expectedOrderIds.next();
      int orderYear = orderId < 3000 ? 2019 : orderId < 6000 ? 2020 : 2021;
      rows.add(r(orderId, orderYear, null));
    }
    return rs(OUTPUT_SCHEMA, rows.toArray(new RsRecord[0]));
  }

  private RsRecord inputRow(IcebergTestTables.Table inTable, String relativePath) throws Exception {
    return inputRow(inTable, relativePath, 0, -1);
  }

  private RsRecord inputRow(
      IcebergTestTables.Table inTable, String relativePath, long offset, long length)
      throws Exception {
    String fullPath = inTable.getLocation() + "/data/" + relativePath;
    PartitionProtobuf.NormalizedPartitionInfo partitionInfo =
        getPartitionInfoForDataFile(relativePath);
    List<RowLevelDeleteFilterFactory.DeleteFileInfo> deleteFilesWithFullPaths =
        Collections.emptyList();
    return inputRow(
        fullPath, offset, length, partitionInfo, deleteFilesWithFullPaths, EXTENDED_PROPS);
  }

  private PartitionProtobuf.NormalizedPartitionInfo getPartitionInfoForDataFile(
      String relativePath) {
    if (relativePath.startsWith("2019")) {
      return PARTITION_INFO_2019;
    } else if (relativePath.startsWith("2020")) {
      return PARTITION_INFO_2020;
    } else {
      return PARTITION_INFO_2021;
    }
  }
}
