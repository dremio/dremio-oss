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

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.iceberg.deletes.EqualityDeleteFileReader;
import com.dremio.exec.store.iceberg.deletes.EqualityDeleteFilter;
import com.dremio.exec.store.iceberg.deletes.LazyEqualityDeleteTableSupplier;
import com.dremio.exec.store.iceberg.deletes.ParquetRowLevelDeleteFileReaderFactory;
import com.dremio.exec.store.iceberg.deletes.RowLevelDeleteFilterFactory.DeleteFileInfo;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.FileContent;
import org.assertj.core.api.Condition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUnifiedParquetReaderWithEqualityDeletes extends BaseTestUnifiedParquetReader {

  private static final ParquetReaderOptions ROWWISE_READER_OPTIONS =
      ParquetReaderOptions.builder().build();

  // data file has 2 row groups of 100 rows each, and contains product_id 0..199
  private static final String DATA_WIDGET_00 = "widget/widget-00.parquet";

  // delete rows where product_id >= 0 && product_id < 30
  private static final DeleteFileInfo EQ_DELETE_WIDGET_00 =
      new DeleteFileInfo(
          "widget/eqdelete-widget-00.parquet",
          FileContent.EQUALITY_DELETES,
          30,
          ImmutableList.of(1));

  protected static IcebergTestTables.Table table;

  @BeforeClass
  public static void setupTestData() {
    table = IcebergTestTables.PRODUCTS_WITH_EQ_DELETES.get();
  }

  @AfterClass
  public static void cleanupTestData() throws Exception {
    table.close();
  }

  @Test
  public void testWithOnlyEqualityDeleteFilter() throws Exception {
    readAndValidateProductIdConditionAndRowCount(
        new ParquetFilters(null, null, createEqualityDeleteFilter(EQ_DELETE_WIDGET_00)),
        ROWWISE_READER_OPTIONS,
        id -> id >= 30 && id < 200,
        "between [ 30, 199 ]",
        170);
  }

  @Test
  public void testWithPushdownFilter() throws Exception {
    // create a pushdown filter: product_id < 100
    SchemaPath productIdCol = SchemaPath.getSimplePath("product_id");
    LogicalExpression expr =
        FunctionCallFactory.createExpression(
            "less_than", new FieldReference(productIdCol), ValueExpressions.getInt(100));
    ParquetFilterCondition condition =
        new ParquetFilterCondition(productIdCol, new ParquetFilterIface() {}, expr, 0);

    readAndValidateProductIdConditionAndRowCount(
        new ParquetFilters(
            ImmutableList.of(condition), null, createEqualityDeleteFilter(EQ_DELETE_WIDGET_00)),
        ROWWISE_READER_OPTIONS,
        id -> id >= 30 && id < 100,
        "between [ 30, 99 ]",
        70);
  }

  private void readAndValidateProductIdConditionAndRowCount(
      ParquetFilters filters,
      ParquetReaderOptions parquetReaderOptions,
      Predicate<Integer> productIdPredicate,
      String predicateDescription,
      int expectedRecordCount)
      throws Exception {

    RecordBatchValidator validator =
        (rowGroupIndex, outputRowIndex, records, mutator) -> {
          IntVector productIdVector = (IntVector) mutator.getVector("product_id");
          for (int i = 0; i < records; i++) {

            int productId = productIdVector.get(i);
            assertThat(productId)
                .as("output row index %d", outputRowIndex + i)
                .is(new Condition<>(productIdPredicate, predicateDescription));
          }
        };

    int recordCount =
        readAndValidate(
            getAbsolutePath(DATA_WIDGET_00),
            filters,
            ImmutableList.of("product_id", "color"),
            parquetReaderOptions,
            validator);

    assertThat(recordCount).isEqualTo(expectedRecordCount);
    assertThat(filters.getEqualityDeleteFilter().refCount()).isEqualTo(0);
  }

  private EqualityDeleteFilter createEqualityDeleteFilter(DeleteFileInfo deleteFile)
      throws Exception {
    ParquetRowLevelDeleteFileReaderFactory factory =
        new ParquetRowLevelDeleteFileReaderFactory(
            getInputStreamProviderFactory(),
            getParquetReaderFactory(),
            fs,
            null,
            IcebergTestTables.PRODUCTS_SCHEMA);
    EqualityDeleteFileReader reader =
        factory.createEqualityDeleteFileReader(
            context,
            getAbsolutePath(deleteFile.getPath()),
            deleteFile.getRecordCount(),
            deleteFile.getEqualityIds(),
            getIcebergSchemaFields(IcebergTestTables.PRODUCTS_SCHEMA),
            null);
    reader.setup();
    // create with refcount 2 since there's 2 rowgroups in the data file
    return new EqualityDeleteFilter(
        getTestAllocator(), new LazyEqualityDeleteTableSupplier(ImmutableList.of(reader)), 2, null);
  }

  private Path getAbsolutePath(String relativePath) {
    return Path.of("file:" + table.getLocation() + "/data/" + relativePath);
  }

  private List<IcebergProtobuf.IcebergSchemaField> getIcebergSchemaFields(BatchSchema schema) {
    List<IcebergProtobuf.IcebergSchemaField> fields = new ArrayList<>();
    for (int i = 0; i < schema.getFields().size(); i++) {
      fields.add(
          IcebergProtobuf.IcebergSchemaField.newBuilder()
              .setSchemaPath(schema.getFields().get(i).getName())
              .setId(i + 1)
              .build());
    }

    return fields;
  }
}
