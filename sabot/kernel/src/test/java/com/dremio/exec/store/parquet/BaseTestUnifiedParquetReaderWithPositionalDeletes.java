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

import java.util.Iterator;
import java.util.function.Predicate;

import org.apache.arrow.vector.IntVector;
import org.assertj.core.api.Condition;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.iceberg.deletes.ParquetRowLevelDeleteFileReaderFactory;
import com.dremio.exec.store.iceberg.deletes.PositionalDeleteFileReader;
import com.dremio.exec.store.iceberg.deletes.PositionalDeleteFilter;
import com.dremio.exec.store.iceberg.deletes.PositionalDeleteIterator;
import com.dremio.io.file.Path;
import com.google.common.collect.ImmutableList;

public class BaseTestUnifiedParquetReaderWithPositionalDeletes extends BaseTestUnifiedParquetReader {

  // Test file with 900 deletes spread equally across 3 different data files.  Deleted positions in each data file are
  // [ 0, 1, 2, 10, 11, 12, 20, 21, 22, ..., 990, 991, 992 ]
  protected static final Path MULTI_ROWGROUP_DELETE_FILE_0 = Path.of(
      "/tmp/iceberg-test-tables/v2/multi_rowgroup_orders_with_deletes/data/2021/delete-2021-00.parquet");
  // Test file with 30 deletes spread equally across 3 different data files.  Deleted positions in each data file are
  // [ 5, 105, ..., 905 ]
  protected static final Path MULTI_ROWGROUP_DELETE_FILE_1 = Path.of(
      "/tmp/iceberg-test-tables/v2/multi_rowgroup_orders_with_deletes/data/2021/delete-2021-01.parquet");
  // Test file with 1000 rows split across two row groups.  First row group has 552 rows, second has 448.
  // order_id values range from 8000 - 8999
  protected static final Path DATA_FILE_2 = Path.of(
      "/tmp/iceberg-test-tables/v2/multi_rowgroup_orders_with_deletes/data/2021/2021-02.parquet");

  protected static IcebergTestTables.Table table;

  @BeforeClass
  public static void setupTestData() {
    table = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
  }

  @AfterClass
  public static void cleanupTestData() throws Exception {
    table.close();
  }

  protected void readAndValidateOrderIdConditionAndRowCount(
      PositionalDeleteFilter positionalDeleteFilter,
      ParquetReaderOptions parquetReaderOptions,
      Predicate<Integer> orderIdPredicate,
      String predicateDescription,
      int expectedRecordCount) throws Exception {
    readAndValidateOrderIdConditionAndRowCount(
        new ParquetFilters(null, positionalDeleteFilter, null),
        parquetReaderOptions,
        orderIdPredicate,
        predicateDescription,
        expectedRecordCount);
  }

  protected void readAndValidateOrderIdConditionAndRowCount(
      ParquetFilters filters,
      ParquetReaderOptions parquetReaderOptions,
      Predicate<Integer> orderIdPredicate,
      String predicateDescription,
      int expectedRecordCount) throws Exception {

    RecordBatchValidator validator = (rowGroupIndex, outputRowIndex, records, mutator) -> {
      IntVector orderIdVector = (IntVector) mutator.getVector("order_id");
      for (int i = 0; i < records; i++) {

        int orderId = orderIdVector.get(i);
        assertThat(orderId)
            .as("output row index %d", outputRowIndex + i)
            .is(new Condition<>(orderIdPredicate, predicateDescription));
      }
    };

    int recordCount = readAndValidate(
        DATA_FILE_2,
        filters,
        ImmutableList.of("order_id"),
        parquetReaderOptions,
        validator);

    assertThat(recordCount).isEqualTo(expectedRecordCount);
    assertThat(filters.getPositionalDeleteFilter().refCount()).isEqualTo(0);
  }

  protected PositionalDeleteFilter createPositionalDeleteFilter(Iterator<Long> iterator, int initialRefCount) {
    PositionalDeleteIterator positionalDeleteIterator = new PositionalDeleteIterator() {
      @Override
      public void close() throws Exception {
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Long next() {
        return iterator.next();
      }
    };

    return new PositionalDeleteFilter(() -> positionalDeleteIterator, initialRefCount, context.getStats());
  }

  protected PositionalDeleteIterator createDeleteIteratorFromFile(Path path) throws Exception {
    ParquetRowLevelDeleteFileReaderFactory factory = new ParquetRowLevelDeleteFileReaderFactory(
        getInputStreamProviderFactory(), getParquetReaderFactory(), fs, null, IcebergTestTables.V2_ORDERS_SCHEMA);
    PositionalDeleteFileReader creator = factory.createPositionalDeleteFileReader(context, path,
        ImmutableList.of(DATA_FILE_2.toString()));
    creator.setup();
    return creator.createIteratorForDataFile(DATA_FILE_2.toString());
  }
}
