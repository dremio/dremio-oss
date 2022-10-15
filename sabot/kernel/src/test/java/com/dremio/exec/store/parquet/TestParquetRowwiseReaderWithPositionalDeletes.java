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
import java.util.stream.Stream;

import org.junit.Test;

import com.dremio.exec.store.iceberg.deletes.MergingPositionalDeleteIterator;
import com.dremio.exec.store.iceberg.deletes.PositionalDeleteFilter;
import com.dremio.exec.store.iceberg.deletes.PositionalDeleteIterator;
import com.google.common.collect.ImmutableList;

public class TestParquetRowwiseReaderWithPositionalDeletes extends BaseTestUnifiedParquetReaderWithPositionalDeletes {

  private static final ParquetReaderOptions ROWWISE_READER_OPTIONS = ParquetReaderOptions.builder().build();

  @Test
  public void testFirstRowGroupDeleted() throws Exception {
    // [ 0 .. 584 ]
    Iterator<Long> iterator = Stream.iterate(0L, i -> i + 1)
        .limit(1000)
        .filter(i -> i < 552)
        .iterator();
    PositionalDeleteFilter positionalDeleteFilter = createPositionalDeleteFilter(iterator, 2);

    readAndValidateOrderIdConditionAndRowCount(
        positionalDeleteFilter,
        ROWWISE_READER_OPTIONS,
        orderId -> orderId >= 8552,
        ">= 8552",
        448);
  }

  @Test
  public void testLastRowGroupDeleted() throws Exception {
    // [ 585 .. 999 ]
    Iterator<Long> iterator = Stream.iterate(0L, i -> i + 1)
        .limit(1000)
        .filter(i -> i >= 552)
        .iterator();
    PositionalDeleteFilter positionalDeleteFilter = createPositionalDeleteFilter(iterator, 2);

    readAndValidateOrderIdConditionAndRowCount(
        positionalDeleteFilter,
        ROWWISE_READER_OPTIONS,
        orderId -> orderId < 8552,
        "< 8552",
        552);
  }

  @Test
  public void testContiguousDeleteRangeSpanningRowGroups() throws Exception {
    // [ 500 .. 599 ]
    Iterator<Long> iterator = Stream.iterate(0L, i -> i + 1)
        .limit(1000)
        .filter(i -> i >= 500 && i <= 599)
        .iterator();
    PositionalDeleteFilter positionalDeleteFilter = createPositionalDeleteFilter(iterator, 2);

    readAndValidateOrderIdConditionAndRowCount(
        positionalDeleteFilter,
        ROWWISE_READER_OPTIONS,
        orderId -> orderId < 8500 || orderId > 8599,
        "not between (8500, 8599)",
        900);
  }

  @Test
  public void testWithMergedDeleteFiles() throws Exception {

    PositionalDeleteIterator source0 = createDeleteIteratorFromFile(MULTI_ROWGROUP_DELETE_FILE_0);
    PositionalDeleteIterator source1 = createDeleteIteratorFromFile(MULTI_ROWGROUP_DELETE_FILE_1);
    PositionalDeleteIterator iterator = MergingPositionalDeleteIterator.merge(ImmutableList.of(source0, source1));
    PositionalDeleteFilter positionalDeleteFilter = new PositionalDeleteFilter(() -> iterator, 2, context.getStats());
    testCloseables.add(iterator);

    readAndValidateOrderIdConditionAndRowCount(
        positionalDeleteFilter,
        ROWWISE_READER_OPTIONS,
        orderId -> orderId % 10 >= 3 && orderId % 100 != 5,
        "orderId % 10 >= 3 && orderId % 100 != 5",
        690);
  }

  @Test
  public void testWithNoProjectedColumns() throws Exception {
    // [ 1, 3, 5, 7, 9, ... 999 ]
    Iterator<Long> iterator = Stream.iterate(0L, i -> i + 1)
        .limit(1000)
        .filter(i -> i % 2 == 1)
        .iterator();

    RecordBatchValidator validator = (rowGroupIndex, outputRowIndex, records, mutator) -> {};

    int recordCount = readAndValidate(
        DATA_FILE_2,
        new ParquetFilters(null, createPositionalDeleteFilter(iterator, 2), null),
        ImmutableList.of(),
        ROWWISE_READER_OPTIONS,
        validator);

    assertThat(recordCount).isEqualTo(500);
  }
}
