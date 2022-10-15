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
package com.dremio.exec.store.iceberg.deletes;

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;
import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rb;
import static com.dremio.sabot.RecordSet.rs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.sabot.Generator;
import com.dremio.sabot.RecordSet;
import com.google.common.collect.ImmutableList;

public class TestEqualityDeleteFilter extends BaseTestEqualityDeleteFilter {

  private static final BatchSchema FULL_SCHEMA = BatchSchema.newBuilder()
      .addField(Field.nullable("col1", Types.MinorType.INT.getType()))
      .addField(Field.nullable("col2", Types.MinorType.VARCHAR.getType()))
      .addField(Field.nullable("col3", Types.MinorType.VARCHAR.getType()))
      .build();
  private static final BatchSchema EQ_SCHEMA_COL1 = FULL_SCHEMA.maskAndReorder(
      ImmutableList.of(SchemaPath.getSimplePath("col1")));
  private static final BatchSchema EQ_SCHEMA_COL2 = FULL_SCHEMA.maskAndReorder(
      ImmutableList.of(SchemaPath.getSimplePath("col2")));
  private static final BatchSchema EQ_SCHEMA_COL1_COL2 = FULL_SCHEMA.maskAndReorder(
      ImmutableList.of(SchemaPath.getSimplePath("col1"), SchemaPath.getSimplePath("col2")));

  private static final RecordSet TABLE_1 = rs(EQ_SCHEMA_COL1,
      r(10), r(2), r(8));
  private static final RecordSet TABLE_2 = rs(EQ_SCHEMA_COL1,
      r(2), r(3));
  private static final RecordSet TABLE_3 = rs(EQ_SCHEMA_COL2,
      r("red"), r("white"));
  private static final RecordSet TABLE_4 = rs(EQ_SCHEMA_COL1_COL2,
      r(null, "yellow"), r(null, null));

  private static final RecordSet PROBE_TABLE = rs(FULL_SCHEMA,
      rb(
          r(1, "red", "chair"),
          r(8, "blue", "table"),
          r(3, "yellow", "lamp")),
      rb(
          r(2, "green", "chair"),
          r(8, "red", "desk"),
          r(null, "yellow", "lamp")),
      rb(
          r(10, "brown", "carpet"),
          r(5, null, null),
          r(null, null, null)));

  @Test
  public void testSingleTable() throws Exception {
    List<Integer> expectedValidity = ImmutableList.of(1, 0, 1, 0, 0, 1, 0, 1, 1);
    validateFilter(ImmutableList.of(TABLE_1), PROBE_TABLE, expectedValidity);
  }

  @Test
  public void testMultipleTablesWithSameEqualityFields() throws Exception {
    List<Integer> expectedValidity = ImmutableList.of(1, 0, 0, 0, 0, 1, 0, 1, 1);
    validateFilter(ImmutableList.of(TABLE_1, TABLE_2), PROBE_TABLE, expectedValidity);
  }

  @Test
  public void testMultipleTablesWithDifferentEqualityFields() throws Exception {
    List<Integer> expectedValidity = ImmutableList.of(0, 0, 0, 0, 0, 0, 0, 1, 0);
    validateFilter(ImmutableList.of(TABLE_1, TABLE_2, TABLE_3, TABLE_4), PROBE_TABLE, expectedValidity);
  }

  @Test
  public void testTablesLoadedLazily() throws Exception {
    try (TestOutputMutator mutator = new TestOutputMutator(getTestAllocator());
        ArrowBuf validityBuf = getTestAllocator().buffer(1)) {
      LazyEqualityDeleteTableSupplier mockSupplier = mock(LazyEqualityDeleteTableSupplier.class);
      when(mockSupplier.get()).thenReturn(ImmutableList.of());

      EqualityDeleteFilter filter = new EqualityDeleteFilter(getTestAllocator(), mockSupplier, 1, null);

      // supplier should not be called until setup is called
      verify(mockSupplier, never()).get();
      filter.setup(mutator, validityBuf);
      verify(mockSupplier, times(1)).get();

      filter.release();
      verify(mockSupplier, times(1)).close();
    }
  }

  private void validateFilter(List<RecordSet> buildRs, RecordSet probeRs, List<Integer> expectedValidity)
      throws Exception {
    List<Integer> actualValidity = new ArrayList<>();

    int batchSize = probeRs.getMaxBatchSize();
    try (EqualityDeleteFilter filter = createFilter(buildRs);
        TestOutputMutator mutator = new TestOutputMutator(getTestAllocator());
        Generator generator = probeRs.toGenerator(getTestAllocator());
        ArrowBuf validityBuf = getTestAllocator().buffer(BitVectorHelper.getValidityBufferSize(batchSize))) {
      VectorAccessible accessible = generator.getOutput();
      for (Field field : FULL_SCHEMA.getFields()) {
        ValueVector vv = getVectorFromSchemaPath(accessible, field.getName());
        mutator.addField(vv);
      }

      filter.setup(mutator, validityBuf);

      int records;
      while ((records = generator.next(batchSize)) > 0) {
        validityBuf.setOne(0, (int) validityBuf.capacity());
        filter.filter(records);
        for (int i = 0; i < records; i++) {
          actualValidity.add(BitVectorHelper.get(validityBuf, i));
        }
      }
    }

    assertThat(actualValidity).isEqualTo(expectedValidity);
  }

  private EqualityDeleteFilter createFilter(List<RecordSet> buildRs) throws Exception {
    List<EqualityDeleteFileReader> readers = new ArrayList<>();
    for (RecordSet rs : buildRs) {
      EqualityDeleteHashTable table = buildTable(rs, null);
      EqualityDeleteFileReader mockReader = mock(EqualityDeleteFileReader.class);
      when(mockReader.getEqualityFields()).thenReturn(table.getEqualityFields());
      when(mockReader.buildHashTable()).thenReturn(table);
      readers.add(mockReader);
    }

    return new EqualityDeleteFilter(getTestAllocator(), new LazyEqualityDeleteTableSupplier(readers), 1, null);
  }
}
