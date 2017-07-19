/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.sort.external;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.DataRow;
import com.dremio.sabot.Generator;
import com.google.common.base.Preconditions;

/**
 * Generates 2 integer fields: ID and VALUE that are randomly set in a way it makes it easy to assert
 * if a given batch is correctly ordered
 */
class CustomGenerator implements Generator {

  final static Field ID = CompleteType.INT.toField("ID");
  final static Field VALUE = CompleteType.VARCHAR.toField("FIELD");

  private final List<Integer> rowIds;
  private final List<String> values;

  private final VectorContainer container;
  private final NullableIntVector.Mutator idMutator;
  private final NullableVarCharVector.Mutator valueMutator;

  private int position;

  CustomGenerator(int numRows, BufferAllocator allocator) {
    Preconditions.checkState(numRows > 0);
    values = listOfStrings(numRows);
    rowIds = randomListOfInts(numRows);

    BatchSchema schema = BatchSchema.newBuilder()
            .addField(ID)
            .addField(VALUE)
            .build();

    container = VectorContainer.create(allocator, schema);
    idMutator = (NullableIntVector.Mutator) container.addOrGet(ID).getMutator();
    valueMutator = (NullableVarCharVector.Mutator) container.addOrGet(VALUE).getMutator();
  }

  @Override
  public VectorAccessible getOutput() {
    return container;
  }

  BatchSchema getSchema() {
    return container.getSchema();
  }

  SortValidator getValidator(int capacity) {
    return new SortValidator(capacity);
  }

  @Override
  public int next(int records) {
    if (position == values.size()) {
      return 0; // no more data available
    }
    int returned = Math.min(records, values.size() - position);

    container.allocateNew();
    for (int i = 0; i < returned; i++) {
      int rowId = rowIds.get(position + i);
      idMutator.set(i, rowId);
      byte[] valueBytes = values.get(rowId).getBytes();
      valueMutator.setSafe(i, valueBytes, 0, valueBytes.length);
    }
    container.setAllCount(returned);
    position += returned;

    return returned;
  }

  public Fixtures.Table getExpectedSortedTable() {
    final DataRow[] rows = new DataRow[rowIds.size()];
    for (int i = 0; i < rowIds.size(); i++) {
      rows[i] = tr(i, values.get(i));
    }
    return t(th("ID", "FIELD"), rows);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(container);
  }

  private static List<String> listOfStrings(int size) {
    List<String> strings = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      strings.add(String.format("%d", System.currentTimeMillis()));
    }
    return strings;
  }

  /**
   * @return shuffled list of integers [0..size)
   */
  private static List<Integer> randomListOfInts(int size) {
    Integer[] ids = new Integer[size];
    for (int i = 0; i < size; i++) {
      ids[i] = i;
    }

    List<Integer> rndList = Arrays.asList(ids);
    Collections.shuffle(rndList);
    return rndList;
  }

  class SortValidator {
    final List<Integer> sortedRowIds;

    SortValidator(int capacity) {
      sortedRowIds = rowIds.subList(0, capacity);
      Collections.sort(sortedRowIds);
    }

    void assertIsSorted(VectorContainer container, int startIndex) {
      final NullableIntVector.Accessor idAccessor =
              (NullableIntVector.Accessor) container.addOrGet(ID).getAccessor();
      final NullableVarCharVector.Accessor valueAccessor =
              (NullableVarCharVector.Accessor) container.addOrGet(VALUE).getAccessor();

      int recordCount = container.getRecordCount();
      int index = startIndex;
      for (int i = 0; i < recordCount; i++, index++) {
        int rowId = sortedRowIds.get(index);
        String value = values.get(rowId);
        assertEquals("non matching ID at row " + index, rowId, idAccessor.get(i));
        assertEquals("non matching VALUE at row " + index, value, valueAccessor.getObject(i).toString());
      }
    }

  }
}
