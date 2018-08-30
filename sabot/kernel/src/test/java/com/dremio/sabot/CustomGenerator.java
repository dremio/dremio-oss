/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.Types;
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
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringArrayList;

/**
 * Generates 2 integer fields: ID and VALUE that are randomly set in a way it makes it easy to assert
 * if a given batch is correctly ordered
 */
public class CustomGenerator implements Generator {

  final public static Field ID = CompleteType.INT.toField("ID");
  final public static Field VALUE = CompleteType.VARCHAR.toField("FIELD");
  final static Field MYLIST = CompleteType.BIGINT.asList().toField("MYLIST");

  private static final int INNER_LIST_SIZE = 3;

  private final List<Integer> rowIds;
  private final List<String> values;
  private final List<JsonStringArrayList<Long>> listValues;

  private final VectorContainer container;
  private final IntVector id;
  private final VarCharVector value;
  private final ListVector list;

  private int position;

  public CustomGenerator(int numRows, BufferAllocator allocator) {
    Preconditions.checkState(numRows > 0);
    values = listOfStrings(numRows);
    rowIds = randomListOfInts(numRows);
    listValues = listOfLists(numRows);

    BatchSchema schema = BatchSchema.newBuilder()
            .addField(ID)
            .addField(VALUE)
            .addField(MYLIST)
            .build();

    container = VectorContainer.create(allocator, schema);
    id = container.addOrGet(ID);
    value = container.addOrGet(VALUE);
    list = container.addOrGet(MYLIST);
    Types.MinorType type = Types.MinorType.BIGINT;
    list.addOrGetVector(FieldType.nullable(type.getType()));
  }

  private static List<JsonStringArrayList<Long>> listOfLists(int size) {
    List<JsonStringArrayList<Long>> listOfLists = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final JsonStringArrayList<Long> list = new JsonStringArrayList<>(INNER_LIST_SIZE);
      for (int j = 0; j < INNER_LIST_SIZE; j++) {
        list.add((long)j + i);
      }
      listOfLists.add(list);
    }
    return listOfLists;
  }

  @Override
  public VectorAccessible getOutput() {
    return container;
  }

  public BatchSchema getSchema() {
    return container.getSchema();
  }

  public SortValidator getValidator(int capacity) {
    return new SortValidator(capacity);
  }

  @Override
  public int next(int records) {
    if (position == values.size()) {
      return 0; // no more data available
    }
    UnionListWriter listWriter = list.getWriter();
    int returned = Math.min(records, values.size() - position);

    container.allocateNew();
    for (int i = 0; i < returned; i++) {
      int rowId = rowIds.get(position + i);
      id.setSafe(i, rowId);
      byte[] valueBytes = values.get(rowId).getBytes();
      value.setSafe(i, valueBytes, 0, valueBytes.length);

      listWriter.setPosition(i);
      listWriter.startList();
      List<Long> list = listValues.get(rowId);
      for (int j = 0; j < INNER_LIST_SIZE; j++) {
        listWriter.bigInt().writeBigInt(list.get(j));
      }
      listWriter.endList();
    }
    container.setAllCount(returned);
    position += returned;

    return returned;
  }

  public Fixtures.Table getExpectedSortedTable() {
    final DataRow[] rows = new DataRow[rowIds.size()];
    for (int i = 0; i < rowIds.size(); i++) {
      rows[i] = tr(i, values.get(i), listValues.get(i));
    }
    return t(th("ID", "FIELD", "MYLIST"), rows);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(container);
  }

  private static List<String> listOfStrings(int size) {
    List<String> strings = new JsonStringArrayList<>(size);
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

  public class SortValidator {
    final List<Integer> sortedRowIds;

    SortValidator(int capacity) {
      sortedRowIds = rowIds.subList(0, capacity);
      Collections.sort(sortedRowIds);
    }

    public void assertIsSorted(VectorContainer container, int startIndex) {
      final IntVector idVector = container.addOrGet(ID);
      final VarCharVector valueVector = container.addOrGet(VALUE);

      int recordCount = container.getRecordCount();
      int index = startIndex;
      for (int i = 0; i < recordCount; i++, index++) {
        int rowId = sortedRowIds.get(index);
        String value = values.get(rowId);
        assertEquals("non matching ID at row " + index, rowId, idVector.get(i));
        assertEquals("non matching VALUE at row " + index, value, valueVector.getObject(i).toString());
      }
    }

  }
}
