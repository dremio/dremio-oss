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
package com.dremio.sabot;

import static com.dremio.sabot.Fixtures.NULL_INT;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.Fixtures.DataRow;
import com.dremio.sabot.op.filter.VectorContainerWithSV;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringArrayList;

/** Creates custom data with a Selection vector. */
public class CustomGeneratorWithSV2 implements Generator {

  public static final Field BITF = CompleteType.BIT.toField("BIT");
  public static final Field INTF = CompleteType.INT.toField("INT");
  public static final Field BIGINTF = CompleteType.BIGINT.toField("BIGINT");
  public static final Field DECIMALF =
      new Field("DECIMAL", new FieldType(true, new Decimal(38, 0, 128), null), null);
  public static final Field STRINGF = CompleteType.VARCHAR.toField("STRING");
  private static final Field LISTF = CompleteType.BIGINT.asList().toField("LIST");

  private static final int INNER_LIST_SIZE = 3;

  private final int numRows;
  private final BitSet bitValues;
  private final List<Integer> intValues;
  private final List<Long> longValues;
  private final List<BigDecimal> decimalValues;
  private final List<String> stringValues;
  private final List<JsonStringArrayList<Long>> listValues;
  private final BitSet selectedBitSet;

  private final VectorContainer container;
  private final BitVector bitVector;
  private final IntVector intVector;
  private final BigIntVector bigIntVector;
  private final DecimalVector decimalVector;
  private final VarCharVector stringVector;
  private final ListVector listVector;

  private final SelectionVector2 sv2;

  private int totalSelectedRows;
  private int position;

  public enum SelectionVariant {
    SELECT_ALTERNATE,
    SELECT_NONE,
    SELECT_ALL
  }

  public CustomGeneratorWithSV2(int numRows, BufferAllocator allocator, SelectionVariant variant) {
    Preconditions.checkState(numRows > 0);
    this.numRows = numRows;
    this.stringValues = listOfStrings(numRows);
    this.intValues = randomListOfInts(numRows);
    this.longValues = randomListOfLongs(numRows);
    this.decimalValues = randomListOfDecimals(numRows);
    this.listValues = listOfLists(numRows);
    this.bitValues = randomBits(numRows);
    this.selectedBitSet = new BitSet(numRows);
    this.totalSelectedRows = 0;
    computeSelection(variant);

    this.sv2 = new SelectionVector2(allocator);
    this.container = new VectorContainerWithSV(allocator, sv2);
    this.bitVector = container.addOrGet(BITF);
    this.intVector = container.addOrGet(INTF);
    this.bigIntVector = container.addOrGet(BIGINTF);
    this.decimalVector = container.addOrGet(DECIMALF);
    this.stringVector = container.addOrGet(STRINGF);
    this.listVector = container.addOrGet(LISTF);
    container.buildSchema(SelectionVectorMode.TWO_BYTE);
    listVector.addOrGetVector(FieldType.nullable(MinorType.BIGINT.getType()));
  }

  private static List<JsonStringArrayList<Long>> listOfLists(int size) {
    List<JsonStringArrayList<Long>> listOfLists = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final JsonStringArrayList<Long> list = new JsonStringArrayList<>(INNER_LIST_SIZE);
      for (int j = 0; j < INNER_LIST_SIZE; j++) {
        list.add((long) j + i);
      }
      listOfLists.add(list);
    }
    return listOfLists;
  }

  private void computeSelection(SelectionVariant variant) {
    switch (variant) {
      case SELECT_ALTERNATE:
        for (int i = 0; i < numRows; i++) {
          if (i % 2 == 0) {
            selectedBitSet.set(i);
            ++totalSelectedRows;
          }
        }
        break;

      case SELECT_ALL:
        selectedBitSet.set(0, numRows);
        totalSelectedRows = numRows;
        break;

      case SELECT_NONE:
        selectedBitSet.clear(0, numRows);
        totalSelectedRows = 0;
        break;
    }
  }

  @Override
  public VectorAccessible getOutput() {
    return container;
  }

  public BatchSchema getSchema() {
    return container.getSchema();
  }

  public int totalSelectedRows() {
    return totalSelectedRows;
  }

  @Override
  public int next(int records) {
    if (position == numRows) {
      return 0; // no more data available
    }
    UnionListWriter listWriter = listVector.getWriter();

    sv2.allocateNew(records);
    int toFill = Math.min(records, numRows - position);
    int selected = 0;
    container.allocateNew();
    for (int i = 0; i < toFill; i++) {
      if (selectedBitSet.get(position + i)) {
        sv2.setIndex(selected, (char) i);
        ++selected;
      }

      int rowId = position + i;
      bitVector.setSafe(i, bitValues.get(rowId) ? 1 : 0);
      if (intValues.get(rowId) != null) {
        intVector.setSafe(i, intValues.get(rowId));
      }
      bigIntVector.setSafe(i, longValues.get(rowId));
      decimalVector.setSafe(i, decimalValues.get(rowId));
      byte[] valueBytes = stringValues.get(rowId).getBytes();
      stringVector.setSafe(i, valueBytes, 0, valueBytes.length);

      listWriter.setPosition(i);
      listWriter.startList();
      List<Long> list = listValues.get(rowId);
      for (int j = 0; j < INNER_LIST_SIZE; j++) {
        listWriter.bigInt().writeBigInt(list.get(j));
      }
      listWriter.endList();
    }
    position += toFill;

    sv2.setRecordCount(selected);
    container.setAllCount(selected);
    return selected;
  }

  public Fixtures.Table getExpectedTable() {
    if (totalSelectedRows == 0) {
      return null;
    }

    final DataRow[] rows = new DataRow[totalSelectedRows];
    int idx = 0;
    for (int i = 0; i < intValues.size(); i++) {
      if (!selectedBitSet.get(i)) {
        continue;
      }

      rows[idx] =
          tr(
              bitValues.get(i),
              intValues.get(i) == null ? NULL_INT : intValues.get(i),
              longValues.get(i),
              decimalValues.get(i),
              stringValues.get(i),
              listValues.get(i));
      ++idx;
    }
    return t(th("BIT", "INT", "BIGINT", "DECIMAL", "STRING", "LIST"), rows);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(container);
  }

  private static List<String> listOfStrings(int size) {
    List<String> strings = new JsonStringArrayList<>(size);
    String appender = "Hello all, this is a brave new world !";

    for (int i = 0; i < size; i++) {
      String subStr = appender.substring(0, i % appender.length());
      strings.add(subStr + String.format("%d", System.currentTimeMillis()));
    }
    return strings;
  }

  /**
   * @return shuffled list of integers [0..size)
   */
  private static List<Integer> randomListOfInts(int size) {
    Integer[] ids = new Integer[size];
    for (int i = 0; i < size; i++) {
      if (i % 10 == 0) {
        // add some nulls
        continue;
      }
      ids[i] = i;
    }

    List<Integer> rndList = Arrays.asList(ids);
    Collections.shuffle(rndList);
    return rndList;
  }

  private static List<BigDecimal> randomListOfDecimals(int size) {
    BigDecimal[] ids = new BigDecimal[size];
    for (int i = 0; i < size; i++) {
      ids[i] = new BigDecimal(i);
    }

    List<BigDecimal> rndList = Arrays.asList(ids);
    Collections.shuffle(rndList);
    return rndList;
  }

  private static List<Long> randomListOfLongs(int size) {
    Long[] ids = new Long[size];
    for (int i = 0; i < size; i++) {
      ids[i] = (long) i;
    }

    List<Long> rndList = Arrays.asList(ids);
    Collections.shuffle(rndList);
    return rndList;
  }

  private static BitSet randomBits(int size) {
    BitSet bits = new BitSet(size);
    for (int i = 0; i < size; ++i) {
      if (i % 3 == 0) {
        bits.set(i);
      }
    }
    return bits;
  }
}
