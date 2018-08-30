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
import static org.apache.arrow.vector.types.Types.MinorType.BIT;
import static org.apache.arrow.vector.types.Types.MinorType.INT;
import static org.apache.arrow.vector.types.Types.MinorType.LIST;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.apache.arrow.vector.types.pojo.FieldType.nullable;

import io.netty.buffer.ArrowBuf;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.Fixtures.DataRow;
import com.google.common.base.Preconditions;

import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;

/**
 * Generates a table with lot of nested data types and two scalar types
 */
public class ComplexDataGenerator implements Generator {

  private static final Random random = new Random(2893467218534L);

  private static final Field ints = CompleteType.INT.toField("ints");
  private static final Field strings = CompleteType.VARCHAR.toField("strings");
  private static final Field l1Ints = CompleteType.INT.asList().toField("l1Ints");
  private static final Field l1Strings = CompleteType.VARCHAR.asList().toField("l1Strings");
  private static final Field l2Ints = CompleteType.INT.asList().asList().toField("l2Ints");
  private static final Field l2Strings = CompleteType.VARCHAR.asList().asList().toField("l2Strings");
  private static final Field l3Ints = CompleteType.INT.asList().asList().asList().toField("l3Ints");
  private static final Field l3Strings = CompleteType.VARCHAR.asList().asList().asList().toField("l3Strings");
  private static final Field map = CompleteType.struct(
      CompleteType.VARCHAR.toField("varchar"),
      CompleteType.INT.toField("int"),
      CompleteType.BIT.asList().toField("bits")
  ).toField("map");

  private final List<Integer> intsVals;
  private final List<String> stringsVals;
  private final List<List<Integer>> l1IntsVals;
  private final List<List<String>> l1StringsVals;
  private final List<List<List<Integer>>> l2IntsVals;
  private final List<List<List<String>>> l2StringsVals;
  private final List<List<List<List<Integer>>>> l3IntsVals;
  private final List<List<List<List<String>>>> l3StringsVals;
  private final List<Map<String, Object>> mapVals;

  private final VectorContainer container;

  private final IntVector intsVector;
  private final VarCharVector stringsVector;
  private final ListVector l1IntsVector;
  private final ListVector l1StringsVector;
  private final ListVector l2IntsVector;
  private final ListVector l2StringsVector;
  private final ListVector l3IntsVector;
  private final ListVector l3StringsVector;
  private final StructVector structVector;

  private int position;
  private ArrowBuf tempBuf;

  public ComplexDataGenerator(int numRows, BufferAllocator allocator) {
    Preconditions.checkState(numRows > 0);
    intsVals = ints(numRows);
    stringsVals = strings(numRows);
    l1IntsVals = l1Ints(numRows);
    l1StringsVals = l1Strings(numRows);
    l2IntsVals = l2Ints(numRows);
    l2StringsVals = l2Strings(numRows);
    l3IntsVals = l3Ints(numRows);
    l3StringsVals = l3Strings(numRows);
    mapVals = mapVals(numRows);

    BatchSchema schema = BatchSchema.newBuilder()
        .addField(ints)
        .addField(strings)
        .addField(l1Ints)
        .addField(l1Strings)
        .addField(l2Ints)
        .addField(l2Strings)
        .addField(l3Ints)
        .addField(l3Strings)
        .addField(map)
        .build();

    container = VectorContainer.create(allocator, schema);

    intsVector = container.addOrGet(ints);
    stringsVector = container.addOrGet(strings);
    l1IntsVector = container.addOrGet(l1Ints);
    addChild(l1IntsVector, INT);
    l1StringsVector = container.addOrGet(l1Strings);
    addChild(l1StringsVector, VARCHAR);
    l2IntsVector = container.addOrGet(l2Ints);
    addChild(addChild(l2IntsVector, LIST), INT);
    l2StringsVector = container.addOrGet(l2Strings);
    addChild(addChild(l2StringsVector, LIST), VARCHAR);
    l3IntsVector = container.addOrGet(l3Ints);
    addChild(addChild(addChild(l3IntsVector, LIST), LIST), INT);
    l3StringsVector = container.addOrGet(l3Strings);
    addChild(addChild(addChild(l3StringsVector, LIST), LIST), VARCHAR);
    structVector = container.addOrGet(map);
    structVector.addOrGet("varchar", nullable(VARCHAR.getType()), VarCharVector.class);
    structVector.addOrGet("int", nullable(INT.getType()), IntVector.class);
    ListVector listVector = structVector.addOrGet("bits", nullable(LIST.getType()), ListVector.class);
    addChild(listVector, BIT);

    tempBuf = allocator.buffer(2048);
  }

  private ValueVector addChild(ValueVector vector, MinorType child) {
    return ((ListVector)vector).addOrGetVector(nullable(child.getType())).getVector();
  }

  private static List<List<Integer>> l1Ints(int size) {
    List<List<Integer>> listOfLists = new JsonStringArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final int listSize = random.nextInt(5);
      final List<Integer> list = new JsonStringArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        list.add(random.nextInt());
      }
      listOfLists.add(list);
    }
    return listOfLists;
  }

  private static List<List<String>> l1Strings(int size) {
    List<List<String>> listOfLists = new JsonStringArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final int listSize = random.nextInt(10);
      final List<String> list = new JsonStringArrayList<>(listSize);
      for (int j = 0; j < listSize; j++) {
        list.add(randomString());
      }
      listOfLists.add(list);
    }
    return listOfLists;
  }

  private static List<List<List<Integer>>> l2Ints(int size) {
    List<List<List<Integer>>> l2Ints = new JsonStringArrayList<>(size);
    for(int i = 0; i < size; i++) {
      final int listSize = random.nextInt(3);
      l2Ints.add(l1Ints(listSize));
    }

    return l2Ints;
  }

  private static List<List<List<String>>> l2Strings(int size) {
    List<List<List<String>>> l2Strings = new JsonStringArrayList<>(size);
    for(int i = 0; i < size; i++) {
      final int listSize = random.nextInt(3);
      l2Strings.add(l1Strings(listSize));
    }

    return l2Strings;
  }

  private static List<List<List<List<Integer>>>> l3Ints(int size) {
    List<List<List<List<Integer>>>> l3Ints = new JsonStringArrayList<>(size);
    for(int i = 0; i < size; i++) {
      final int listSize = random.nextInt(3);
      l3Ints.add(l2Ints(listSize));
    }

    return l3Ints;
  }

  private static List<List<List<List<String>>>> l3Strings(int size) {
    List<List<List<List<String>>>> l3Strings = new JsonStringArrayList<>(size);
    for(int i = 0; i < size; i++) {
      final int listSize = random.nextInt(3);
      l3Strings.add(l2Strings(listSize));
    }

    return l3Strings;
  }

  private static List<Map<String, Object>> mapVals(int size) {
    List<Map<String, Object>> vals = new JsonStringArrayList<>(size);
    for(int i = 0; i < size; i++) {
      Map<String, Object> val = new JsonStringHashMap<>();
      val.put("varchar", randomString());
      val.put("int", random.nextInt());
      val.put("bits", new Boolean[] { true, false, false, true});
      vals.add(val);
    }

    return vals;
  }

  private static String randomString() {
    int length = random.nextInt(50);
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      builder.append((char)random.nextInt(26) + 65);
    }
    return builder.toString();
  }

  @Override
  public VectorAccessible getOutput() {
    return container;
  }

  public BatchSchema getSchema() {
    return container.getSchema();
  }

  @Override
  public int next(int records) {
    if (position == intsVals.size()) {
      return 0; // no more data available
    }
    int toReturn = Math.min(records, intsVals.size() - position);

    container.allocateNew();

    UnionListWriter l1IntsWriter = l1IntsVector.getWriter();
    UnionListWriter l1StringsWriter = l1StringsVector.getWriter();
    UnionListWriter l2IntsWriter = l2IntsVector.getWriter();
    UnionListWriter l2StringsWriter = l2StringsVector.getWriter();
    UnionListWriter l3IntsWriter = l3IntsVector.getWriter();
    UnionListWriter l3StringsWriter = l3StringsVector.getWriter();
    NullableStructWriter structWriter = structVector.getWriter();

    for (int i = 0; i < toReturn; i++) {
      intsVector.setSafe(i, intsVals.get(position + i));
      byte[] valueBytes = stringsVals.get(position + i).getBytes();
      stringsVector.setSafe(i, valueBytes, 0, valueBytes.length);

      {
        l1IntsWriter.setPosition(i);
        l1IntsWriter.startList();
        List<Integer> intsList = l1IntsVals.get(position + i);
        for (int j = 0; j < intsList.size(); j++) {
          l1IntsWriter.integer().writeInt(intsList.get(j));
        }
        l1IntsWriter.endList();
      }

      {
        l1StringsWriter.setPosition(i);
        l1StringsWriter.startList();
        List<String> stringsList = l1StringsVals.get(position + i);
        for (int j = 0; j < stringsList.size(); j++) {
          byte[] bytes = stringsList.get(j).getBytes();
          tempBuf.setBytes(0, bytes, 0, bytes.length);
          l1StringsWriter.varChar().writeVarChar(0, bytes.length, tempBuf);
        }
        l1StringsWriter.endList();
      }

      {
        l2IntsWriter.setPosition(i);
        l2IntsWriter.startList();
        List<List<Integer>> intsListList = l2IntsVals.get(position + i);
        for (int j = 0; j < intsListList.size(); j++) {
          ListWriter nestedWriter = l2IntsWriter.list();
          nestedWriter.setPosition(j);
          nestedWriter.startList();
          for(int k = 0; k < intsListList.get(j).size(); k++) {
            nestedWriter.integer().writeInt(intsListList.get(j).get(k));
          }
          nestedWriter.startList();
        }
        l2IntsWriter.endList();
      }

      {
        l2StringsWriter.setPosition(i);
        l2StringsWriter.startList();
        List<List<String>> stringsListList = l2StringsVals.get(position + i);
        for (int j = 0; j < stringsListList.size(); j++) {
          ListWriter nestedWriter = l2StringsWriter.list();
          nestedWriter.setPosition(j);
          nestedWriter.startList();
          for(int k = 0; k < stringsListList.get(j).size(); k++) {
            byte[] bytes = stringsListList.get(j).get(k).getBytes();
            tempBuf.setBytes(0, bytes, 0, bytes.length);
            nestedWriter.varChar().writeVarChar(0, bytes.length, tempBuf);
          }
          nestedWriter.startList();
        }
        l2StringsWriter.endList();
      }

      {
        l3IntsWriter.setPosition(i);
        l3IntsWriter.startList();
        List<List<List<Integer>>> intsListListList = l3IntsVals.get(position + i);
        for (int j = 0; j < intsListListList.size(); j++) {
          ListWriter nested1Writer = l3IntsWriter.list();
          nested1Writer.setPosition(j);
          nested1Writer.startList();
          List<List<Integer>> intsListList = intsListListList.get(j);
          for(int k = 0; k < intsListList.size(); k++) {
            ListWriter nested2Writer = nested1Writer.list();
            nested2Writer.setPosition(k);
            nested2Writer.startList();
            List<Integer> intsList = intsListList.get(k);
            for(int l = 0; l < intsList.size(); l++) {
              nested2Writer.integer().writeInt(intsList.get(l));
            }
            nested2Writer.endList();
          }
          nested1Writer.startList();
        }
        l3IntsWriter.endList();
      }

      {
        l3StringsWriter.setPosition(i);
        l3StringsWriter.startList();
        List<List<List<String>>> stringsListListList = l3StringsVals.get(position + i);
        for (int j = 0; j < stringsListListList.size(); j++) {
          ListWriter nested1Writer = l3StringsWriter.list();
          nested1Writer.setPosition(j);
          nested1Writer.startList();
          List<List<String>> stringsListList = stringsListListList.get(j);
          for(int k = 0; k < stringsListList.size(); k++) {
            ListWriter nested2Writer = nested1Writer.list();
            nested2Writer.setPosition(k);
            nested2Writer.startList();
            List<String> stringsList = stringsListList.get(k);
            for(int l = 0; l < stringsList.size(); l++) {
              byte[] bytes = stringsList.get(l).getBytes();
              tempBuf.setBytes(0, bytes, 0, bytes.length);
              nested2Writer.varChar().writeVarChar(0, bytes.length, tempBuf);
            }
            nested2Writer.endList();
          }
          nested1Writer.startList();
        }
        l3StringsWriter.endList();
      }

      {
        structWriter.setPosition(i);

        Map<String, Object> val = mapVals.get(position + i);

        byte[] bytes = ((String)val.get("varchar")).getBytes();
        tempBuf.setBytes(0, bytes, 0, bytes.length);
        structWriter.varChar("varchar").writeVarChar(0, bytes.length, tempBuf);
        structWriter.integer("int").writeInt((Integer)val.get("int"));
        ListWriter listWriter = structWriter.list("bits");
        listWriter.startList();
        Boolean[] bits = (Boolean[])val.get("bits");
        for(int j = 0; j < bits.length; j++) {
          listWriter.setPosition(j);
          listWriter.bit().writeBit(bits[j] ? 1 : 0);
        }
        listWriter.endList();
      }
    }

    container.setAllCount(toReturn);
    position += toReturn;

    return toReturn;
  }

  public Fixtures.Table getExpectedSortedTable() {
    final int size = intsVals.size();
    final DataRow[] rows = new DataRow[size];
    for (int i = 0; i < size; i++) {
      rows[i] = tr(
          intsVals.get(i),
          stringsVals.get(i),
          l1IntsVals.get(i),
          l1StringsVals.get(i),
          l2IntsVals.get(i),
          l2StringsVals.get(i),
          l3IntsVals.get(i),
          l3StringsVals.get(i)
      );
    }
    return t(th("ints", "strings", "l1Ints", "l1Strings", "l2Ints", "l2String", "l3Ints", "l3Strings"), rows);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(container, tempBuf);
  }

  private static List<String> strings(int size) {
    List<String> strings = new JsonStringArrayList<>(size);
    for (int i = 0; i < size; i++) {
      strings.add(randomString());
    }
    return strings;
  }

  /**
   * @return shuffled list of integers [0..size)
   */
  private static List<Integer> ints(int size) {
    Integer[] ids = new Integer[size];
    for (int i = 0; i < size; i++) {
      ids[i] = i;
    }

    List<Integer> rndList = Arrays.asList(ids);
    Collections.shuffle(rndList);
    return rndList;
  }
}
