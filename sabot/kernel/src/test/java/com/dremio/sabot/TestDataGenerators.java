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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.FieldInfo.SortOrder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

public class TestDataGenerators extends BaseTestOperator {

  @Test
  public void testSortDataGeneartorSingleUniqueValue() throws Exception {
    List<FieldInfo> fieldInfos = new ArrayList<>();
    int batchSize = 1000;
    // add a string field
    addFieldInfos(fieldInfos, new ArrowType.Utf8(), 1, SortOrder.ASCENDING, 1);
    // add int field
    addFieldInfos(fieldInfos, new ArrowType.Int(32, true), 1, SortOrder.DESCENDING, 1);
    // add boolean field
    addFieldInfos(fieldInfos, new ArrowType.Bool(), 1, SortOrder.ASCENDING, 1);
    // add date field
    addFieldInfos(fieldInfos, new ArrowType.Date(DateUnit.MILLISECOND), 1, SortOrder.DESCENDING, 1);

    SortDataGenerator sdg = new SortDataGenerator(fieldInfos, allocator, 10_000, batchSize, 15);

    VectorContainer container = (VectorContainer) sdg.getOutput();

    // check all fields added above are part of container's schema
    assertThat(container.getSchema().getFields())
        .containsExactlyElementsOf(
            fieldInfos.stream().map(FieldInfo::getField).collect(Collectors.toList()));

    int totalRecords = 0;
    // generate records less than batch size
    totalRecords += sdg.next(100);
    for (FieldInfo fieldInfo : fieldInfos) {
      // check if 100 records are generated for all field vectors
      assertEquals(container.addOrGet(fieldInfo.getField()).getValueCount(), 100);
    }

    // generate records more than batch size
    totalRecords += sdg.next(batchSize + 250);
    for (FieldInfo fieldInfo : fieldInfos) {
      // check if number of records generated are capped at batchSize
      assertEquals(container.addOrGet(fieldInfo.getField()).getValueCount(), batchSize);
    }

    while (sdg.getRecordsRemaining() != 0) {
      totalRecords += sdg.next(batchSize);
    }

    assertEquals(totalRecords, 10_000);

    Set<Integer> integerOutput = new HashSet<>();
    Set<String> stringOutputs = new HashSet<>();

    ArrowType integerType = new ArrowType.Int(32, true);
    ArrowType stringType = new ArrowType.Utf8();

    for (VectorWrapper<?> vw : container) {
      ArrowType type = vw.getField().getType();
      ValueVector vv = vw.getValueVector();
      if (type.equals(integerType)) {
        for (int i = 0; i < batchSize; i++) {
          integerOutput.add(((Integer) vv.getObject(i)));
        }
      } else if (type.equals(stringType)) {
        for (int i = 0; i < batchSize; i++) {
          stringOutputs.add(vv.getObject(i).toString());
        }
      }
    }
    // Since the parameter numberOfUniqueValues is 1, all the values in the output should be the
    // same
    assertEquals(integerOutput.size(), 1);
    assertEquals(stringOutputs.size(), 1);

    container.close();
    sdg.close();
  }

  @Test
  public void testSortDataGeneratorAllUniqueValues() throws Exception {
    List<FieldInfo> fieldInfos = new ArrayList<>();
    int batchSize = 1000;

    // add a string field
    addFieldInfos(fieldInfos, new ArrowType.Utf8(), 10_000, SortOrder.ASCENDING, 1);
    // add int field
    addFieldInfos(fieldInfos, new ArrowType.Int(32, true), 10_000, SortOrder.DESCENDING, 1);
    // add boolean field
    addFieldInfos(fieldInfos, new ArrowType.Bool(), 10_000, SortOrder.ASCENDING, 1);
    // add date field
    addFieldInfos(
        fieldInfos, new ArrowType.Date(DateUnit.MILLISECOND), 10_000, SortOrder.DESCENDING, 1);

    SortDataGenerator sdg = new SortDataGenerator(fieldInfos, allocator, 10_000, batchSize, 15);

    VectorContainer container = (VectorContainer) sdg.getOutput();

    sdg.next(batchSize);

    Set<Integer> integerOutput = new HashSet<>();
    Set<String> stringOutputs = new HashSet<>();

    ArrowType integerType = new ArrowType.Int(32, true);
    ArrowType stringType = new ArrowType.Utf8();

    for (VectorWrapper<?> vw : container) {
      ArrowType type = vw.getField().getType();
      ValueVector vv = vw.getValueVector();
      if (type.equals(integerType)) {
        for (int i = 0; i < batchSize; i++) {
          integerOutput.add(((Integer) vv.getObject(i)));
        }
      } else if (type.equals(stringType)) {
        for (int i = 0; i < batchSize; i++) {
          stringOutputs.add(vv.getObject(i).toString());
        }
      }
    }
    // Every single value should be unique
    assertEquals(integerOutput.size(), 1000);
    assertEquals(stringOutputs.size(), 1000);

    container.close();
    sdg.close();
  }

  private static void addFieldInfos(
      List<FieldInfo> list,
      ArrowType arrowType,
      int numberOfUniqueValues,
      SortOrder sortOrder,
      int numberOfInstances) {
    int labelIndex = list.size();
    for (int i = 0; i < numberOfInstances; i++) {
      list.add(
          new FieldInfo(
              arrowType.getTypeID().toString() + "_" + labelIndex + "_" + i,
              arrowType,
              numberOfUniqueValues,
              sortOrder));
    }
  }
}
