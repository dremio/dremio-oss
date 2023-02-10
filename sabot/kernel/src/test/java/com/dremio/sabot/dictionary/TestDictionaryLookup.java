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
package com.dremio.sabot.dictionary;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.util.Map;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.DictionaryLookupPOP;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.driver.OperatorCreatorRegistry;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.dictionary.DictionaryLookupOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.collect.Maps;

/**
 * Test dictionary lookup operator
 */
public class TestDictionaryLookup extends BaseTestOperator {

  @Test
  public void testDictionaryLookup() throws Throwable {


    try (final VectorContainer dict1 = new VectorContainer(getTestAllocator());
         final VectorContainer dict2 = new VectorContainer(getTestAllocator());
         final VectorContainer dict3 = new VectorContainer(getTestAllocator())) {

      final Map<String, GlobalDictionaryFieldInfo> dictionaryFieldInfoMap = Maps.newHashMap();
      final Field field1 = new Field(SchemaPath.getSimplePath("c0").getAsUnescapedPath(), new FieldType(true, new ArrowType.Int(64, true), null), null);
      final BigIntVector longVector = dict1.addOrGet(field1);
      longVector.allocateNew();
      longVector.setSafe(0, 10L);
      longVector.setSafe(1, 20L);
      longVector.setSafe(2, 30L);
      longVector.setSafe(3, 40L);
      longVector.setSafe(4, 50L);
      longVector.setValueCount(5);
      dict1.setRecordCount(5);
      dict1.buildSchema(BatchSchema.SelectionVectorMode.NONE);


      dictionaryFieldInfoMap.put("c0", new GlobalDictionaryFieldInfo(0, "c0", null, field1.getType(), "local"));

      final Field field2 = new Field(SchemaPath.getSimplePath("c1").getAsUnescapedPath(), new FieldType(true, new ArrowType.Binary(), null), null);
      final VarBinaryVector binaryVector = dict2.addOrGet(field2);
      binaryVector.allocateNew();
      binaryVector.setSafe(0, "abc".getBytes(UTF_8), 0, 3);
      binaryVector.setSafe(1, "bcd".getBytes(UTF_8), 0, 3);
      binaryVector.setSafe(2, "cde".getBytes(UTF_8), 0, 3);
      binaryVector.setSafe(3, "def".getBytes(UTF_8), 0, 3);
      binaryVector.setSafe(4, "efg".getBytes(UTF_8), 0, 3);
      binaryVector.setValueCount(5);
      dict2.setRecordCount(5);
      dict2.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      dictionaryFieldInfoMap.put("c1", new GlobalDictionaryFieldInfo(0, "c1", null, field2.getType(), "local"));

      final Field field3 = new Field(SchemaPath.getSimplePath("c2").getAsUnescapedPath(), new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null);
      final Float8Vector doubleVector = dict3.addOrGet(field3);
      doubleVector.allocateNew();
      doubleVector.setSafe(0, 100.1);
      doubleVector.setSafe(1, 200.2);
      doubleVector.setSafe(2, 300.3);
      doubleVector.setSafe(3, 400.4);
      doubleVector.setSafe(4, 500.5);
      doubleVector.setValueCount(5);
      dict3.setRecordCount(5);
      dict3.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      dictionaryFieldInfoMap.put("c2", new GlobalDictionaryFieldInfo(0, "c2", null, field3.getType(), "local"));

      OperatorCreatorRegistry registry = Mockito.mock(OperatorCreatorRegistry.class);
      Mockito.when(registry.getSingleInputOperator(any(OperatorContext.class), any(PhysicalOperator.class)))
        .thenAnswer(new Answer<SingleInputOperator>() {
          @Override
          public SingleInputOperator answer(InvocationOnMock invocation) throws Exception {
            Object[] args = invocation.getArguments();
            DictionaryLookupOperator dictionaryLookupOperator = Mockito.spy(new DictionaryLookupOperator(
              (OperatorContext)args[0], (DictionaryLookupPOP)args[1]));

            Mockito.doReturn(dict1).when(dictionaryLookupOperator).loadDictionary(eq("c0"));
            Mockito.doReturn(dict2).when(dictionaryLookupOperator).loadDictionary(eq("c1"));
            Mockito.doReturn(dict3).when(dictionaryLookupOperator).loadDictionary(eq("c2"));
            return dictionaryLookupOperator;
          }
        });

      BaseTestOperator.testContext.setRegistry(registry);

      DictionaryLookupPOP lookup = new DictionaryLookupPOP(null, PROPS, null, dictionaryFieldInfoMap);
      Table input = t(
        th("c0", "c1", "c2"),
        tr(0, 1, 2),
        tr(1, 2, 0),
        tr(2, 0, 1)
      );

      Table output = t(
        th("c0", "c1", "c2"),
        tr(10L, "bcd".getBytes(UTF_8), 300.3),
        tr(20L, "cde".getBytes(UTF_8), 100.1),
        tr(30L, "abc".getBytes(UTF_8), 200.2)
      );

      validateSingle(lookup, DictionaryLookupOperator.class, input, output);
    }
  }
}
