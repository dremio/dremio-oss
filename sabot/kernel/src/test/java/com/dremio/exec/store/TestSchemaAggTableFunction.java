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
package com.dremio.exec.store;

import static com.dremio.common.expression.CompleteType.BIGINT;
import static com.dremio.common.expression.CompleteType.BIT;
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.store.metadatarefresh.schemaagg.SchemaAggTableFunction;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.test.AllocatorRule;

public class TestSchemaAggTableFunction extends BaseTestQuery {

  private BufferAllocator testAllocator;
  private SampleMutator mutator;
  private RecordReader reader;

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setup() throws Exception {
    testAllocator = allocatorRule.newAllocator("test-SchemaAggTableFunction-allocation", 0, Long.MAX_VALUE);
  }

  @After
  public void close() throws Exception {
    testAllocator.close();
  }

  @Test
  public void TestSchemaAggTableFunctionOutputSchema() throws Exception {
    List<Field> fieldList = Arrays.asList(
      CompleteType.VARBINARY.toField("randomField1"),
      CompleteType.VARBINARY.toField("randomField2"),
      CompleteType.VARBINARY.toField(MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA)
    );

    BatchSchema schema = new BatchSchema(fieldList);

    try(VectorContainer container = VectorContainer.create(testAllocator, schema);
        AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      SchemaAggTableFunction tableFunction = new SchemaAggTableFunction(getOpCtx(), getConfig());
      VectorAccessible output = tableFunction.setup(container);
      closer.addAll(output);
      assertEquals(new BatchSchema(fieldList), output.getSchema());
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void TestSchemaAggTableFunctionOutputValues() throws Exception {
    List<Field> fieldList = Arrays.asList(
      CompleteType.INT.toField("intField"),
      CompleteType.FLOAT.toField("floatField"),
      CompleteType.VARBINARY.toField(MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA)
    );

    BatchSchema schema = new BatchSchema(fieldList);
    try(VectorContainer inputContainer = VectorContainer.create(testAllocator, schema);
        AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      SchemaAggTableFunction tableFunction = new SchemaAggTableFunction(getOpCtx(), getConfig());
      VectorAccessible output = tableFunction.setup(inputContainer);
      closer.addAll(output);
      assertEquals(new BatchSchema(fieldList), output.getSchema());

      IntVector intVector = (IntVector) VectorUtil.getVectorFromSchemaPath(inputContainer, fieldList.get(0).getName());

      intVector.setSafe(0,12);
      intVector.setSafe(1, 13);
      intVector.setSafe(2,14);
      intVector.setSafe(3,15);
      intVector.setValueCount(4);

      Float4Vector float4Vector = (Float4Vector) VectorUtil.getVectorFromSchemaPath(inputContainer, fieldList.get(1).getName());

      float4Vector.setSafe(0, 2.0f);
      float4Vector.setSafe(1, 3.0f);
      float4Vector.setSafe(2,4.0f);
      float4Vector.setSafe(3,5.0f);
      float4Vector.setValueCount(4);

      VarBinaryVector varBinaryVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(inputContainer, fieldList.get(2).getName());

      BatchSchema schema1 = new BatchSchema(Arrays.asList(
        INT.toField("field1")));

      varBinaryVector.setSafe(0, schema1.serialize());
      varBinaryVector.setSafe(1, schema1.serialize());
      varBinaryVector.setSafe(2, schema1.serialize());
      varBinaryVector.setSafe(3, schema1.serialize());
      varBinaryVector.setValueCount(4);

      inputContainer.setRecordCount(4);

      IntVector outputIntVector = (IntVector) VectorUtil.getVectorFromSchemaPath(output, fieldList.get(0).getName());
      Float4Vector outputFloatVector = (Float4Vector) VectorUtil.getVectorFromSchemaPath(output, fieldList.get(1).getName());
      VarBinaryVector outputSchemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(output, fieldList.get(2).getName());

      tableFunction.startRow(0);
      assertEquals(1, tableFunction.processRow(0, 4));
      assertEquals(outputIntVector.getValueCount(), 0);
      assertEquals(outputFloatVector.getValueCount(), 0);
      assertEquals(outputSchemaVector.getValueCount(), 0);

      tableFunction.startRow(1);
      assertEquals(1, tableFunction.processRow(1, 3));
      assertEquals(outputIntVector.getValueCount(), 0);
      assertEquals(outputFloatVector.getValueCount(), 0);
      assertEquals(outputSchemaVector.getValueCount(), 0);

      tableFunction.startRow(2);
      assertEquals(1, tableFunction.processRow(2, 2));
      assertEquals(outputIntVector.getValueCount(), 0);
      assertEquals(outputFloatVector.getValueCount(), 0);
      assertEquals(outputSchemaVector.getValueCount(), 0);

      tableFunction.startRow(3);
      assertEquals(1, tableFunction.processRow(3, 1));
      assertEquals(outputIntVector.getValueCount(), 4);
      assertEquals(outputFloatVector.getValueCount(), 4);
      assertEquals(outputSchemaVector.getValueCount(), 1);


      compareVectors((VectorContainer) output, fieldList.get(0), IntVector.class, Arrays.asList(12, 13, 14, 15));
      compareVectors((VectorContainer) output, fieldList.get(1), Float4Vector.class, Arrays.asList(2.0f, 3.0f, 4.0f, 5.0f));

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }


  @Test
  public void TestSchemaAggUpCasting() throws Exception {
    List<Field> fieldList = Arrays.asList(
      CompleteType.VARBINARY.toField(MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA)
    );

    BatchSchema schema = new BatchSchema(fieldList);
    try(VectorContainer inputContainer = VectorContainer.create(testAllocator, schema);
        AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      VarBinaryVector schemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(inputContainer, fieldList.get(0).getName());

      SchemaAggTableFunction tableFunction = new SchemaAggTableFunction(getOpCtx(), getConfig());
      VectorAccessible outgoing = tableFunction.setup(inputContainer);
      closer.addAll(outgoing);

      BatchSchema schema1 = new BatchSchema(Arrays.asList(
        INT.toField("field1"),
        INT.toField("field2"),
        BIGINT.toField("field3"),
        FLOAT.toField("field4"),
        BIT.toField("field5")
      ));

      BatchSchema schema2 = new BatchSchema(Arrays.asList(
        BIGINT.toField("field1"),
        FLOAT.toField("field2"),
        FLOAT.toField("field3"),
        DOUBLE.toField("field4"),
        VARCHAR.toField("field5"),
        //extra fields in the array
        INT.toField("extraField1"),
        INT.toField("extraField2")
      ));

      schemaVector.setSafe(0, schema1.serialize());
      schemaVector.setSafe(1, schema2.serialize());

      //outgoing vector
      VarBinaryVector outVarBinaryVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);

      tableFunction.startRow(0);
      assertEquals(1, tableFunction.processRow(0, 2));
      assertEquals(outVarBinaryVector.getValueCount(), 0);

      tableFunction.startRow(1);
      assertEquals(1, tableFunction.processRow(1, 1));
      assertEquals(outVarBinaryVector.getValueCount(), 1);

      assertEquals(BatchSchema.deserialize(outVarBinaryVector.get(0)).toJSONString(), schema2.toJSONString());
      tableFunction.closeRow();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void TestSchemaAggMultipleBatches() throws Exception {
    List<Field> fieldList = Arrays.asList(
      CompleteType.VARBINARY.toField(MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA)
    );
    BatchSchema schema = new BatchSchema(fieldList);

    try(VectorContainer inputContainer = VectorContainer.create(testAllocator, schema);
        AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      VarBinaryVector schemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(inputContainer, fieldList.get(0).getName());

      SchemaAggTableFunction tableFunction = new SchemaAggTableFunction(getOpCtx(), getConfig());
      VectorAccessible outgoing = tableFunction.setup(inputContainer);
      closer.addAll(outgoing);

      List<Field> fields = Arrays.asList(INT.toField("field1"), BIGINT.toField("field2"),
        INT.toField("field3"), BIGINT.toField("field4"), FLOAT.toField("field5")
      );

      for(int i = 0; i < fields.size(); i++) {
        BatchSchema sc = new BatchSchema(Collections.singletonList(fields.get(i)));
        schemaVector.setSafe(i, sc.serialize());
      }

      schemaVector.setValueCount(5);

      //outgoing vector
      VarBinaryVector outVarBinaryVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);

      tableFunction.startRow(0);
      assertEquals(1, tableFunction.processRow(0, 5));
      assertEquals(outVarBinaryVector.getValueCount(), 0);

      tableFunction.startRow(1);
      assertEquals(1, tableFunction.processRow(1, 4));
      assertEquals(outVarBinaryVector.getValueCount(), 0);

      tableFunction.startRow(2);
      assertEquals(1, tableFunction.processRow(2, 3));
      assertEquals(outVarBinaryVector.getValueCount(), 0);

      tableFunction.startRow(3);
      assertEquals(1, tableFunction.processRow(3, 2));
      assertEquals(outVarBinaryVector.getValueCount(), 0);

      tableFunction.startRow(4);
      assertEquals(1, tableFunction.processRow(4, 1));
      assertEquals(outVarBinaryVector.getValueCount(), 1);

      BatchSchema finalSchema = new BatchSchema(fields);

      assertEquals(BatchSchema.deserialize(outVarBinaryVector.get(0)).toJSONString(), finalSchema.toJSONString());
      tableFunction.closeRow();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void TestSchemaAggValueVectorHasLessRecords() throws Exception {
    List<Field> fieldList = Arrays.asList(
      CompleteType.VARBINARY.toField(MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA)
    );

    BatchSchema schema = new BatchSchema(fieldList);

    try(VectorContainer inputContainer = VectorContainer.create(testAllocator, schema);
        AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      VarBinaryVector schemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(inputContainer, fieldList.get(0).getName());

      List<Field> fields = Arrays.asList(INT.toField("field1"), BIGINT.toField("field2"));

      for(int i = 0; i < fields.size(); i++) {
        BatchSchema sc = new BatchSchema(Collections.singletonList(fields.get(i)));
        schemaVector.setSafe(i, sc.serialize());
      }

      schemaVector.setValueCount(2);

      SchemaAggTableFunction tableFunction = new SchemaAggTableFunction(getOpCtx(), getConfig());
      VectorAccessible outgoing = tableFunction.setup(inputContainer);

      closer.addAll(outgoing);
      //outgoing vector
      VarBinaryVector outVarBinaryVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);

      tableFunction.startRow(0);
      assertEquals(1, tableFunction.processRow(0, 5));
      assertEquals(outVarBinaryVector.getValueCount(), 0);

      tableFunction.startRow(1);
      assertEquals(1, tableFunction.processRow(1, 4));
      assertEquals(outVarBinaryVector.getValueCount(), 1);

      BatchSchema finalSchema = new BatchSchema(fields);
      assertEquals(BatchSchema.deserialize(outVarBinaryVector.get(0)).toJSONString(), finalSchema.toJSONString());
      tableFunction.closeRow();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private TableFunctionConfig getConfig() {

    FileConfig fileConfig = new FileConfig();
    fileConfig.setType(FileType.PARQUET);

    TableFunctionContext functionContext = mock(TableFunctionContext.class);

    TableFunctionConfig config = new TableFunctionConfig(TableFunctionConfig.FunctionType.SCHEMA_AGG, true,
      functionContext);
    return config;
  }

  private OperatorContext getOpCtx() {
    SabotContext sabotContext = getSabotContext();
    return new OperatorContextImpl(sabotContext.getConfig(), sabotContext.getDremioConfig(), testAllocator, sabotContext.getOptionManager(), 10, sabotContext.getExpressionSplitCache());
  }

  private void compareVectors(VectorContainer outputContainer, Field field, Class type, List<Object> values) {

    ValueVector outputVector = VectorUtil.getVectorFromSchemaPath(outputContainer, field.getName());
    assertEquals(outputVector.getValueCount(), outputVector.getValueCount());

    for (int i = 0; i < outputVector.getValueCount(); i++) {
      assertEquals(outputVector.getObject(i).toString(), values.get(i).toString());
    }
  }
}
