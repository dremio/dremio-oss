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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.store.SystemSchemas.COL_IDS;
import static com.dremio.exec.store.SystemSchemas.DATAFILE_PATH;
import static com.dremio.exec.store.SystemSchemas.DELETE_FILE;
import static com.dremio.exec.store.SystemSchemas.FILE_SIZE;
import static com.dremio.exec.store.SystemSchemas.PARTITION_INFO;
import static com.dremio.exec.store.SystemSchemas.PARTITION_SPEC_ID;
import static com.dremio.exec.store.SystemSchemas.buildDeleteFileStruct;
import static com.dremio.sabot.RecordSet.li;
import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rb;
import static com.dremio.sabot.RecordSet.rs;
import static com.dremio.sabot.RecordSet.st;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.RecordBatchValidatorDefaultImpl;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.RecordSet.Tuple;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.FileContent;
import org.junit.Test;

public class TestIcebergDeleteFileAggTableFunction extends BaseTestTableFunction {

  private static final BatchSchema INPUT_SCHEMA =
      BatchSchema.newBuilder()
          .addField(Field.nullable(DATAFILE_PATH, Types.MinorType.VARCHAR.getType()))
          .addField(Field.nullable(FILE_SIZE, Types.MinorType.BIGINT.getType()))
          .addField(Field.nullable(PARTITION_INFO, Types.MinorType.VARBINARY.getType()))
          .addField(Field.nullable(COL_IDS, Types.MinorType.VARBINARY.getType()))
          .addField(buildDeleteFileStruct(DELETE_FILE))
          .addField(Field.nullable(PARTITION_SPEC_ID, Types.MinorType.INT.getType()))
          .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
          .build();

  private static final byte[] PARTITION_1 = new byte[] {10};
  private static final byte[] PARTITION_2 = new byte[] {20};
  private static final byte[] PARTITION_3 = new byte[] {30};

  private static final int PARTITION_SPEC_ID_1 = 1;
  private static final int PARTITION_SPEC_ID_2 = 2;
  private static final int PARTITION_SPEC_ID_3 = 3;

  private static final byte[] COL_IDS_1 = new byte[] {1};
  private static final byte[] COL_IDS_2 = new byte[] {2};
  private static final byte[] COL_IDS_3 = new byte[] {3};

  private static final Tuple DELETE_1 = st("delete1", FileContent.POSITION_DELETES.id(), 10L, null);
  private static final Tuple DELETE_2 =
      st("delete2", FileContent.EQUALITY_DELETES.id(), 20L, li(1, 2));
  private static final Tuple DELETE_3 =
      st("delete3", FileContent.EQUALITY_DELETES.id(), 30L, li(2));
  private static final Tuple DELETE_4 = st("delete4", FileContent.POSITION_DELETES.id(), 40L, null);
  private static final Tuple DELETE_5 = st("delete5", FileContent.POSITION_DELETES.id(), 50L, null);
  private static final Tuple DELETE_6 =
      st("delete6", FileContent.EQUALITY_DELETES.id(), 60L, li(1, 2));
  private static final Tuple DELETE_7 = st("delete7", FileContent.POSITION_DELETES.id(), 70L, null);

  @Test
  public void testBasicAgg() throws Exception {
    RecordSet input =
        rs(
            INPUT_SCHEMA,
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_1, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_2, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_3, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_4, PARTITION_SPEC_ID_1),
            r("data2", 200L, PARTITION_2, COL_IDS_2, DELETE_2, PARTITION_SPEC_ID_2),
            r("data3", 300L, PARTITION_3, COL_IDS_3, DELETE_2, PARTITION_SPEC_ID_3),
            r("data3", 300L, PARTITION_3, COL_IDS_3, DELETE_5, PARTITION_SPEC_ID_3));

    RecordSet output =
        rs(
            SystemSchemas.ICEBERG_DELETE_FILE_AGG_SCHEMA,
            r(
                "data1",
                100L,
                PARTITION_1,
                COL_IDS_1,
                li(DELETE_1, DELETE_2, DELETE_3, DELETE_4),
                PARTITION_SPEC_ID_1),
            r("data2", 200L, PARTITION_2, COL_IDS_2, li(DELETE_2), PARTITION_SPEC_ID_2),
            r("data3", 300L, PARTITION_3, COL_IDS_3, li(DELETE_2, DELETE_5), PARTITION_SPEC_ID_3));

    validateSingle(
        getPop(),
        TableFunctionOperator.class,
        input,
        new RecordBatchValidatorDefaultImpl(output),
        3);
  }

  @Test
  public void testNullDeleteFilePath() throws Exception {
    RecordSet input =
        rs(
            INPUT_SCHEMA,
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_1, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_2, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_3, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_4, PARTITION_SPEC_ID_1),
            r("data2", 200L, PARTITION_2, COL_IDS_2, DELETE_2, PARTITION_SPEC_ID_2),
            r("data3", 300L, PARTITION_3, COL_IDS_3, DELETE_2, PARTITION_SPEC_ID_3),
            r("data3", 300L, PARTITION_3, COL_IDS_3, DELETE_5, PARTITION_SPEC_ID_3),
            r("data4", 400L, PARTITION_1, COL_IDS_1, null, PARTITION_SPEC_ID_1));

    RecordSet output =
        rs(
            SystemSchemas.ICEBERG_DELETE_FILE_AGG_SCHEMA,
            r(
                "data1",
                100L,
                PARTITION_1,
                COL_IDS_1,
                li(DELETE_1, DELETE_2, DELETE_3, DELETE_4),
                PARTITION_SPEC_ID_1),
            r("data2", 200L, PARTITION_2, COL_IDS_2, li(DELETE_2), PARTITION_SPEC_ID_2),
            r("data3", 300L, PARTITION_3, COL_IDS_3, li(DELETE_2, DELETE_5), PARTITION_SPEC_ID_3),
            r("data4", 400L, PARTITION_1, COL_IDS_1, li(), PARTITION_SPEC_ID_1));

    validateSingle(
        getPop(),
        TableFunctionOperator.class,
        input,
        new RecordBatchValidatorDefaultImpl(output),
        3);
  }

  @Test
  public void testAggregateWithMultipleOutputBathes() throws Exception {
    RecordSet input =
        rs(
            INPUT_SCHEMA,
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_1, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_2, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_3, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_4, PARTITION_SPEC_ID_1),
            r("data2", 200L, PARTITION_2, COL_IDS_2, DELETE_2, PARTITION_SPEC_ID_2),
            r("data3", 300L, PARTITION_3, COL_IDS_3, DELETE_2, PARTITION_SPEC_ID_3),
            r("data3", 300L, PARTITION_3, COL_IDS_3, DELETE_5, PARTITION_SPEC_ID_3),
            r("data4", 400L, PARTITION_1, COL_IDS_1, null, PARTITION_SPEC_ID_1),
            r("data5", 500L, PARTITION_2, COL_IDS_2, DELETE_6, PARTITION_SPEC_ID_2),
            r("data5", 500L, PARTITION_2, COL_IDS_2, DELETE_7, PARTITION_SPEC_ID_2),
            r("data6", 100L, PARTITION_1, COL_IDS_1, null, PARTITION_SPEC_ID_1),
            r("data7", 200L, PARTITION_3, COL_IDS_3, DELETE_1, PARTITION_SPEC_ID_3),
            r("data7", 200L, PARTITION_3, COL_IDS_3, DELETE_5, PARTITION_SPEC_ID_3));

    RecordSet output =
        rs(
            SystemSchemas.ICEBERG_DELETE_FILE_AGG_SCHEMA,
            r(
                "data1",
                100L,
                PARTITION_1,
                COL_IDS_1,
                li(DELETE_1, DELETE_2, DELETE_3, DELETE_4),
                PARTITION_SPEC_ID_1),
            r("data2", 200L, PARTITION_2, COL_IDS_2, li(DELETE_2), PARTITION_SPEC_ID_2),
            r("data3", 300L, PARTITION_3, COL_IDS_3, li(DELETE_2, DELETE_5), PARTITION_SPEC_ID_3),
            r("data4", 400L, PARTITION_1, COL_IDS_1, li(), PARTITION_SPEC_ID_1),
            r("data5", 500L, PARTITION_2, COL_IDS_2, li(DELETE_6, DELETE_7), PARTITION_SPEC_ID_2),
            r("data6", 100L, PARTITION_1, COL_IDS_1, li(), PARTITION_SPEC_ID_1),
            r("data7", 200L, PARTITION_3, COL_IDS_3, li(DELETE_1, DELETE_5), PARTITION_SPEC_ID_3));

    validateSingle(
        getPop(),
        TableFunctionOperator.class,
        input,
        new RecordBatchValidatorDefaultImpl(output),
        3);
  }

  @Test
  public void testAggregateAcrossMultipleInputBatches() throws Exception {
    RecordSet input =
        rs(
            INPUT_SCHEMA,
            rb(
                r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_1, PARTITION_SPEC_ID_1),
                r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_2, PARTITION_SPEC_ID_1),
                r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_3, PARTITION_SPEC_ID_1)),
            rb(
                r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_4, PARTITION_SPEC_ID_1),
                r("data2", 200L, PARTITION_2, COL_IDS_2, DELETE_2, PARTITION_SPEC_ID_2),
                r("data3", 300L, PARTITION_3, COL_IDS_3, DELETE_2, PARTITION_SPEC_ID_3)),
            rb(
                r("data3", 300L, PARTITION_3, COL_IDS_3, DELETE_5, PARTITION_SPEC_ID_3),
                r("data4", 400L, PARTITION_1, COL_IDS_1, null, PARTITION_SPEC_ID_1)));

    RecordSet output =
        rs(
            SystemSchemas.ICEBERG_DELETE_FILE_AGG_SCHEMA,
            r(
                "data1",
                100L,
                PARTITION_1,
                COL_IDS_1,
                li(DELETE_1, DELETE_2, DELETE_3, DELETE_4),
                PARTITION_SPEC_ID_1),
            r("data2", 200L, PARTITION_2, COL_IDS_2, li(DELETE_2), PARTITION_SPEC_ID_2),
            r("data3", 300L, PARTITION_3, COL_IDS_3, li(DELETE_2, DELETE_5), PARTITION_SPEC_ID_3),
            r("data4", 400L, PARTITION_1, COL_IDS_1, li(), PARTITION_SPEC_ID_1));

    validateSingle(
        getPop(),
        TableFunctionOperator.class,
        input,
        new RecordBatchValidatorDefaultImpl(output),
        3);
  }

  @Test
  public void testAggregateWithEmptyInput() throws Exception {
    RecordSet input = rs(INPUT_SCHEMA);
    validateSingle(
        getPop(), TableFunctionOperator.class, input.toGenerator(getTestAllocator()), null, 3, 0L);
  }

  @Test
  public void testOutputBufferNotReused() throws Exception {
    RecordSet input =
        rs(
            INPUT_SCHEMA,
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_1, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_2, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_3, PARTITION_SPEC_ID_1),
            r("data1", 100L, PARTITION_1, COL_IDS_1, DELETE_4, PARTITION_SPEC_ID_1),
            r("data2", 200L, PARTITION_2, COL_IDS_2, DELETE_2, PARTITION_SPEC_ID_2),
            r("data3", 300L, PARTITION_3, COL_IDS_3, DELETE_2, PARTITION_SPEC_ID_3),
            r("data3", 300L, PARTITION_3, COL_IDS_3, DELETE_5, PARTITION_SPEC_ID_3));

    validateOutputBufferNotReused(getPop(), input, 3);
  }

  private TableFunctionPOP getPop() {
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.ICEBERG_DELETE_FILE_AGG,
            true,
            new TableFunctionContext(
                null,
                SystemSchemas.ICEBERG_DELETE_FILE_AGG_SCHEMA,
                null,
                null,
                null,
                null,
                null,
                null,
                SystemSchemas.ICEBERG_DELETE_FILE_AGG_SCHEMA.getFields().stream()
                    .map(f -> SchemaPath.getSimplePath(f.getName()))
                    .collect(Collectors.toList()),
                null,
                null,
                false,
                false,
                false,
                null)));
  }
}
