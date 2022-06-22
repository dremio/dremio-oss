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

import static com.dremio.exec.store.SystemSchemas.AGG_DELETEFILE_PATHS;
import static com.dremio.exec.store.SystemSchemas.COL_IDS;
import static com.dremio.exec.store.SystemSchemas.DATAFILE_PATH;
import static com.dremio.exec.store.SystemSchemas.DELETEFILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_SIZE;
import static com.dremio.exec.store.SystemSchemas.PARTITION_INFO;
import static com.dremio.sabot.Fixtures.NULL_VARCHAR;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.tb;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static com.dremio.sabot.Fixtures.varCharList;

import java.util.stream.Collectors;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.Generator;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;

public class TestIcebergDeleteFileAggTableFunction extends BaseTestTableFunction {

  private static final byte[] PARTITION_1 = new byte[]{10};
  private static final byte[] PARTITION_2 = new byte[]{20};
  private static final byte[] PARTITION_3 = new byte[]{30};

  private static final byte[] COL_IDS_1 = new byte[]{1};
  private static final byte[] COL_IDS_2 = new byte[]{2};
  private static final byte[] COL_IDS_3 = new byte[]{3};

  @Test
  public void testBasicAgg() throws Exception {

    Table input = t(
        th(DATAFILE_PATH, FILE_SIZE, PARTITION_INFO, COL_IDS, DELETEFILE_PATH),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete1"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete2"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete3"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete4"),
        tr("data2", 200L, PARTITION_2, COL_IDS_2, "delete2"),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, "delete2"),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, "delete5"));

    Table output = t(
        th(DATAFILE_PATH, FILE_SIZE, PARTITION_INFO, COL_IDS, AGG_DELETEFILE_PATHS),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, varCharList("delete1", "delete2", "delete3", "delete4")),
        tr("data2", 200L, PARTITION_2, COL_IDS_2, varCharList("delete2")),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, varCharList("delete2", "delete5")));

    validateSingle(getPop(), TableFunctionOperator.class, input, output, 3);
  }

  @Test
  public void testNullDeleteFilePath() throws Exception {

    Table input = t(
        th(DATAFILE_PATH, FILE_SIZE, PARTITION_INFO, COL_IDS, DELETEFILE_PATH),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete1"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete2"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete3"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete4"),
        tr("data2", 200L, PARTITION_2, COL_IDS_2, "delete2"),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, "delete2"),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, "delete5"),
        tr("data4", 400L, PARTITION_1, COL_IDS_1, NULL_VARCHAR));

    Table output = t(
        th(DATAFILE_PATH, FILE_SIZE, PARTITION_INFO, COL_IDS, AGG_DELETEFILE_PATHS),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, varCharList("delete1", "delete2", "delete3", "delete4")),
        tr("data2", 200L, PARTITION_2, COL_IDS_2, varCharList("delete2")),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, varCharList("delete2", "delete5")),
        tr("data4", 400L, PARTITION_1, COL_IDS_1, varCharList()));

    validateSingle(getPop(), TableFunctionOperator.class, input, output, 3);
  }

  @Test
  public void testAggregateWithMultipleOutputBathes() throws Exception {
    Table input = t(
        th(DATAFILE_PATH, FILE_SIZE, PARTITION_INFO, COL_IDS, DELETEFILE_PATH),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete1"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete2"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete3"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete4"),
        tr("data2", 200L, PARTITION_2, COL_IDS_2, "delete2"),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, "delete2"),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, "delete5"),
        tr("data4", 400L, PARTITION_1, COL_IDS_1, NULL_VARCHAR),
        tr("data5", 500L, PARTITION_2, COL_IDS_2, "delete6"),
        tr("data5", 500L, PARTITION_2, COL_IDS_2, "delete7"),
        tr("data6", 100L, PARTITION_1, COL_IDS_1, NULL_VARCHAR),
        tr("data7", 200L, PARTITION_3, COL_IDS_3, "delete1"),
        tr("data7", 200L, PARTITION_3, COL_IDS_3, "delete5"));

    Table output = t(
        th(DATAFILE_PATH, FILE_SIZE, PARTITION_INFO, COL_IDS, AGG_DELETEFILE_PATHS),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, varCharList("delete1", "delete2", "delete3", "delete4")),
        tr("data2", 200L, PARTITION_2, COL_IDS_2, varCharList("delete2")),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, varCharList("delete2", "delete5")),
        tr("data4", 400L, PARTITION_1, COL_IDS_1, varCharList()),
        tr("data5", 500L, PARTITION_2, COL_IDS_2, varCharList("delete6", "delete7")),
        tr("data6", 100L, PARTITION_1, COL_IDS_1, varCharList()),
        tr("data7", 200L, PARTITION_3, COL_IDS_3, varCharList("delete1", "delete5")));

    validateSingle(getPop(), TableFunctionOperator.class, input, output, 3);
  }

  @Test
  public void testAggregateAcrossMultipleInputBatches() throws Exception {
    Table input = t(
        th(DATAFILE_PATH, FILE_SIZE, PARTITION_INFO, COL_IDS, DELETEFILE_PATH),
        tb(
            tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete1"),
            tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete2"),
            tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete3")),
        tb(
            tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete4"),
            tr("data2", 200L, PARTITION_2, COL_IDS_2, "delete2"),
            tr("data3", 300L, PARTITION_3, COL_IDS_3, "delete2")),
        tb(
            tr("data3", 300L, PARTITION_3, COL_IDS_3, "delete5"),
            tr("data4", 400L, PARTITION_1, COL_IDS_1, NULL_VARCHAR)));

    Table output = t(
        th(DATAFILE_PATH, FILE_SIZE, PARTITION_INFO, COL_IDS, AGG_DELETEFILE_PATHS),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, varCharList("delete1", "delete2", "delete3", "delete4")),
        tr("data2", 200L, PARTITION_2, COL_IDS_2, varCharList("delete2")),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, varCharList("delete2", "delete5")),
        tr("data4", 400L, PARTITION_1, COL_IDS_1, varCharList()));

    validateSingle(getPop(), TableFunctionOperator.class, input, output, 3);
  }

  @Test
  public void testAggregateWithEmptyInput() throws Exception {
    validateSingle(getPop(), TableFunctionOperator.class, emptyInputBatchGenerator(), null, 3, 0L);
  }

  @Test
  public void testOutputBufferNotReused() throws Exception {

    Table input = t(
        th(DATAFILE_PATH, FILE_SIZE, PARTITION_INFO, COL_IDS, DELETEFILE_PATH),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete1"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete2"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete3"),
        tr("data1", 100L, PARTITION_1, COL_IDS_1, "delete4"),
        tr("data2", 200L, PARTITION_2, COL_IDS_2, "delete2"),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, "delete2"),
        tr("data3", 300L, PARTITION_3, COL_IDS_3, "delete5"));

    validateOutputBufferNotReused(getPop(), input, 3);
  }

  private TableFunctionPOP getPop() {
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.ICEBERG_DELETE_FILE_AGG,
            true,
            new TableFunctionContext(null,
                SystemSchemas.ICEBERG_DELETE_FILE_AGG_SCHEMA,
                null,
                null,
                null,
                null,
                null,
                SystemSchemas.ICEBERG_DELETE_FILE_AGG_SCHEMA.getFields().stream()
                    .map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList()),
                null,
                null,
                null,
                false,
                false,
                false,
                null)));
  }

  private Generator emptyInputBatchGenerator() {
    return new Generator() {
      private final VectorContainer container = new VectorContainer(getTestAllocator());

      {
        container.addOrGet(Field.nullable(DATAFILE_PATH, Types.MinorType.VARCHAR.getType()));
        container.addOrGet(Field.nullable(FILE_SIZE, Types.MinorType.BIGINT.getType()));
        container.addOrGet(Field.nullable(PARTITION_INFO, Types.MinorType.VARBINARY.getType()));
        container.addOrGet(Field.nullable(COL_IDS, Types.MinorType.VARBINARY.getType()));
        container.addOrGet(Field.nullable(DELETEFILE_PATH, Types.MinorType.VARCHAR.getType()));
        container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      }

      @Override
      public VectorAccessible getOutput() {
        return container;
      }

      @Override
      public int next(int records) {
        return 0;
      }

      @Override
      public void close() throws Exception {
        AutoCloseables.close(container);
      }
    };
  }
}
