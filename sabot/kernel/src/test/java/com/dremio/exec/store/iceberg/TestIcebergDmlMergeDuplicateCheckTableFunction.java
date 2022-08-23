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

import static com.dremio.sabot.Fixtures.NULL_VARCHAR;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.tb;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Collectors;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.base.Strings;

public class TestIcebergDmlMergeDuplicateCheckTableFunction extends BaseTestTableFunction {

  private static final String ID = "id";
  private static final String DATA = "data";

  private static final BatchSchema ICEBERG_TEST_SCHEMA = BatchSchema.newBuilder()
          .addField(Field.nullable(ID, Types.MinorType.INT.getType()))
          .addField(Field.nullable(DATA, Types.MinorType.VARCHAR.getType()))
          .addField(Field.nullable(ColumnUtils.FILE_PATH_COLUMN_NAME, Types.MinorType.VARCHAR.getType()))
          .addField(Field.nullable(ColumnUtils.ROW_INDEX_COLUMN_NAME, Types.MinorType.BIGINT.getType()))
          .build();

  @Test
  public void testSingleRow() throws Exception {
    Table input = t(
            th(ID, DATA, ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME),
            tr(0, "zero", "file_path_1", 0L));

    validateSingle(getPop(), TableFunctionOperator.class, input, input, 5);
  }

  @Test
  public void testTwoRowsDiffFilePaths() throws Exception {
    Table input = t(
            th(ID, DATA, ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME),
            tr(0, "zero", "file_path_1", 0L),
            tr(1, "zero", "file_path_2", 0L));

    validateSingle(getPop(), TableFunctionOperator.class, input, input, 5);
  }

  @Test
  public void testNullFilePath() throws Exception {
    Table input = t(
      th(ID, DATA, ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME),
      tr(0, "zero", "file_path_1", 0L),
      tr(1, "one", NULL_VARCHAR, 1L),
      tr(2, "two", "file_path_1", 1L),
      tr(3, "three", "file_path_1", 3L),
      tr(4, "four", "file_path_1", 4L));

    validateSingle(getPop(), TableFunctionOperator.class, input, input, 5);
  }

  @Test
  public void testSingleBatch() throws Exception {
    Table input = t(
            th(ID, DATA, ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME),
            tr(0, "zero", "file_path_1", 0L),
            tr(1, "one", "file_path_1", 1L),
            tr(2, "two", "file_path_1", 2L),
            tr(3, "three", "file_path_1", 3L),
            tr(4, "four", "file_path_1", 4L));

    validateSingle(getPop(), TableFunctionOperator.class, input, input, 5);
  }

  @Test
  public void testMultipleBatches() throws Exception {
    Table input = t(
            th(ID, DATA, ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME),
            tb(
                    tr(0, "zero", "file_path_1", 0L),
                    tr(1, "one", "file_path_1", 1L),
                    tr(2, "two", "file_path_1", 2L),
                    tr(3, "three", "file_path_1", 3L),
                    tr(4, "four", "file_path_1", 4L)),
            tb(
                    tr(5, "five", "file_path_1", 5L),
                    tr(6, "six", "file_path_1", 6L)));

    Table output = t(
            th(ID, DATA, ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME),
            tr(0, "zero", "file_path_1", 0L),
            tr(1, "one", "file_path_1", 1L),
            tr(2, "two", "file_path_1", 2L),
            tr(3, "three", "file_path_1", 3L),
            tr(4, "four", "file_path_1", 4L),
            tr(5, "five", "file_path_1", 5L),
            tr(6, "six", "file_path_1", 6L));

    validateSingle(getPop(), TableFunctionOperator.class, input, output, 5);
  }

  @Test
  public void testBasicDup() {
    Table input = t(
            th(ID, DATA, ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME),
            tr(0, "zero", "file_path_1", 0L),
            tr(1, "one", "file_path_1", 0L));

    assertProperExceptionThrown(input);
  }

  @Test
  public void testDelayedDup() {
    Table input = t(
            th(ID, DATA, ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME),
            tr(0, "zero", "file_path_1", 0L),
            tr(1, "one", "file_path_1", 1L),
            tr(2, "two", "file_path_1", 2L),
            tr(3, "three", "file_path_1", 2L),
            tr(4, "four", "file_path_1", 4L));

    assertProperExceptionThrown(input);
  }

  @Test
  public void testMultipleBatchesDup() {
    Table input = t(
            th(ID, DATA, ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME),
            tb(
                    tr(0, "zero", "file_path_1", 0L),
                    tr(1, "one", "file_path_1", 1L),
                    tr(2, "two", "file_path_1", 2L),
                    tr(3, "three", "file_path_1", 3L),
                    tr(4, "four", "file_path_1", 4L)),
            tb(
                    tr(5, "five", "file_path_1", 4L),
                    tr(6, "six", "file_path_1", 6L)));

    assertProperExceptionThrown(input);
  }

  @Test
  public void testWithLongFilePath() throws Exception {
    Table input = t(
        th(ID, DATA, ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME),
        tr(0, "zero", "file_path_1-" + Strings.repeat("0", 1000), 0L));

    validateSingle(getPop(), TableFunctionOperator.class, input, input, 5);
  }

  private void assertProperExceptionThrown(Table input) {
    assertThatThrownBy(() -> validateSingle(getPop(), TableFunctionOperator.class, input, null, 5))
      .isInstanceOf(Exception.class)
      .hasMessageContaining("A target row matched more than once. Please update your query.");
  }

  private TableFunctionPOP getPop() {
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.ICEBERG_DML_MERGE_DUPLICATE_CHECK,
            true,
            new TableFunctionContext(null,
                ICEBERG_TEST_SCHEMA,
                null,
                null,
                null,
                null,
                null,
                    ICEBERG_TEST_SCHEMA.getFields().stream()
                    .map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList()),
                null,
                null,
                null,
                false,
                false,
                false,
                null)));
  }
}
