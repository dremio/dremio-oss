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

import static com.dremio.exec.store.RecordWriter.FILESIZE_COLUMN;
import static com.dremio.exec.store.RecordWriter.FRAGMENT_COLUMN;
import static com.dremio.exec.store.RecordWriter.OPERATION_TYPE_COLUMN;
import static com.dremio.exec.store.RecordWriter.PATH_COLUMN;
import static com.dremio.exec.store.RecordWriter.RECORDS_COLUMN;
import static com.dremio.exec.util.ColumnUtils.FILE_PATH_COLUMN_NAME;
import static com.dremio.exec.util.ColumnUtils.ROW_COUNT_COLUMN_NAME;
import static com.dremio.sabot.Fixtures.NULL_VARCHAR;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.DeletedFilesMetadataTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class TestDeletedFilesMetadataTableFunction extends BaseTestTableFunction {

  // using a subset of fields here as any other fields are exposed as null columns, Fragment is
  // included as
  // a test of the null column handling
  private static final BatchSchema TEST_SCHEMA =
      RecordWriter.SCHEMA
          .subset(
              ImmutableList.of(
                  FRAGMENT_COLUMN,
                  RECORDS_COLUMN,
                  PATH_COLUMN,
                  OPERATION_TYPE_COLUMN,
                  FILESIZE_COLUMN))
          .get();

  private static final TableFunctionPOP TABLE_FUNCTION_POP =
      new TableFunctionPOP(
          PROPS,
          null,
          new TableFunctionConfig(
              TableFunctionConfig.FunctionType.DELETED_FILES_METADATA,
              true,
              new DeletedFilesMetadataTableFunctionContext(
                  OperationType.DELETE_DATAFILE,
                  null,
                  TEST_SCHEMA,
                  null,
                  null,
                  null,
                  null,
                  null,
                  TEST_SCHEMA.getFields().stream()
                      .map(f -> SchemaPath.getSimplePath(f.getName()))
                      .collect(ImmutableList.toImmutableList()),
                  null,
                  null,
                  false,
                  false,
                  true,
                  null)));

  private static final TableFunctionPOP DELETE_TABLE_FUNCTION_POP =
      new TableFunctionPOP(
          PROPS,
          null,
          new TableFunctionConfig(
              TableFunctionConfig.FunctionType.DELETED_FILES_METADATA,
              true,
              new DeletedFilesMetadataTableFunctionContext(
                  OperationType.DELETE_DELETEFILE,
                  null,
                  TEST_SCHEMA,
                  null,
                  null,
                  null,
                  null,
                  null,
                  TEST_SCHEMA.getFields().stream()
                      .map(f -> SchemaPath.getSimplePath(f.getName()))
                      .collect(ImmutableList.toImmutableList()),
                  null,
                  null,
                  false,
                  false,
                  true,
                  null)));

  @Test
  public void testResults() throws Exception {

    Table input =
        t(
            th(FILE_PATH_COLUMN_NAME, ROW_COUNT_COLUMN_NAME),
            tr("path1", 20L),
            tr("path2", 5L),
            tr("path3", 10L),
            tr("path4", 3L),
            tr("path5", 9L));

    Table output =
        t(
            th(
                FRAGMENT_COLUMN,
                RECORDS_COLUMN,
                PATH_COLUMN,
                OPERATION_TYPE_COLUMN,
                FILESIZE_COLUMN),
            tr(NULL_VARCHAR, 20L, "path1", OperationType.DELETE_DATAFILE.value, 0L),
            tr(NULL_VARCHAR, 5L, "path2", OperationType.DELETE_DATAFILE.value, 0L),
            tr(NULL_VARCHAR, 10L, "path3", OperationType.DELETE_DATAFILE.value, 0L),
            tr(NULL_VARCHAR, 3L, "path4", OperationType.DELETE_DATAFILE.value, 0L),
            tr(NULL_VARCHAR, 9L, "path5", OperationType.DELETE_DATAFILE.value, 0L));

    Table deleteOutput =
        t(
            th(
                FRAGMENT_COLUMN,
                RECORDS_COLUMN,
                PATH_COLUMN,
                OPERATION_TYPE_COLUMN,
                FILESIZE_COLUMN),
            tr(NULL_VARCHAR, 20L, "path1", OperationType.DELETE_DELETEFILE.value, 0L),
            tr(NULL_VARCHAR, 5L, "path2", OperationType.DELETE_DELETEFILE.value, 0L),
            tr(NULL_VARCHAR, 10L, "path3", OperationType.DELETE_DELETEFILE.value, 0L),
            tr(NULL_VARCHAR, 3L, "path4", OperationType.DELETE_DELETEFILE.value, 0L),
            tr(NULL_VARCHAR, 9L, "path5", OperationType.DELETE_DELETEFILE.value, 0L));

    validateSingle(TABLE_FUNCTION_POP, TableFunctionOperator.class, input, output, 3);
    validateSingle(DELETE_TABLE_FUNCTION_POP, TableFunctionOperator.class, input, deleteOutput, 3);
  }

  @Test
  public void testOutputBufferNotReused() throws Exception {

    Table input =
        t(
            th(FILE_PATH_COLUMN_NAME, ROW_COUNT_COLUMN_NAME),
            tr("path1", 20L),
            tr("path2", 5L),
            tr("path3", 10L),
            tr("path4", 3L),
            tr("path5", 9L));

    validateOutputBufferNotReused(TABLE_FUNCTION_POP, input, 3);
    validateOutputBufferNotReused(DELETE_TABLE_FUNCTION_POP, input, 3);
  }
}
