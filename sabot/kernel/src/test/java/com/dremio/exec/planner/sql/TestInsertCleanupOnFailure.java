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
package com.dremio.exec.planner.sql;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.physical.base.IcebergWriterOptions;
import com.dremio.exec.physical.base.TableFormatWriterOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.parquet.ParquetRecordWriter;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.writer.WriterOperator;

public class TestInsertCleanupOnFailure extends BaseTestQuery {

  @Test
  public void testCleanupOnWriterOperator() throws Exception{
    OperatorContext context = mock(OperatorContext.class);
    WriterOptions options = mock(WriterOptions.class);
    RecordWriter recordWriter = mock(RecordWriter.class);
    FileSystem fs = mock(FileSystem.class);
    Path path = mock(Path.class);
    ExecProtos.FragmentHandle handle = mock(ExecProtos.FragmentHandle.class);

    when(options.getTableFormatOptions()).thenReturn(mock(TableFormatWriterOptions.class));
    when(options.getTableFormatOptions().getIcebergSpecificOptions()).thenReturn(mock(IcebergWriterOptions.class));
    when(options.getTableFormatOptions().getIcebergSpecificOptions().getIcebergTableProps()).thenReturn(mock(IcebergTableProps.class));
    when(options.getTableFormatOptions().getIcebergSpecificOptions().getIcebergTableProps().getIcebergOpType()).thenReturn(IcebergCommandType.INSERT);
    when(recordWriter.getFs()).thenReturn(fs);
    when(recordWriter.getLocation()).thenReturn(path);
    when(context.getFragmentHandle()).thenReturn(handle);
    when(handle.getMajorFragmentId()).thenReturn(1);
    when(handle.getMinorFragmentId()).thenReturn(1);
    when(options.getRecordLimit()).thenReturn(100L);

    WriterOperator operator = new WriterOperator(context, options, recordWriter);
    operator.close();

    verify(fs,times(1)).delete(path, true);
  }

  @Test
  public void testCleanupWhenParquetRecordWriterFailsDuringInsert() throws Exception {
    String schema = "c1 int, c2 int, c3 int";
    String tableName = "test_table";
    String values = "(1,1,1),(2,2,2),(3,3,3)";
    String insertTableName = "test_table_insert";
    String ctasQuery = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s) AS VALUES %s", TEMP_SCHEMA, insertTableName, schema, values);
    String createQuery = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s)", TEMP_SCHEMA, tableName, schema);
    runSQL(ctasQuery);
    runSQL(createQuery);
    String sql = String.format("INSERT INTO %s.%s AS SELECT * FROM %s.%s", TEMP_SCHEMA, tableName, TEMP_SCHEMA, insertTableName);

    setupErrorControllerAndTest(sql, tableName);

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
    dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, insertTableName);
    test(dropQuery);
  }

  @Test
  public void testCleanupWhenParquetRecordWriterFailsDuringInsertIntoPartitions() throws Exception {
    String schema = "c1 int, c2 int, c3 int";
    String tableName = "test_table_partition";
    String values = "(1,1,1),(2,2,2),(3,3,3)";
    String insertTableName = "test_table_insert";
    String partitionClause = "(c1, c2)";
    String ctasQuery = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s) AS VALUES %s", TEMP_SCHEMA, insertTableName, schema, values);
    String createQuery = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s) PARTITION BY %s", TEMP_SCHEMA, tableName, schema, partitionClause);
    runSQL(ctasQuery);
    runSQL(createQuery);
    String sql = String.format("INSERT INTO %s.%s AS SELECT * FROM %s.%s", TEMP_SCHEMA, tableName, TEMP_SCHEMA, insertTableName);

    setupErrorControllerAndTest(sql, tableName);

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
    dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, insertTableName);
    test(dropQuery);
  }

  @Test
  public void  testCleanupWhenParquetRecordWriterFailsDuringCopyInto() throws Exception {
    String schema = "c1 int, c2 int, c3 int";
    String tableName = "test_table";
    String fileName = "file1.csv";
    File location = CopyIntoTests.createTempLocation();
    File newSourceFile = CopyIntoTests.createTableAndGenerateSourceFiles(tableName, schema, fileName , location, true);
    String  storageLocation = "\'@" + TEMP_SCHEMA_HADOOP + "/" + location.getName() + "\'";
    String sql = String.format("COPY INTO %s.%s FROM %s files(\'%s\') (RECORD_DELIMITER '\n')", TEMP_SCHEMA, tableName, storageLocation, fileName);

    setupErrorControllerAndTest(sql, tableName);

    Assert.assertTrue(newSourceFile.delete());
    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }

  @Test
  public void  testCleanupWhenParquetRecordWriterFailsDuringCopyIntoWithPartitions() throws Exception {
    String schema = "c1 int, c2 int, c3 int";
    String tableName = "test_table_partition";
    String partitionClause = "(c1, c2)";
    String createQuery = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s) PARTITION BY %s", TEMP_SCHEMA, tableName, schema, partitionClause);
    runSQL(createQuery);
    String fileName = "file1.csv";
    File location = CopyIntoTests.createTempLocation();
    File newSourceFile = CopyIntoTests.createTableAndGenerateSourceFiles(tableName, schema, fileName , location, true);
    String  storageLocation = "\'@" + TEMP_SCHEMA_HADOOP + "/" + location.getName() + "\'";
    String sql = String.format("COPY INTO %s.%s FROM %s files(\'%s\') (RECORD_DELIMITER '\n')", TEMP_SCHEMA, tableName, storageLocation, fileName);

    setupErrorControllerAndTest(sql, tableName);

    Assert.assertTrue(newSourceFile.delete());
    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }

  private void setupErrorControllerAndTest(String sql, String tableName){
    String errorAfterFileWritten = Controls.newBuilder()
      .addException(ParquetRecordWriter.class, ParquetRecordWriter.INJECTOR_AFTER_RECORDS_WRITTEN_ERROR,
        UnsupportedOperationException.class)
      .build();
    ControlsInjectionUtil.setControls(client, errorAfterFileWritten);

    try {
      //this will fail after inserting records
      runSQL(sql);
      Assert.fail("expecting injected exception to be thrown");
    } catch (Exception ex) {}

    File tableLocation = new File(getDfsTestTmpSchemaLocation(), tableName);
    //Check to ensure that the table folder contains only one folder inside it, i.e metadata folder.
    Assert.assertEquals(tableLocation.listFiles().length, 1);
    tableLocation.deleteOnExit();
  }
}
