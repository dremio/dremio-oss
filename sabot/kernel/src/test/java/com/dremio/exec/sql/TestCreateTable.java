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
package com.dremio.exec.sql;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;


public class TestCreateTable extends PlanTestBase {
  @Before
  public void setUp() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG, "true");
  }

  @After
  public void cleanUp() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG,
      ExecConstants.ENABLE_ICEBERG.getDefault().getBoolVal().toString());
  }

  @Test
  public void withInvalidColumnTypes() throws Exception {
    errorMsgTestHelper(String.format("CREATE TABLE %s.%s (region inte, region_id int)", TEMP_SCHEMA, "testTableName1"),
        "Invalid column type [`inte`]");
  }

  @Test
  public void withDuplicateColumnsInDef1() throws Exception {
    errorMsgTestHelper(String.format("CREATE TABLE %s.%s (region_id int, region_id int)", TEMP_SCHEMA, "testTableName2"),
        "Column [REGION_ID] specified multiple times.");
  }

  @Test
  public void withDuplicateColumnsInDef2() throws Exception {
    errorMsgTestHelper(String.format("CREATE TABLE %s.%s (region_id int, sales_city varchar, sales_city varchar)",
        TEMP_SCHEMA, "testTableName3"),"Column [SALES_CITY] specified multiple times.");
  }

  @Test
  public void createTableInvalidType() throws Exception {
    errorMsgTestHelper(String.format("CREATE TABLE %s.%s (region_id int, sales_city Decimal(39, 4))",
        TEMP_SCHEMA, "testTableName4"),"Precision larger than 38 is not supported.");
  }

  @Test
  public void createTableWithVarcharPrecision() throws Exception {
    final String newTblName = "createTableVarcharPrecision";
    try {
      final String ctasQuery =
          String.format("create table %s.%s ( VOTER_ID INT, NAME VARCHAR(40), AGE INT, REGISTRATION CHAR(51), " +
                  "CONTRIBUTIONS float, VOTERZONE INT, CREATE_TIMESTAMP TIMESTAMP, create_date date) partition by(AGE)",
              TEMP_SCHEMA, newTblName);
      test(ctasQuery);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void createTableAndSelect() throws Exception {
    final String newTblName = "createEmptyTable";

    try {
      final String ctasQuery =
        String.format("CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3))", TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String describeCreatedTable = String.format("describe %s.%s", TEMP_SCHEMA, newTblName);
      testBuilder()
        .sqlQuery(describeCreatedTable)
        .unOrdered()
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "NUMERIC_PRECISION", "NUMERIC_SCALE")
        .baselineValues("id", "INTEGER", "YES", 32, 0)
        .baselineValues("name", "CHARACTER VARYING", "YES", null, null)
        .baselineValues("distance", "DECIMAL", "YES", 38, 3)
        .build()
        .run();

      final String selectFromCreatedTable = String.format("select count(*) as cnt from %s.%s", TEMP_SCHEMA, newTblName);
      testBuilder()
        .sqlQuery(selectFromCreatedTable)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(0L)
        .build()
        .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void createTableWhenATableWithSameNameAlreadyExists() throws Exception{
    final String newTblName = "createTableWhenTableAlreadyExists";

    try {
      final String ctasQuery =
          String.format("CREATE TABLE %s.%s (id int, name varchar)", TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      errorMsgTestHelper(ctasQuery,
          String.format("A table or view with given name [%s.%s] already exists", TEMP_SCHEMA, newTblName));
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void createEmptyTablePartitionWithEmptyList() throws Exception {
    final String newTblName = "ctasPartitionWithEmptyList";
    final String ctasQuery = String.format("CREATE TABLE %s.%s (id int, name varchar) PARTITION BY ", TEMP_SCHEMA, newTblName);

    try {
      errorTypeTestHelper(ctasQuery, ErrorType.PARSE);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void testDroppingOfMapTypeColumn() throws Exception{
    String table1 = "iceberg_map_test";
    try {
      File table1Folder = new File(getDfsTestTmpSchemaLocation(), table1);
      HadoopTables hadoopTables = new HadoopTables(new Configuration());

      Schema schema = new Schema(
        Types.NestedField.optional(1, "col1", Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StringType.get())),
        Types.NestedField.optional(2, "col2", Types.IntegerType.get())
      );
      PartitionSpec spec = PartitionSpec
        .builderFor(schema)
        .build();
      Table table = hadoopTables.create(schema, spec, table1Folder.getPath());
      Transaction transaction = table.newTransaction();
      AppendFiles appendFiles = transaction.newAppend();
      final String testWorkingPath = TestTools.getWorkingPath() + "/src/test/resources/iceberg/mapTest";
      final String parquetFile = "iceberg_map_test.parquet";
      File dataFile = new File(testWorkingPath, parquetFile);
      appendFiles.appendFile(
        DataFiles.builder(spec)
          .withInputFile(Files.localInput(dataFile))
          .withRecordCount(1)
          .withFormat(FileFormat.PARQUET)
          .build()
      );
      appendFiles.commit();
      transaction.commitTransaction();

      testBuilder()
        .sqlQuery("select * from dfs_test.iceberg_map_test")
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .build()
        .run();

      Thread.sleep(1001);
      String insertCommandSql = "insert into  dfs_test.iceberg_map_test select * from (values(2))";
      test(insertCommandSql);
      Thread.sleep(1001);

      testBuilder()
        .sqlQuery("select * from dfs_test.iceberg_map_test")
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .baselineValues(2)
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), table1));
    }
  }
}
