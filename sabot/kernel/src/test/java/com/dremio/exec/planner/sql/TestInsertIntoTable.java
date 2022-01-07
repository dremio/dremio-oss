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

import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;

import java.io.File;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.config.DremioConfig;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.SqlInsertTable;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.collect.Sets;

public class TestInsertIntoTable extends BaseTestQuery {

  private ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  private void simpleInsertCommandTest() {
    String sql = "INSERT INTO tblName select * from sourceTable";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.INSERT)));
    SqlInsertTable sqlInsertTable = (SqlInsertTable) sqlNode;

    String query = "select * from sourceTable";
    SqlNode querySqlNode = SqlConverter.parseSingleStatementImpl(query, parserConfig, false);

    Assert.assertEquals(sqlInsertTable.getQuery().toString(), querySqlNode.toString());
    Assert.assertEquals("tblName", sqlInsertTable.getTblName().toString());
  }

  @Test
  public void testSimpleInsertCommand() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      simpleInsertCommandTest();
    }
  }

  private void testInsertCommandInvalidPath(String tblName, String schema) throws Exception {
    try {
      final String createTableQuery = String.format("CREATE TABLE %s.%s(id int, code int)",
        schema, tblName);
      test(createTableQuery);

      Thread.sleep(1001);

      expectedEx.expect(UserException.class);
      expectedEx.expectMessage(String.format("Table [%s] not found", tblName));
      final String insertQuery = String.format("INSERT INTO %s SELECT n_nationkey id, n_regionkey CODE " +
          "from cp.\"tpch/nation.parquet\"",
        tblName);
      test(insertQuery);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testInsertCommandInvalidPath() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testInsertCommandInvalidPath("invalid_path_test", TEMP_SCHEMA_HADOOP);
      testInsertCommandInvalidPath("invalid_path_test_v2", TEMP_SCHEMA);
    }
  }

  @Test
  public void testInsertIntoSysTable() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String insertQuery = "INSERT INTO INFORMATION_SCHEMA.CATALOGS VALUES('A', 'A', 'A')";
      errorMsgTestHelper(insertQuery, "[INFORMATION_SCHEMA.CATALOGS] is a SYSTEM_TABLE");
    }
  }

  private void testInsertIntoView(String tblName, String schema) throws Exception {
    try {
      properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
      try {
        final String createTableQuery = String.format("CREATE or REPLACE VIEW %s.%s as select * from INFORMATION_SCHEMA.CATALOGS",
          schema, tblName);
        test(createTableQuery);

        Thread.sleep(1001);

        final String insertQuery = String.format("INSERT INTO %s.%s VALUES('A', 'A', 'A')", schema, tblName);
        errorMsgTestHelper(insertQuery, String.format("[%s.%s] is a VIEW", schema, tblName));
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
      }
    } finally {
      properties.clear(DremioConfig.LEGACY_STORE_VIEWS_ENABLED);
    }
  }

  @Test
  public void testInsertIntoView() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testInsertIntoView("sysverview", TEMP_SCHEMA_HADOOP);
      testInsertIntoView("sysverview_v2", TEMP_SCHEMA);
    }
  }

  private void testInsertDuplicateColumnsInSelect(String table, String schema) throws Exception {
    try {
      final String tableCreate = String.format("CREATE TABLE %s.%s(id int, code int)",
        schema, table);
      test(tableCreate);

      Thread.sleep(1001);

      final String insertIntoTable = String.format("INSERT INTO %s.%s SELECT n_nationkey, n_nationkey " +
              "from cp.\"tpch/nation.parquet\"",
        schema, table);
      test(insertIntoTable);

      testBuilder()
          .sqlQuery(String.format("select ID, CODE from %s.%s", schema, table))
          .unOrdered()
          .sqlBaselineQuery("SELECT n_nationkey ID, n_nationkey CODE from cp.\"tpch/nation.parquet\"")
          .build()
          .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), table));
    }
  }

  @Test
  public void testInsertDuplicateColumnsInSelect() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testInsertDuplicateColumnsInSelect("dupColsInSelect", TEMP_SCHEMA_HADOOP);
      testInsertDuplicateColumnsInSelect("dupColsInSelect_v2", TEMP_SCHEMA);
    }
  }

  private void testInsertColumnNameCaseMismatch(String tableLower, String schema) throws Exception {
    try {
      final String tableLowerCreate = String.format("CREATE TABLE %s.%s(id int, code int) partition by (code)",
        schema, tableLower);
      test(tableLowerCreate);

      Thread.sleep(1001);

      final String insertUpperIntoLower = String.format("INSERT INTO %s.%s SELECT n_nationkey id, n_regionkey CODE " +
          "from cp.\"tpch/nation.parquet\"",
        schema, tableLower);
      test(insertUpperIntoLower);

      testBuilder()
        .sqlQuery(String.format("select ID, CODE from %s.%s", schema, tableLower))
        .unOrdered()
        .sqlBaselineQuery("SELECT n_nationkey ID, n_regionkey CODE from cp.\"tpch/nation.parquet\"")
        .build()
        .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableLower));
    }
  }

  @Test
  public void testInsertColumnNameCaseMismatch() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testInsertColumnNameCaseMismatch("tableLower", TEMP_SCHEMA_HADOOP);
      testInsertColumnNameCaseMismatch("tableLower_v2", TEMP_SCHEMA);
    }
  }

  private void testInsertIntoPartitionedTable(String table1, String table2, String schema, IcebergCatalogType catalogType) throws Exception {
    try {
      final String table1Create = String.format("CREATE TABLE %s.%s(id int, code int) partition by (code)",
        schema, table1);
      test(table1Create);

      // table2 partitioned by n_regionkey
      final String table2CTAS = String.format("CREATE TABLE %s.%s partition by(n_regionkey) " +
          " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 10",
        schema, table2);
      test(table2CTAS);

      Thread.sleep(1001);

      // insert into table1 which is partitioned by n_nationkey values
      final String insertIntoTable1 = String.format("INSERT INTO %s.%s SELECT n_regionkey id , n_nationkey code from %s.%s",
        schema, table1, schema, table2);
      test(insertIntoTable1);

      Thread.sleep(1001);

      // same data in table1 and table2
      testBuilder()
        .sqlQuery(String.format("select id, code from %s.%s", schema, table1))
        .unOrdered()
        .baselineColumns("id", "code")
        .sqlBaselineQuery(String.format("SELECT n_regionkey id, n_nationkey code from %s.%s", schema, table2))
        .build()
        .run();

      List<Integer> valuesInCodeColumn = IntStream.range(0, 10).boxed().collect(Collectors.toList());
      testBuilder()
        .sqlQuery(String.format("select code from %s.%s", schema, table1))
        .unOrdered()
        .baselineColumns("code")
        .baselineValues(0)
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .baselineValues(5)
        .baselineValues(6)
        .baselineValues(7)
        .baselineValues(8)
        .baselineValues(9)
        .build()
        .run();

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), table1);
      Table icebergTable1 = getIcebergTable(tableFolder, catalogType);

      List<Integer> partitionValues = StreamSupport.stream(icebergTable1.newScan().planFiles().spliterator(), false)
        .map(fileScanTask -> fileScanTask.file().partition())
        .map(structLike -> structLike.get(0, Integer.class))
        .collect(Collectors.toList());

      Assert.assertEquals(valuesInCodeColumn.size(), partitionValues.size());
      Assert.assertTrue(partitionValues.containsAll(valuesInCodeColumn));

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), table1));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), table2));
    }
  }

  @Test
  public void testInsertIntoPartitionedTable() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testInsertIntoPartitionedTable("table1", "table2", TEMP_SCHEMA, IcebergCatalogType.NESSIE);
      testInsertIntoPartitionedTable("table1_v2", "table2_v2", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
    }
  }

  private void testInsertUsingValues(String insert_values_test, String schema) throws Exception {
    try {
      String createCommandSql = "create table " + schema + "." + insert_values_test +
        "(PolicyID bigint, statecode varchar, county varchar, " +
        "eq_site_limit double, hu_site_limit double, fl_site_limit double, " +
        "fr_site_limit double, tiv_2011 double, tiv_2012 double, " +
        "eq_site_deductible varchar, hu_site_deductible varchar, " +
        "fl_site_deductible varchar, fr_site_dedu varchar)";
      test(createCommandSql);
      Thread.sleep(1001);
      String insertCommandSql = "insert into " + schema + "." + insert_values_test +
        " SELECT * FROM (VALUES(" +
        "cast(1 as bigint), " +
        "cast('CT' as varchar), " +
        "cast('Conn' as varchar), " +
        "cast(0 as double), " +
        "cast(0 as double), cast(0 as double), cast(0 as double), " +
        "cast(0 as double), cast(0 as double), cast('0' as varchar), " +
        "cast('0' as varchar), cast('0' as varchar), cast('0' as varchar)  ))";
      test(insertCommandSql);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s", schema, insert_values_test))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(1L)
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), insert_values_test));
    }
  }

  @Test
  public void testInsertUsingValues() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testInsertUsingValues("insert_values_test", TEMP_SCHEMA_HADOOP);
      testInsertUsingValues("insert_values_test_v2", TEMP_SCHEMA);
    }
  }

  private void testInsertUsingValuesWithSchemaMismatch(String insert_values_test, String schema) throws Exception {
    try {
      String createCommandSql = "create table " + schema + "." + insert_values_test +
        "(PolicyID bigint, statecode varchar, county varchar, " +
        "eq_site_limit double, hu_site_limit double, fl_site_limit double, " +
        "fr_site_limit double, tiv_2011 double, tiv_2012 double, " +
        "eq_site_deductible varchar, hu_site_deductible varchar, " +
        "fl_site_deductible varchar, fr_site_dedu varchar)";
      test(createCommandSql);
      Thread.sleep(1001);
      String insertCommandSql = "insert into " + schema + "." + insert_values_test +
        " SELECT * FROM (VALUES(" +
        "cast(1 as int), 'CT', 'Conn', cast(0 as double), " +
        "cast(0 as double), cast(0 as double), cast(0 as double), " +
        "cast(0 as float), cast(0 as double), '0', '0', '0', '0'  ))";
      test(insertCommandSql);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s", schema, insert_values_test))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(1L)
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), insert_values_test));
    }
  }

  @Test
  public void testInsertUsingValuesWithSchemaMismatch() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testInsertUsingValuesWithSchemaMismatch("insert_values_test_mismatch", TEMP_SCHEMA_HADOOP);
      testInsertUsingValuesWithSchemaMismatch("insert_values_test_mismatch_v2", TEMP_SCHEMA);
    }
  }

  private void testIncomptibleStringToInt(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable +
        "(col1 int)";
      test(table1);
      String insert = "insert into " + schema + "." + newTable + " select * from (values('abcd'))";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage(String.format("Table schema(col1::int32) doesn't match with query schema(EXPR$0::varchar"));
      test(insert);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void testIncomptibleStringToInt() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testIncomptibleStringToInt("insert_incompatible_test", TEMP_SCHEMA_HADOOP);
      testIncomptibleStringToInt("insert_incompatible_test_v2", TEMP_SCHEMA);
    }
  }

  private void testUpPromotableInsert(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 boolean, zcol3 date, zcol4 float, zcol5 decimal(10,3), " +
        "zcol6 double, zcol7 int, zcol8 bigint, zcol9 time, zcol10 timestamp, zcol11 varchar)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(true, '2019-10-27', cast(0.3 as float), cast(12345.34 as decimal(7,2)), " +
        "cast(3.6 as double), 1, 123456, '12:00:34', 1230768000000, 'abcd'))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col1 bigint, col2 decimal(20,5), col3 double)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select zcol7, zcol5, zcol4 from " + schema + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + schema + "." + newTable + "_2")
        .baselineColumns("col1", "col2", "col3")
        .baselineValues(new Long("1"), new BigDecimal("12345.340"), new Double("0.3"))
        .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testUpPromotableInsert() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testUpPromotableInsert("insert_uppromotable_test", TEMP_SCHEMA_HADOOP);
      testUpPromotableInsert("insert_uppromotable_test_v2", TEMP_SCHEMA);
    }
  }

  private void testListComplexInsert(String newTable, String dfsSchema, String testSchema) throws Exception {
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String listbigint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/listbigint";

      final String createListBigInt = String.format("CREATE TABLE %s.%s_1  " +
          " AS SELECT col1 as listcol1 from " + dfsSchema + ".\"" + listbigint +"\"",
        testSchema, newTable);

      test(createListBigInt);
      Thread.sleep(1001);

      final String createListInt = String.format("CREATE TABLE %s.%s_2  " +
          " AS SELECT col1 as listcol2 from " + dfsSchema + ".\"" + listbigint +"\"",
        testSchema, newTable);

      test(createListInt);
      Thread.sleep(1001);

      final String insertQuery = String.format("insert into %s.%s_1 select listcol2 from %s.%s_2",
        testSchema, newTable, testSchema, newTable);
      test(insertQuery);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s_1", testSchema, newTable))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(2L)
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testListComplexInsert() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testListComplexInsert("insert_list_test", "dfs", TEMP_SCHEMA);
      testListComplexInsert("insert_list_test_v2", "dfs_hadoop", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testListComplexInsertFailure(String newTable, String dfsSchema, String testSchema) throws Exception {
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String listbigint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/listbigint";
      final String listint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/listint";

      final String createListBigInt = String.format("CREATE TABLE %s.%s_1  " +
          " AS SELECT col1 as listcol1 from " + dfsSchema + ".\"" + listbigint +"\"",
        testSchema, newTable);

      test(createListBigInt);
      Thread.sleep(1001);

      final String createListInt = String.format("CREATE TABLE %s.%s_2  " +
          " AS SELECT col1 as listcol2 from " + dfsSchema + ".\"" + listint +"\"",
        testSchema, newTable);

      test(createListInt);
      Thread.sleep(1001);

      final String insertQuery = String.format("insert into %s.%s_1 select listcol2 from %s.%s_2",
        testSchema, newTable, testSchema, newTable);
      expectedEx.expect(UserException.class);
      String expected = isComplexTypeSupport() ? "Table schema(listcol1::list<int64>) doesn't match with query schema(listcol2::list<int32>)" : "Table schema(listcol1::list<int64>) doesn't match with query schema(listcol1::list<int32>)";
      expectedEx.expectMessage(String.format(expected));
      test(insertQuery);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testListComplexInsertFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testListComplexInsertFailure("insert_list_failure_test", "dfs", TEMP_SCHEMA);
      testListComplexInsertFailure("insert_list_failure_test_v2", "dfs_hadoop", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testStructComplexInsert(String newTable, String dfsSchema, String testSchema) throws Exception {
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String structbigint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/structbigint";

      final String createStructBigInt = String.format("CREATE TABLE %s.%s_1  " +
          " AS SELECT col1 as structcol1 from " + dfsSchema + ".\"" + structbigint +"\"",
        testSchema, newTable);

      test(createStructBigInt);
      Thread.sleep(1001);

      final String createStructInt = String.format("CREATE TABLE %s.%s_2  " +
          " AS SELECT col1 as structcol2 from " + dfsSchema + ".\"" + structbigint +"\"",
        testSchema, newTable);

      test(createStructInt);
      Thread.sleep(1001);

      final String insertQuery = String.format("insert into %s.%s_1 select structcol2 from %s.%s_2",
        testSchema, newTable, testSchema, newTable);
      test(insertQuery);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s_1", testSchema, newTable))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(2L)
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testStructComplexInsert() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testStructComplexInsert("insert_struct_test", "dfs", TEMP_SCHEMA);
      testStructComplexInsert("insert_struct_test_v2", "dfs_hadoop", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testStructComplexInsertFailure(String newTable, String dfsSchema, String testSchema) throws Exception {
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String structbigint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/structbigint";
      final String structint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/structint";

      final String createStructBigInt = String.format("CREATE TABLE %s.%s_1  " +
          " AS SELECT col1 as structcol1 from " + dfsSchema + ".\"" + structbigint +"\"",
        testSchema, newTable);

      test(createStructBigInt);
      Thread.sleep(1001);

      final String createStructInt = String.format("CREATE TABLE %s.%s_2  " +
          " AS SELECT col1 as structcol2 from " + dfsSchema + ".\"" + structint +"\"",
        testSchema, newTable);

      test(createStructInt);
      Thread.sleep(1001);

      final String insertQuery = String.format("insert into %s.%s_1 select structcol2 from %s.%s_2",
        testSchema, newTable, testSchema, newTable);
      expectedEx.expect(UserException.class);
      String expected = isComplexTypeSupport() ? "Table schema(structcol1::struct<name::varchar, age::int64>) doesn't match with query schema(structcol2::struct<name::varchar, age::int32>)" : "schema(structcol1::struct<name::varchar, age::int64>) doesn't match with query schema(structcol1::struct<name::varchar, age::int32>)";
      expectedEx.expectMessage(String.format(expected));
      test(insertQuery);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testStructComplexInsertFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testStructComplexInsertFailure("insert_struct_failure_test", "dfs", TEMP_SCHEMA);
      testStructComplexInsertFailure("insert_struct_failure_test_v2", "dfs_hadoop", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testComplexInsertIncompatibleFailure(String newTable, String dfsSchema, String testSchema) throws Exception {
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String structbigint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/structbigint";
      final String structint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/structint";

      final String createStructBigInt = String.format("CREATE TABLE %s.%s_1  " +
          " AS SELECT col1 as structcol1 from " + dfsSchema + ".\"" + structbigint +"\"",
        testSchema, newTable);

      test(createStructBigInt);
      Thread.sleep(1001);

      final String createStructInt = String.format("CREATE TABLE %s.%s_2  " +
          " AS SELECT col1 as structcol2 from " + dfsSchema + ".\"" + structint +"\"",
        testSchema, newTable);

      test(createStructInt);
      Thread.sleep(1001);

      final String insertQuery = String.format("insert into %s.%s_1 select %s_2.structcol2.name from %s.%s_2",
        testSchema, newTable, newTable, testSchema, newTable);
      expectedEx.expect(UserException.class);
      String expected = isComplexTypeSupport() ? "Table schema(structcol1::struct<name::varchar, age::int64>) doesn't match with query schema(name::varchar)" : "Table schema(structcol1::struct<name::varchar, age::int64>) doesn't match with query schema(structcol1::varchar)";
      expectedEx.expectMessage(String.format(expected));
      test(insertQuery);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testComplexInsertIncompatibleFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testComplexInsertIncompatibleFailure("insert_struct_incompatible_test", "dfs", TEMP_SCHEMA);
      testComplexInsertIncompatibleFailure("insert_struct_incompatible_test_v2", "dfs_hadoop", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testUpPromotablePartitionInsert(String newTable, String schema, IcebergCatalogType catalogType) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 boolean, zcol3 date, zcol4 float, zcol5 decimal(10,3), " +
        "zcol6 double, zcol7 int, zcol8 bigint, zcol9 time, zcol10 timestamp, zcol11 varchar)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(true, '2019-10-27', cast(0.3 as float), cast(12345.34 as decimal(7,2)), " +
        "cast(3.6 as double), 1, 123456, '12:00:34', 1230768000000, 'abcd'))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col1 bigint, col2 decimal(20,5), col3 double) partition by (col1)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select zcol7, zcol5, zcol4 from " + schema + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + schema + "." + newTable + "_2")
        .baselineColumns("col1", "col2", "col3")
        .baselineValues(new Long("1"), new BigDecimal("12345.340"), new Double("0.3"))
        .go();

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTable + "_2");
      checkSinglePartitionValue(tableFolder, Long.class, new Long(1), catalogType);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testUpPromotablePartitionInsert() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testUpPromotablePartitionInsert("insert_uppromotable_partition_test", TEMP_SCHEMA, IcebergCatalogType.NESSIE);
      testUpPromotablePartitionInsert("insert_uppromotable_partition_test_v2", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
    }
  }

  private void checkSinglePartitionValue(File tableFolder, Class expectedClass, Object expectedValue, IcebergCatalogType catalogType) {
    Table table = getIcebergTable(tableFolder, catalogType);

    for (FileScanTask fileScanTask : table.newScan().planFiles()) {
      StructLike structLike = fileScanTask.file().partition();
      Assert.assertTrue(structLike.get(0, expectedClass).equals(expectedValue));
    }
  }

  private void testUpPromotablePartitionWithStarInsert(String newTable, String schema, IcebergCatalogType catalogType) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 int, zcol2 decimal(10,2), zcol3 float)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(true, cast(12345.34 as decimal(10,2)), cast(0.3 as float)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col1 bigint, col2 decimal(20,5), col3 double) partition by (col1)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + schema + "." + newTable + "_2")
        .baselineColumns("col1", "col2", "col3")
        .baselineValues(new Long("1"), new BigDecimal("12345.340"), new Double("0.3"))
        .go();

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTable + "_2");
      checkSinglePartitionValue(tableFolder, Long.class, new Long(1), catalogType);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testUpPromotablePartitionWithStarInsert() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testUpPromotablePartitionWithStarInsert("insert_uppromotable_partition_withstar_test", TEMP_SCHEMA, IcebergCatalogType.NESSIE);
      testUpPromotablePartitionWithStarInsert("insert_uppromotable_partition_withstar_test_v2", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
    }
  }

  private void testDecimalInsertMorePrecisionEqualScale(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 decimal(10,2))";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as decimal(10,2))))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col2 decimal(20,2))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + schema + "." + newTable + "_2")
        .baselineColumns("col2")
        .baselineValues(new BigDecimal("12345.34"))
        .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testDecimalInsertMorePrecisionEqualScale() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testDecimalInsertMorePrecisionEqualScale("insert_decimal_more_precision_test", TEMP_SCHEMA_HADOOP);
      testDecimalInsertMorePrecisionEqualScale("insert_decimal_more_precision_test_v2", TEMP_SCHEMA);
    }
  }

  private void testDecimalInsertMorePrecisionUnequalScale(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 decimal(10,5))";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34232 as decimal(10,5))))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col2 decimal(20,2))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col2::decimal(20,2)) doesn't match with query schema(zcol1::decimal(10,5))");
      test(insertUppromoting);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testDecimalInsertMorePrecisionUnequalScale() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testDecimalInsertMorePrecisionUnequalScale("insert_decimal_more_precision_unequal_scale_test", TEMP_SCHEMA_HADOOP);
      testDecimalInsertMorePrecisionUnequalScale("insert_decimal_more_precision_unequal_scale_test_v2", TEMP_SCHEMA);
    }
  }

  private void testDecimalInsertLessPrecision(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 decimal(10,2))";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as decimal(10,2))))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col2 decimal(9,2))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col2::decimal(9,2)) doesn't match with query schema(zcol1::decimal(10,2))");
      test(insertUppromoting);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testDecimalInsertLessPrecision() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testDecimalInsertLessPrecision("insert_decimal_less_precision_test", TEMP_SCHEMA_HADOOP);
      testDecimalInsertLessPrecision("insert_decimal_less_precision_test_v2", TEMP_SCHEMA);
    }
  }

  private void testDoubleToDecimalInsertFailure(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 double)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as double)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col2 decimal(18,2))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col2::decimal(18,2)) doesn't match with query schema(zcol1::double)");
      test(insertUppromoting);
      Thread.sleep(1001);

      String table3 = "create table " + schema + "." + newTable + "_3" +
        "(col3 decimal(16,2))";
      test(table3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + schema + "." + newTable + "_3" +
        " select * from " + schema + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col3::decimal(16,2)) doesn't match with query schema(zcol1::double)");
      test(insertUppromotingFailure);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_3"));
    }
  }

  @Test
  public void testDoubleToDecimalInsertFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testDoubleToDecimalInsertFailure("insert_double_decimal_failure_test", TEMP_SCHEMA_HADOOP);
      testDoubleToDecimalInsertFailure("insert_double_decimal_failure_test_v2", TEMP_SCHEMA);
    }
  }

  private void testFloatToDecimalInsertFailure(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 float)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as float)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col2 decimal(7,0))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col2::decimal(7,0)) doesn't match with query schema(zcol1::float)");
      test(insertUppromoting);

      String table3 = "create table " + schema + "." + newTable + "_3" +
        "(col3 decimal(7,1))";
      test(table3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + schema + "." + newTable + "_3" +
        " select * from " + schema + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col3::decimal(7,1)) doesn't match with query schema(zcol1::float)");
      test(insertUppromotingFailure);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_3"));
    }
  }

  @Test
  public void testFloatToDecimalInsertFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testFloatToDecimalInsertFailure("insert_float_decimal_failure_test", TEMP_SCHEMA_HADOOP);
      testFloatToDecimalInsertFailure("insert_float_decimal_failure_test_v2", TEMP_SCHEMA);
    }
  }

  private void testBigIntToDecimalInsertFailure(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 bigint)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345 as bigint)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col2 decimal(20,1))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + schema + "." + newTable + "_2")
        .baselineColumns("col2")
        .baselineValues(new BigDecimal("12345.0"))
        .go();
      String table3 = "create table " + schema + "." + newTable + "_3" +
        "(col3 decimal(20,2))";
      test(table3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + schema + "." + newTable + "_3" +
        " select * from " + schema + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col3::decimal(20,2)) doesn't match with query schema(zcol1::int64)");
      test(insertUppromotingFailure);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_3"));
    }
  }

  @Test
  public void testBigIntToDecimalInsertFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testBigIntToDecimalInsertFailure("insert_bigint_decimal_failure_test", TEMP_SCHEMA_HADOOP);
      testBigIntToDecimalInsertFailure("insert_bigint_decimal_failure_test_v2", TEMP_SCHEMA);
    }
  }

  private void testIntToDecimalInsertFailure(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 int)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345 as int)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col2 decimal(11,1))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + schema + "." + newTable + "_2")
        .baselineColumns("col2")
        .baselineValues(new BigDecimal("12345.0"))
        .go();
      String table3 = "create table " + schema + "." + newTable + "_3" +
        "(col3 decimal(11,2))";
      test(table3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + schema + "." + newTable + "_3" +
        " select * from " + schema + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col3::decimal(11,2)) doesn't match with query schema(zcol1::int32)");
      test(insertUppromotingFailure);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_3"));
    }
  }

  @Test
  public void testIntToDecimalInsertFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testIntToDecimalInsertFailure("insert_int_decimal_failure_test", TEMP_SCHEMA_HADOOP);
      testIntToDecimalInsertFailure("insert_int_decimal_failure_test_v2", TEMP_SCHEMA);
    }
  }

  private void testDecimalToDoubleInsertFailure(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 decimal(25,9))";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as decimal(25,9))))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col2 double)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + schema + "." + newTable + "_2")
        .baselineColumns("col2")
        .baselineValues(new Double("12345.34"))
        .go();
      String table3 = "create table " + schema + "." + newTable + "_3" +
        "(col3 decimal(25,3))";
      test(table3);
      Thread.sleep(1001);
      String insertTable3 = "insert into " + schema + "." + newTable + "_3" +  " select * from (" +
        "values(cast(12345.34 as decimal(25,3))))";
      test(insertTable3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_3";
      test(insertUppromotingFailure);
      testBuilder()
        .unOrdered()
        .sqlQuery("select count(*) c from " + schema + "." + newTable + "_2")
        .baselineColumns("c")
        .baselineValues(new Long(2))
        .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_3"));
    }
  }

  @Test
  public void testDecimalToDoubleInsertFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testDecimalToDoubleInsertFailure("insert_decimal_double_failure_test", TEMP_SCHEMA_HADOOP);
      testDecimalToDoubleInsertFailure("insert_decimal_double_failure_test_v2", TEMP_SCHEMA);
    }
  }

  private void testDecimalToFloatInsertFailure(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
        "(zcol1 decimal(10,3))";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as decimal(10,3))))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
        "(col2 float)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + schema + "." + newTable + "_2")
        .baselineColumns("col2")
        .baselineValues(new Float("12345.34"))
        .go();
      String table3 = "create table " + schema + "." + newTable + "_3" +
        "(col3 decimal(10,1))";
      test(table3);
      Thread.sleep(1001);
      String insertTable3 = "insert into " + schema + "." + newTable + "_3" +  " select * from (" +
        "values(cast(12345.34 as decimal(10,1))))";
      test(insertTable3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + schema + "." + newTable + "_2" +
        " select * from " + schema + "." + newTable + "_3";
      test(insertUppromotingFailure);
      testBuilder()
        .unOrdered()
        .sqlQuery("select count(*) c from " + schema + "." + newTable + "_2")
        .baselineColumns("c")
        .baselineValues(new Long(2))
        .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_3"));
    }
  }

  @Test
  public void testDecimalToFloatInsertFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testDecimalToFloatInsertFailure("insert_decimal_float_failure_test", TEMP_SCHEMA_HADOOP);
      testDecimalToFloatInsertFailure("insert_decimal_float_failure_test_v2", TEMP_SCHEMA);
    }
  }

  private void testBigIntToIntInsertFailure(String newTable, String testSchema) throws Exception {
    try {
      String table1 = "create table " + testSchema + "." + newTable + "_1" +
        "(zcol1 bigint)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + testSchema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345 as bigint)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + testSchema + "." + newTable + "_2" +
        "(col2 int)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + testSchema + "." + newTable + "_2" +
        " select * from " + testSchema + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col2::int32) doesn't match with query schema(zcol1::int64)");
      test(insertUppromoting);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testBigIntToIntInsertFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testBigIntToIntInsertFailure("insert_bigint_int_failure_test", TEMP_SCHEMA_HADOOP);
      testBigIntToIntInsertFailure("insert_bigint_int_failure_test_v2", TEMP_SCHEMA);
    }
  }

  private void testDoubleToFloatInsertFailure(String newTable, String testSchema) throws Exception {
    try {
      String table1 = "create table " + testSchema + "." + newTable + "_1" +
        "(zcol1 double)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + testSchema + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.23 as double)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + testSchema + "." + newTable + "_2" +
        "(col2 float)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + testSchema + "." + newTable + "_2" +
        " select * from " + testSchema + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col2::float) doesn't match with query schema(zcol1::double)");
      test(insertUppromoting);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testDoubleToFloatInsertFailure() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testDoubleToFloatInsertFailure("insert_double_float_failure_test", TEMP_SCHEMA_HADOOP);
      testDoubleToFloatInsertFailure("insert_double_float_failure_test", TEMP_SCHEMA);
    }
  }

  private void testInsertValuesMatchingTypes(String insertTable, String testSchema) throws Exception {
    try {
      final String tableCreate = String.format("CREATE TABLE %s.%s(id int, code1 varchar, value1 double, correct1 boolean, " +
          "start_time timestamp, id2 decimal(10,3))", testSchema, insertTable);
      test(tableCreate);

      Thread.sleep(1001);

      final String insertValuesQuery = String.format("INSERT INTO %s.%s VALUES(1, 'cat'," +
              " 1.2, false, 1230768000000, cast(134.56 as decimal(10, 3)))",
        testSchema, insertTable);
      test(insertValuesQuery);

      testBuilder()
          .sqlQuery(String.format("select ID, CODE1 from %s.%s", testSchema, insertTable))
          .unOrdered()
          .baselineColumns("ID", "CODE1")
          .baselineValues(1, "cat")
          .build()
          .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), insertTable));
    }
  }

  @Test
  public void testInsertValuesMatchingTypes() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testInsertValuesMatchingTypes("insertTable1", TEMP_SCHEMA_HADOOP);
      testInsertValuesMatchingTypes("insertTable1_v2", TEMP_SCHEMA);
    }
  }

  private void testUpPromotableInsertValues(String newTable, String testSchema) throws Exception {
    try {
      String table1 = "create table " + testSchema + "." + newTable + "_1" +
          "(zcol1 boolean, zcol3 date, zcol4 float, zcol5 decimal(10,3), " +
          "zcol6 double, zcol7 int, zcol8 bigint, zcol9 time, zcol10 timestamp, zcol11 varchar)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + testSchema + "." + newTable + "_1" + " values(true, '2019-10-27', " +
          "cast(0.3 as float), cast(12345.34 as decimal(7,2)), " +
          "cast(3.6 as double), 1, 123456, '12:00:34', 1230768000000, 'abcd')";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + testSchema + "." + newTable + "_2" +
          "(col1 bigint, col2 decimal(20,5), col3 double)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + testSchema + "." + newTable + "_2"
          + " values(1, cast(12345.34 as decimal(7,2)), cast(0.3 as float))";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + testSchema + "." + newTable + "_2")
          .baselineColumns("col1", "col2", "col3")
          .baselineValues(new Long("1"), new BigDecimal("12345.340"), new Double("0.3"))
          .go();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testUpPromotableInsertValues() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testUpPromotableInsertValues("insertvalues_uppromotable_test", TEMP_SCHEMA_HADOOP);
      testUpPromotableInsertValues("insertvalues_uppromotable_test_v2", TEMP_SCHEMA);
    }
  }

  private void testWriteMinMaxAndVerifyPruning(String tableName, String testSchema, IcebergCatalogType catalogType) throws Exception {
    try {
      test("create table " + testSchema + "." + tableName +
        " (col1 boolean, col2 int, col3 bigint, col4 float, col5 double, " +
        "col6 decimal(15,3), col7 date, col8 time, " +
        "col9 timestamp, col10 varchar)");

      test("insert into " + testSchema + "." + tableName +
        " select * from (values(false, 1, 1, cast(1.0 as float), cast(1.0 as double), " +
        "cast(1.0 as decimal(15,3)), cast('2019-12-25' as date), cast('12:00:00' as time), " +
        "cast('2019-12-25 12:00:00' as timestamp), 'abc'))");

      test("insert into " + testSchema + "." + tableName +
        " select * from (values(true, 10, 10, cast(10.0 as float), cast(10.0 as double), " +
        "cast(10.0 as decimal(15,3)), cast('2019-12-26' as date), cast('12:10:00' as time), " +
        "cast('2019-12-26 12:10:00' as timestamp), 'def'))");

      test("insert into " + testSchema + "." + tableName +
        " select * from (values(true, 100, 100, cast(100.0 as float), cast(100.0 as double), " +
        "cast(100.0 as decimal(15,3)), cast('2019-12-27' as date), cast('12:20:00' as time), " +
        "cast('2019-12-27 12:20:00' as timestamp), 'ghi'))");

      File tableDir = new File(getDfsTestTmpSchemaLocation(), tableName);
      Table icebergTable = getIcebergTable(tableDir, catalogType);

      Iterable<DataFile> files = FindFiles.in(icebergTable)
        .withRecordsMatching(greaterThanOrEqual("col1", true))
        .withRecordsMatching(greaterThanOrEqual("col2", 10))
        .withRecordsMatching(greaterThanOrEqual("col3", 10L))
        .withRecordsMatching(greaterThanOrEqual("col4", 10.0f))
        .withRecordsMatching(greaterThanOrEqual("col5", 10.0d))
        .withRecordsMatching(greaterThanOrEqual("col6", 10.0d))
        .withRecordsMatching(greaterThanOrEqual("col7", "2019-12-26"))
        .withRecordsMatching(greaterThanOrEqual("col8", Literal.of("12:10:00").to(Types.TimeType.get()).value()))
        .withRecordsMatching(greaterThanOrEqual("col9", Literal.of("2019-12-26T12:10:00").to(Types.TimestampType.withoutZone()).value()))
        .withRecordsMatching(greaterThanOrEqual("col10", "def"))
        .collect();
      Assert.assertEquals(2, pathSet(files).size());
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

  @Test
  public void testWriteMinMaxAndVerifyPruning() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testWriteMinMaxAndVerifyPruning("testWriteMinMaxAndVerifyPruning", TEMP_SCHEMA, IcebergCatalogType.NESSIE);
      testWriteMinMaxAndVerifyPruning("testWriteMinMaxAndVerifyPruning_v2", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
    }
  }

  private void testInsertValuesPartitionedTable(String table1, String testSchema, IcebergCatalogType catalogType) throws Exception {
    try {
      final String table1Create = String.format("CREATE TABLE %s.%s(id int, code int) partition by (code)",
        testSchema, table1);
      test(table1Create);

      Thread.sleep(1001);

      test(String.format("INSERT INTO %s.%s VALUES(1, 1)", testSchema, table1));
      Thread.sleep(1001);
      test(String.format("INSERT INTO %s.%s VALUES(1, 2)", testSchema, table1));
      Thread.sleep(1001);
      test(String.format("INSERT INTO %s.%s VALUES(1, 3)", testSchema, table1));
      Thread.sleep(1001);
      test(String.format("INSERT INTO %s.%s VALUES(1, 4)", testSchema, table1));
      Thread.sleep(1001);
      test(String.format("INSERT INTO %s.%s VALUES(1, 5)", testSchema, table1));

      // same data in table1 and table2
      testBuilder()
          .sqlQuery(String.format("select id, code from %s.%s", testSchema, table1))
          .unOrdered()
          .baselineColumns("id", "code")
          .baselineValues(1, 1)
          .baselineValues(1, 2)
          .baselineValues(1, 3)
          .baselineValues(1, 4)
          .baselineValues(1, 5)
          .build()
          .run();

      List<Integer> valuesInCodeColumn = IntStream.range(1, 6).boxed().collect(Collectors.toList());

      File table1Folder = new File(getDfsTestTmpSchemaLocation(), table1);
      Table icebergTable1 = getIcebergTable(table1Folder, catalogType);
      List<Integer> partitionValues = StreamSupport.stream(icebergTable1.newScan().planFiles().spliterator(), false)
          .map(fileScanTask -> fileScanTask.file().partition())
          .map(structLike -> structLike.get(0, Integer.class))
          .collect(Collectors.toList());

      Assert.assertEquals(valuesInCodeColumn.size(), partitionValues.size());
      Assert.assertTrue(partitionValues.containsAll(valuesInCodeColumn));

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), table1));
    }
  }

  @Test
  public void testInsertValuesPartitionedTable() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testInsertValuesPartitionedTable("insertTable2", TEMP_SCHEMA, IcebergCatalogType.NESSIE);
      testInsertValuesPartitionedTable("insertTable2_v2", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
    }
  }

  private void insertIntoFewCols(String newTable, String schema) throws Exception {
    try {
      String table = String.format("create table %s.%s(id int, name varchar)", schema, newTable);
      test(table);
      String insertTable1 = String.format("insert into %s.%s(id, name) select * from (values(1, 'name1'))", schema, newTable);
      test(insertTable1);
      Thread.sleep(1001);
      String insertTable2 = String.format("insert into %s.%s(name) select * from (values('name2'))", schema, newTable);
      test(insertTable2);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + schema + "." + newTable)
          .baselineColumns("id", "name")
          .baselineValues(1, "name1")
          .baselineValues(null, "name2")
          .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewCols() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      insertIntoFewCols("insert_select_cols1", TEMP_SCHEMA_HADOOP);
      insertIntoFewCols("insert_select_cols1_v2", TEMP_SCHEMA);
    }
  }

  private void insertIntoFewColsDuplicate(String newTable, String tempSchema) throws Exception {
    try {
      String table = String.format("create table %s.%s(id int, name varchar)", tempSchema, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(id, id) select id, id from %s.%s", tempSchema, newTable, tempSchema, newTable);
      errorMsgTestHelper(insertTable, "Duplicate column name [id]");
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsDuplicate() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      insertIntoFewColsDuplicate("insert_select_cols2", TEMP_SCHEMA_HADOOP);
      insertIntoFewColsDuplicate("insert_select_cols2_v2", TEMP_SCHEMA);
    }
  }

  private void insertIntoFewColsTypeSpecified(String newTable, String schema) throws Exception {
    try {
      String table = String.format("create table %s.%s(id int, name varchar)", schema, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(id int, name varchar) select id, name from %s.%s", schema, newTable, schema, newTable);
      errorMsgTestHelper(insertTable, "Column type specified");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsTypeSpecified() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      insertIntoFewColsTypeSpecified("insert_select_cols2002", TEMP_SCHEMA_HADOOP);
      insertIntoFewColsTypeSpecified("insert_select_cols2002_v2", TEMP_SCHEMA);
    }
  }

  private void insertIntoFewColsNonExistingCol(String newTable, String schema) throws Exception {
    try {
      String table = String.format("create table %s.%s(id int, name varchar)", schema, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(id, id1, id2) select id, id, id from %s.%s", schema, newTable, schema, newTable);
      errorMsgTestHelper(insertTable, "Specified column(s) [id2, id1] not found in schema.");
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsNonExistingCol() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      insertIntoFewColsNonExistingCol("insert_select_cols3", TEMP_SCHEMA_HADOOP);
      insertIntoFewColsNonExistingCol("insert_select_cols3_v2", TEMP_SCHEMA);
    }
  }

  private void insertIntoFewColsSchemaMismatch(String newTable, String schema) throws Exception {
    try {
      String table = String.format("create table %s.%s(id int, name varchar)", schema, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(id) select name from %s.%s", schema, newTable, schema, newTable);
      errorMsgTestHelper(insertTable, "Table schema(id::int32) doesn't match with query schema(name::varchar)");
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsSchemaMismatch() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      insertIntoFewColsSchemaMismatch("insert_select_cols4", TEMP_SCHEMA_HADOOP);
      insertIntoFewColsSchemaMismatch("insert_select_cols4_v2", TEMP_SCHEMA);
    }
  }

  private void insertIntoFewColsOrderedFields(String newTable, String schema) throws Exception {
    try {
      String table = String.format("create table %s.%s(id int, name varchar, address varchar, pin int)", schema, newTable);
      test(table);

      String insertTable = String.format("insert into %s.%s values(1, 'name1', 'address1', 1)", schema, newTable);
      test(insertTable);
      Thread.sleep(1001);
      String insertTable2 = String.format("insert into %s.%s(name, pin) values('name2', 2)", schema, newTable);
      test(insertTable2);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + schema + "." + newTable)
          .baselineColumns("id", "name", "address", "pin")
          .baselineValues(1, "name1", "address1", 1)
          .baselineValues(null, "name2", null, 2)
          .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsOrderedFields() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      insertIntoFewColsOrderedFields("insert_select_cols5", TEMP_SCHEMA_HADOOP);
      insertIntoFewColsOrderedFields("insert_select_cols5_v2", TEMP_SCHEMA);
    }
  }

  private void insertIntoFewColsUnorderedFields(String newTable, String schema) throws Exception {
    try {
      String table = String.format("create table %s.%s(id int, name varchar)", schema, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(name, id) values('name1', 1)", schema, newTable);
      test(insertTable);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + schema + "." + newTable)
          .baselineColumns("id", "name")
          .baselineValues(1, "name1")
          .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsUnorderedFields() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      insertIntoFewColsUnorderedFields("insert_select_cols005", TEMP_SCHEMA_HADOOP);
      insertIntoFewColsUnorderedFields("insert_select_cols005_v2", TEMP_SCHEMA);
    }
  }

  private void insertIntoFewColsUppromotableTypes(String newTable, String schema) throws Exception {
    try {
      String table = String.format("create table %s.%s(id float, name varchar)", schema, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(id, name) values(1, 'name1')", schema, newTable);
      test(insertTable);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + schema + "." + newTable)
          .baselineColumns("id", "name")
          .baselineValues(1.0f, "name1")
          .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsUppromotableTypes() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      insertIntoFewColsUppromotableTypes("insert_select_cols6", TEMP_SCHEMA_HADOOP);
      insertIntoFewColsUppromotableTypes("insert_select_cols6_v2", TEMP_SCHEMA);
    }
  }

  private void testFewColsUpPromotableInsert(String newTable, String schema) throws Exception {
    try {
      String table1 = "create table " + schema + "." + newTable + "_1" +
          "(zcol1 boolean, zcol3 date, zcol4 float, zcol5 decimal(10,3), " +
          "zcol6 double, zcol7 int, zcol8 bigint, zcol9 time, zcol10 timestamp, zcol11 varchar)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + schema + "." + newTable + "_1(zcol1, zcol3, zcol4, zcol5, zcol6, " +
          "zcol7, zcol8, zcol9, zcol10, zcol11)" +  " select * from (" +
          "values(true, '2019-10-27', cast(0.3 as float), cast(12345.34 as decimal(7,2)), " +
          "cast(3.6 as double), 1, 123456, '12:00:34', 1230768000000, 'abcd'))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + schema + "." + newTable + "_2" +
          "(col1 bigint, col2 decimal(20,5), col3 double)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + schema + "." + newTable + "_2(col1, col2, col3)" +
          " select zcol7, zcol5, zcol4 from " + schema + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + schema + "." + newTable + "_2")
          .baselineColumns("col1", "col2", "col3")
          .baselineValues(new Long("1"), new BigDecimal("12345.340"), new Double("0.3"))
          .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testFewColsUpPromotableInsert() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testFewColsUpPromotableInsert("insert_few_cols7", TEMP_SCHEMA_HADOOP);
      testFewColsUpPromotableInsert("insert_few_cols7_v2", TEMP_SCHEMA);
    }
  }

  private Set<String> pathSet(Iterable<DataFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }
}
