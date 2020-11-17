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

import static org.joda.time.DateTimeZone.UTC;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Time;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
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
import com.dremio.test.TemporarySystemProperties;
import com.google.common.collect.Sets;

public class TestInsertIntoTable extends BaseTestQuery {

  private ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Test
  public void testSimpleInsertCommand() {
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
  public void testInsertCommandInvalidPath() throws Exception {
    final String tblName = "invalid_path_test";
    try (AutoCloseable c = enableIcebergTables()) {
      final String createTableQuery = String.format("CREATE TABLE %s.%s(id int, code int)",
        TEMP_SCHEMA, tblName);
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
  public void testInsertIntoSysTable() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String insertQuery = "INSERT INTO sys.version VALUES('A', 'A', 'A', 'A', 'A', 'A')";
      errorMsgTestHelper(insertQuery, "[sys.version] is a SYSTEM_TABLE");
    }
  }

  @Test
  public void testInsertIntoView() throws Exception {
    try {
      properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
      final String tblName = "sysverview";
      try (AutoCloseable c = enableIcebergTables()) {
        final String createTableQuery = String.format("CREATE or REPLACE VIEW %s.%s as select * from sys.version",
          TEMP_SCHEMA, tblName);
        test(createTableQuery);

        Thread.sleep(1001);

        final String insertQuery = String.format("INSERT INTO %s.%s VALUES('A', 'A', 'A', 'A', 'A', 'A')", TEMP_SCHEMA, tblName);
        errorMsgTestHelper(insertQuery, String.format("[%s.%s] is a VIEW", TEMP_SCHEMA, tblName));
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
      }
    } finally {
      properties.clear(DremioConfig.LEGACY_STORE_VIEWS_ENABLED);
    }
  }

  @Test
  public void testInsertDuplicateColumnsInSelect() throws Exception {
    final String table = "dupColsInSelect";

    try (AutoCloseable c = enableIcebergTables()) {
      final String tableCreate = String.format("CREATE TABLE %s.%s(id int, code int)",
          TEMP_SCHEMA, table);
      test(tableCreate);

      Thread.sleep(1001);

      final String insertIntoTable = String.format("INSERT INTO %s.%s SELECT n_nationkey, n_nationkey " +
              "from cp.\"tpch/nation.parquet\"",
          TEMP_SCHEMA, table);
      test(insertIntoTable);

      testBuilder()
          .sqlQuery(String.format("select ID, CODE from %s.%s", TEMP_SCHEMA, table))
          .unOrdered()
          .sqlBaselineQuery("SELECT n_nationkey ID, n_nationkey CODE from cp.\"tpch/nation.parquet\"")
          .build()
          .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), table));
    }
  }

  @Test
  public void testInsertColumnNameCaseMismatch() throws Exception {
    final String tableLower = "tableLower";

    try (AutoCloseable c = enableIcebergTables()) {
      final String tableLowerCreate = String.format("CREATE TABLE %s.%s(id int, code int) partition by (code)",
        TEMP_SCHEMA, tableLower);
      test(tableLowerCreate);

      Thread.sleep(1001);

      final String insertUpperIntoLower = String.format("INSERT INTO %s.%s SELECT n_nationkey id, n_regionkey CODE " +
          "from cp.\"tpch/nation.parquet\"",
        TEMP_SCHEMA, tableLower);
      test(insertUpperIntoLower);

      testBuilder()
        .sqlQuery(String.format("select ID, CODE from %s.%s", TEMP_SCHEMA, tableLower))
        .unOrdered()
        .sqlBaselineQuery("SELECT n_nationkey ID, n_regionkey CODE from cp.\"tpch/nation.parquet\"")
        .build()
        .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableLower));
    }
  }

  @Test
  public void testInsertIntoPartitionedTable() throws Exception {
    final String table1 = "table1";
    final String table2 = "table2";

    try (AutoCloseable c = enableIcebergTables()) {
      final String table1Create = String.format("CREATE TABLE %s.%s(id int, code int) partition by (code)",
        TEMP_SCHEMA, table1);
      test(table1Create);

      // table2 partitioned by n_regionkey
      final String table2CTAS = String.format("CREATE TABLE %s.%s partition by(n_regionkey) " +
          " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 10",
        TEMP_SCHEMA, table2);
      test(table2CTAS);

      Thread.sleep(1001);

      // insert into table1 which is partitioned by n_nationkey values
      final String insertIntoTable1 = String.format("INSERT INTO %s.%s SELECT n_regionkey id , n_nationkey code from %s.%s",
        TEMP_SCHEMA, table1, TEMP_SCHEMA, table2);
      test(insertIntoTable1);

      Thread.sleep(1001);

      // same data in table1 and table2
      testBuilder()
        .sqlQuery(String.format("select id, code from %s.%s", TEMP_SCHEMA, table1))
        .unOrdered()
        .baselineColumns("id", "code")
        .sqlBaselineQuery(String.format("SELECT n_regionkey id, n_nationkey code from %s.%s", TEMP_SCHEMA, table2))
        .build()
        .run();

      List<Integer> valuesInCodeColumn = IntStream.range(0, 10).boxed().collect(Collectors.toList());
      testBuilder()
        .sqlQuery(String.format("select code from %s.%s", TEMP_SCHEMA, table1))
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

      File table1Folder = new File(getDfsTestTmpSchemaLocation(), table1);
      Table icebergTable1 = new HadoopTables(new Configuration()).load(table1Folder.getPath());
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
  public void testInsertUsingValues() throws Exception {
    final String insert_values_test = "insert_values_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + insert_values_test +
        "(PolicyID bigint, statecode varchar, county varchar, " +
        "eq_site_limit double, hu_site_limit double, fl_site_limit double, " +
        "fr_site_limit double, tiv_2011 double, tiv_2012 double, " +
        "eq_site_deductible varchar, hu_site_deductible varchar, " +
        "fl_site_deductible varchar, fr_site_dedu varchar)";
      test(createCommandSql);
      Thread.sleep(1001);
      String insertCommandSql = "insert into " + TEMP_SCHEMA + "." + insert_values_test +
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
        .sqlQuery(String.format("select count(*) c from %s.%s", TEMP_SCHEMA, insert_values_test))
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
    final String insert_values_test = "insert_values_schema_mismatch_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + insert_values_test +
        "(PolicyID bigint, statecode varchar, county varchar, " +
        "eq_site_limit double, hu_site_limit double, fl_site_limit double, " +
        "fr_site_limit double, tiv_2011 double, tiv_2012 double, " +
        "eq_site_deductible varchar, hu_site_deductible varchar, " +
        "fl_site_deductible varchar, fr_site_dedu varchar)";
      test(createCommandSql);
      Thread.sleep(1001);
      String insertCommandSql = "insert into " + TEMP_SCHEMA + "." + insert_values_test +
        " SELECT * FROM (VALUES(" +
        "cast(1 as int), 'CT', 'Conn', cast(0 as double), " +
        "cast(0 as double), cast(0 as double), cast(0 as double), " +
        "cast(0 as float), cast(0 as double), '0', '0', '0', '0'  ))";
      test(insertCommandSql);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s", TEMP_SCHEMA, insert_values_test))
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
  public void testTypesNamesMatchExactly() throws Exception {
    final String newTable = "insert_types_names_match_exactly";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(col1 boolean, col4 float, col5 decimal(10,3), " +
      "col6 double, col7 int, col8 bigint, col9 time, col10 timestamp, col11 varchar)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(true, cast(0.3 as float), cast(12345.34 as decimal(7,2)), " +
        "cast(3.6 as double), 1, 123456, " +
        "'12:00:34'" +
        ", 1230768000000, 'abcd'))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col1 boolean, col4 float, col5 decimal(10,3), " +
        "col6 double, col7 int, col8 bigint, col9 time, col10 timestamp, col11 varchar)";
      test(table2);
      Thread.sleep(1001);
      String insertTypesNamesMatchingQuery = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertTypesNamesMatchingQuery);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
        .unOrdered()
        .sqlBaselineQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_1")
        .build()
        .run();

      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
        .baselineColumns("col1", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11")
        .baselineValues(true, new Float("0.3"),
          new BigDecimal("12345.340"), new Double("3.6"), new Integer(1),
          new Long("123456"), new LocalDateTime(Time.valueOf("12:00:34").getTime(), DateTimeZone.forTimeZone(TimeZone.getDefault())), new LocalDateTime(1230768000000L, UTC), "abcd")
        .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testTypesMatchExactlyButNotNames() throws Exception {
    final String newTable = "insert_types_match_not_names_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 boolean, zcol4 float, zcol5 decimal(10,3), " +
        "zcol6 double, zcol7 int, zcol8 bigint, zcol9 time, zcol10 timestamp, zcol11 varchar)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(true, cast(0.3 as float), cast(12345.34 as decimal(7,2)), " +
        "cast(3.6 as double), 1, 123456, " +
        "'12:00:34'" +
        ", 1230768000000, 'abcd'))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col1 boolean, col4 float, col5 decimal(10,3), " +
        "col6 double, col7 int, col8 bigint, col9 time, col10 timestamp, col11 varchar)";
      test(table2);
      Thread.sleep(1001);
      String insertTypesNamesMatchingQuery = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertTypesNamesMatchingQuery);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
        .unOrdered()
        .sqlBaselineQuery("select zcol1 col1, zcol4 col4, " +
          "zcol5 col5, zcol6 col6, zcol7 col7, zcol8 col8, zcol9 col9, zcol10 col10, " +
          "zcol11 col11 from " + TEMP_SCHEMA + "." + newTable + "_1")
        .build()
        .run();

      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
        .baselineColumns("col1", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11")
        .baselineValues(true, new Float("0.3"),
          new BigDecimal("12345.340"), new Double("3.6"), new Integer(1),
          new Long("123456"), new LocalDateTime(Time.valueOf("12:00:34").getTime(), DateTimeZone.forTimeZone(TimeZone.getDefault())), new LocalDateTime(1230768000000L, UTC), "abcd")
        .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testIncomptibleStringToInt() throws Exception {
    final String newTable = "insert_incompatible_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable +
        "(col1 int)";
      test(table1);
      String insert = "insert into " + TEMP_SCHEMA + "." + newTable + " select * from (values('abcd'))";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage(String.format("Table schema(col1::int32) doesn't match with query schema(EXPR$0::varchar"));
      test(insert);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void testUpPromotableInsert() throws Exception {
    final String newTable = "insert_uppromotable_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 boolean, zcol3 date, zcol4 float, zcol5 decimal(10,3), " +
        "zcol6 double, zcol7 int, zcol8 bigint, zcol9 time, zcol10 timestamp, zcol11 varchar)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(true, '2019-10-27', cast(0.3 as float), cast(12345.34 as decimal(7,2)), " +
        "cast(3.6 as double), 1, 123456, '12:00:34', 1230768000000, 'abcd'))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col1 bigint, col2 decimal(20,5), col3 double)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select zcol7, zcol5, zcol4 from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
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
  public void testListComplexInsert() throws Exception {
    final String newTable = "insert_list_test";
    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String listbigint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/listbigint";

      final String createListBigInt = String.format("CREATE TABLE %s.%s_1  " +
          " AS SELECT col1 as listcol1 from dfs.\"" + listbigint +"\"",
        TEMP_SCHEMA, newTable);

      test(createListBigInt);
      Thread.sleep(1001);

      final String createListInt = String.format("CREATE TABLE %s.%s_2  " +
          " AS SELECT col1 as listcol2 from dfs.\"" + listbigint +"\"",
        TEMP_SCHEMA, newTable);

      test(createListInt);
      Thread.sleep(1001);

      final String insertQuery = String.format("insert into %s.%s_1 select listcol2 from %s.%s_2",
        TEMP_SCHEMA, newTable, TEMP_SCHEMA, newTable);
      test(insertQuery);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s_1", TEMP_SCHEMA, newTable))
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
  public void testListComplexInsertFailure() throws Exception {
    final String newTable = "insert_list_failure_test";
    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String listbigint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/listbigint";
      final String listint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/listint";

      final String createListBigInt = String.format("CREATE TABLE %s.%s_1  " +
          " AS SELECT col1 as listcol1 from dfs.\"" + listbigint +"\"",
        TEMP_SCHEMA, newTable);

      test(createListBigInt);
      Thread.sleep(1001);

      final String createListInt = String.format("CREATE TABLE %s.%s_2  " +
          " AS SELECT col1 as listcol2 from dfs.\"" + listint +"\"",
        TEMP_SCHEMA, newTable);

      test(createListInt);
      Thread.sleep(1001);

      final String insertQuery = String.format("insert into %s.%s_1 select listcol2 from %s.%s_2",
        TEMP_SCHEMA, newTable, TEMP_SCHEMA, newTable);
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
  public void testStructComplexInsert() throws Exception {
    final String newTable = "insert_struct_test";
    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String structbigint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/structbigint";

      final String createStructBigInt = String.format("CREATE TABLE %s.%s_1  " +
          " AS SELECT col1 as structcol1 from dfs.\"" + structbigint +"\"",
        TEMP_SCHEMA, newTable);

      test(createStructBigInt);
      Thread.sleep(1001);

      final String createStructInt = String.format("CREATE TABLE %s.%s_2  " +
          " AS SELECT col1 as structcol2 from dfs.\"" + structbigint +"\"",
        TEMP_SCHEMA, newTable);

      test(createStructInt);
      Thread.sleep(1001);

      final String insertQuery = String.format("insert into %s.%s_1 select structcol2 from %s.%s_2",
        TEMP_SCHEMA, newTable, TEMP_SCHEMA, newTable);
      test(insertQuery);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s_1", TEMP_SCHEMA, newTable))
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
  public void testStructComplexInsertFailure() throws Exception {
    final String newTable = "insert_struct_failure_test";
    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String structbigint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/structbigint";
      final String structint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/structint";

      final String createStructBigInt = String.format("CREATE TABLE %s.%s_1  " +
          " AS SELECT col1 as structcol1 from dfs.\"" + structbigint +"\"",
        TEMP_SCHEMA, newTable);

      test(createStructBigInt);
      Thread.sleep(1001);

      final String createStructInt = String.format("CREATE TABLE %s.%s_2  " +
          " AS SELECT col1 as structcol2 from dfs.\"" + structint +"\"",
        TEMP_SCHEMA, newTable);

      test(createStructInt);
      Thread.sleep(1001);

      final String insertQuery = String.format("insert into %s.%s_1 select structcol2 from %s.%s_2",
        TEMP_SCHEMA, newTable, TEMP_SCHEMA, newTable);
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
  public void testComplexInsertIncompatibleFailure() throws Exception {
    final String newTable = "insert_struct_incompatible_test";
    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String structbigint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/structbigint";
      final String structint = testWorkingPath + "/src/test/resources/iceberg/complexTypeTest/structint";

      final String createStructBigInt = String.format("CREATE TABLE %s.%s_1  " +
          " AS SELECT col1 as structcol1 from dfs.\"" + structbigint +"\"",
        TEMP_SCHEMA, newTable);

      test(createStructBigInt);
      Thread.sleep(1001);

      final String createStructInt = String.format("CREATE TABLE %s.%s_2  " +
          " AS SELECT col1 as structcol2 from dfs.\"" + structint +"\"",
        TEMP_SCHEMA, newTable);

      test(createStructInt);
      Thread.sleep(1001);

      final String insertQuery = String.format("insert into %s.%s_1 select %s_2.structcol2.name from %s.%s_2",
        TEMP_SCHEMA, newTable, newTable, TEMP_SCHEMA, newTable);
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
  public void testUpPromotablePartitionInsert() throws Exception {
    final String newTable = "insert_uppromotable_partition_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 boolean, zcol3 date, zcol4 float, zcol5 decimal(10,3), " +
        "zcol6 double, zcol7 int, zcol8 bigint, zcol9 time, zcol10 timestamp, zcol11 varchar)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(true, '2019-10-27', cast(0.3 as float), cast(12345.34 as decimal(7,2)), " +
        "cast(3.6 as double), 1, 123456, '12:00:34', 1230768000000, 'abcd'))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col1 bigint, col2 decimal(20,5), col3 double) partition by (col1)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select zcol7, zcol5, zcol4 from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
        .baselineColumns("col1", "col2", "col3")
        .baselineValues(new Long("1"), new BigDecimal("12345.340"), new Double("0.3"))
        .go();

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTable + "_2");
      checkSinglePartitionValue(tableFolder, Long.class, new Long(1));
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  private void checkSinglePartitionValue(File tableFolder, Class expectedClass, Object expectedValue) {
    Table table = new HadoopTables(new Configuration()).load(tableFolder.getPath());
    for (FileScanTask fileScanTask : table.newScan().planFiles()) {
      StructLike structLike = fileScanTask.file().partition();
      Assert.assertTrue(structLike.get(0, expectedClass).equals(expectedValue));
    }
  }

  @Test
  public void testUpPromotablePartitionWithStarInsert() throws Exception {
    final String newTable = "insert_uppromotable_partition_withstar_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 int, zcol2 decimal(10,2), zcol3 float)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(true, cast(12345.34 as decimal(10,2)), cast(0.3 as float)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col1 bigint, col2 decimal(20,5), col3 double) partition by (col1)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
        .baselineColumns("col1", "col2", "col3")
        .baselineValues(new Long("1"), new BigDecimal("12345.340"), new Double("0.3"))
        .go();

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTable + "_2");
      checkSinglePartitionValue(tableFolder, Long.class, new Long(1));
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testDecimalInsertMorePrecisionEqualScale() throws Exception {
    final String newTable = "insert_decimal_more_precision_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 decimal(10,2))";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as decimal(10,2))))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 decimal(20,2))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
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
  public void testDecimalInsertMorePrecisionUnequalScale() throws Exception {
    final String newTable = "insert_decimal_more_precision_unequal_scale_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 decimal(10,5))";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34232 as decimal(10,5))))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 decimal(20,2))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
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
  public void testDecimalInsertLessPrecision() throws Exception {
    final String newTable = "insert_decimal_less_precision_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 decimal(10,2))";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as decimal(10,2))))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 decimal(9,2))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
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
  public void testDoubleToDecimalInsertFailure() throws Exception {
    final String newTable = "insert_double_decimal_failure_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 double)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as double)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 decimal(18,2))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col2::decimal(18,2)) doesn't match with query schema(zcol1::double)");
      test(insertUppromoting);
      Thread.sleep(1001);

      String table3 = "create table " + TEMP_SCHEMA + "." + newTable + "_3" +
        "(col3 decimal(16,2))";
      test(table3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + TEMP_SCHEMA + "." + newTable + "_3" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
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
  public void testFloatToDecimalInsertFailure() throws Exception {
    final String newTable = "insert_float_decimal_failure_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 float)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as float)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 decimal(7,0))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage("Table schema(col2::decimal(7,0)) doesn't match with query schema(zcol1::float)");
      test(insertUppromoting);

      String table3 = "create table " + TEMP_SCHEMA + "." + newTable + "_3" +
        "(col3 decimal(7,1))";
      test(table3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + TEMP_SCHEMA + "." + newTable + "_3" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
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
  public void testBigIntToDecimalInsertFailure() throws Exception {
    final String newTable = "insert_bigint_decimal_failure_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 bigint)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345 as bigint)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 decimal(20,1))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
        .baselineColumns("col2")
        .baselineValues(new BigDecimal("12345.0"))
        .go();
      String table3 = "create table " + TEMP_SCHEMA + "." + newTable + "_3" +
        "(col3 decimal(20,2))";
      test(table3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + TEMP_SCHEMA + "." + newTable + "_3" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
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
  public void testIntToDecimalInsertFailure() throws Exception {
    final String newTable = "insert_int_decimal_failure_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 int)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345 as int)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 decimal(11,1))";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
        .baselineColumns("col2")
        .baselineValues(new BigDecimal("12345.0"))
        .go();
      String table3 = "create table " + TEMP_SCHEMA + "." + newTable + "_3" +
        "(col3 decimal(11,2))";
      test(table3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + TEMP_SCHEMA + "." + newTable + "_3" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
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
  public void testDecimalToDoubleInsertFailure() throws Exception {
    final String newTable = "insert_decimal_double_failure_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 decimal(25,9))";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as decimal(25,9))))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 double)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
        .baselineColumns("col2")
        .baselineValues(new Double("12345.34"))
        .go();
      String table3 = "create table " + TEMP_SCHEMA + "." + newTable + "_3" +
        "(col3 decimal(25,3))";
      test(table3);
      Thread.sleep(1001);
      String insertTable3 = "insert into " + TEMP_SCHEMA + "." + newTable + "_3" +  " select * from (" +
        "values(cast(12345.34 as decimal(25,3))))";
      test(insertTable3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_3";
      test(insertUppromotingFailure);
      testBuilder()
        .unOrdered()
        .sqlQuery("select count(*) c from " + TEMP_SCHEMA + "." + newTable + "_2")
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
    final String newTable = "insert_decimal_float_failure_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 decimal(10,3))";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.34 as decimal(10,3))))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 float)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
        .unOrdered()
        .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
        .baselineColumns("col2")
        .baselineValues(new Float("12345.34"))
        .go();
      String table3 = "create table " + TEMP_SCHEMA + "." + newTable + "_3" +
        "(col3 decimal(10,1))";
      test(table3);
      Thread.sleep(1001);
      String insertTable3 = "insert into " + TEMP_SCHEMA + "." + newTable + "_3" +  " select * from (" +
        "values(cast(12345.34 as decimal(10,1))))";
      test(insertTable3);
      Thread.sleep(1001);
      String insertUppromotingFailure = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_3";
      test(insertUppromotingFailure);
      testBuilder()
        .unOrdered()
        .sqlQuery("select count(*) c from " + TEMP_SCHEMA + "." + newTable + "_2")
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
  public void testBigIntToIntInsertFailure() throws Exception {
    final String newTable = "insert_bigint_int_failure_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 bigint)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345 as bigint)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 int)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
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
  public void testDoubleToFloatInsertFailure() throws Exception {
    final String newTable = "insert_double_float_failure_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
        "(zcol1 double)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" +  " select * from (" +
        "values(cast(12345.23 as double)))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
        "(col2 float)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2" +
        " select * from " + TEMP_SCHEMA + "." + newTable + "_1";
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
  public void testInsertValuesMatchingTypes() throws Exception {
    final String insertTable = "insertTable1";

    try (AutoCloseable c = enableIcebergTables()) {
      final String tableCreate = String.format("CREATE TABLE %s.%s(id int, code1 varchar, value1 double, correct1 boolean, " +
          "start_time timestamp, id2 decimal(10,3))", TEMP_SCHEMA, insertTable);
      test(tableCreate);

      Thread.sleep(1001);

      final String insertValuesQuery = String.format("INSERT INTO %s.%s VALUES(1, 'cat'," +
              " 1.2, false, 1230768000000, cast(134.56 as decimal(10, 3)))",
          TEMP_SCHEMA, insertTable);
      test(insertValuesQuery);

      testBuilder()
          .sqlQuery(String.format("select ID, CODE1 from %s.%s", TEMP_SCHEMA, insertTable))
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
  public void testUpPromotableInsertValues() throws Exception {
    final String newTable = "insertvalues_uppromotable_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
          "(zcol1 boolean, zcol3 date, zcol4 float, zcol5 decimal(10,3), " +
          "zcol6 double, zcol7 int, zcol8 bigint, zcol9 time, zcol10 timestamp, zcol11 varchar)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1" + " values(true, '2019-10-27', " +
          "cast(0.3 as float), cast(12345.34 as decimal(7,2)), " +
          "cast(3.6 as double), 1, 123456, '12:00:34', 1230768000000, 'abcd')";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
          "(col1 bigint, col2 decimal(20,5), col3 double)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2"
          + " values(1, cast(12345.34 as decimal(7,2)), cast(0.3 as float))";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
          .baselineColumns("col1", "col2", "col3")
          .baselineValues(new Long("1"), new BigDecimal("12345.340"), new Double("0.3"))
          .go();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

  @Test
  public void testInsertValuesPartitionedTable() throws Exception {
    final String table1 = "insertTable2";

    try (AutoCloseable c = enableIcebergTables()) {
      final String table1Create = String.format("CREATE TABLE %s.%s(id int, code int) partition by (code)",
          TEMP_SCHEMA, table1);
      test(table1Create);

      Thread.sleep(1001);

      test(String.format("INSERT INTO %s.%s VALUES(1, 1)", TEMP_SCHEMA, table1));
      Thread.sleep(1001);
      test(String.format("INSERT INTO %s.%s VALUES(1, 2)", TEMP_SCHEMA, table1));
      Thread.sleep(1001);
      test(String.format("INSERT INTO %s.%s VALUES(1, 3)", TEMP_SCHEMA, table1));
      Thread.sleep(1001);
      test(String.format("INSERT INTO %s.%s VALUES(1, 4)", TEMP_SCHEMA, table1));
      Thread.sleep(1001);
      test(String.format("INSERT INTO %s.%s VALUES(1, 5)", TEMP_SCHEMA, table1));

      // same data in table1 and table2
      testBuilder()
          .sqlQuery(String.format("select id, code from %s.%s", TEMP_SCHEMA, table1))
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
      Table icebergTable1 = new HadoopTables(new Configuration()).load(table1Folder.getPath());
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
  public void insertIntoFewCols() throws Exception {
    final String newTable = "insert_select_cols1";
    try (AutoCloseable c = enableIcebergTables()) {
      String table = String.format("create table %s.%s(id int, name varchar)", TEMP_SCHEMA, newTable);
      test(table);
      String insertTable1 = String.format("insert into %s.%s(id, name) select * from (values(1, 'name1'))", TEMP_SCHEMA, newTable);
      test(insertTable1);
      Thread.sleep(1001);
      String insertTable2 = String.format("insert into %s.%s(name) select * from (values('name2'))", TEMP_SCHEMA, newTable);
      test(insertTable2);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable)
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
  public void insertIntoFewColsDuplicate() throws Exception {
    final String newTable = "insert_select_cols2";
    try (AutoCloseable c = enableIcebergTables()) {
      String table = String.format("create table %s.%s(id int, name varchar)", TEMP_SCHEMA, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(id, id) select id, id from %s.%s", TEMP_SCHEMA, newTable, TEMP_SCHEMA, newTable);
      errorMsgTestHelper(insertTable, "Duplicate column name [id]");
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsTypeSpecified() throws Exception {
    final String newTable = "insert_select_cols2002";
    try (AutoCloseable c = enableIcebergTables()) {
      String table = String.format("create table %s.%s(id int, name varchar)", TEMP_SCHEMA, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(id int, name varchar) select id, name from %s.%s", TEMP_SCHEMA, newTable, TEMP_SCHEMA, newTable);
      errorMsgTestHelper(insertTable, "Column type specified");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsNonExistingCol() throws Exception {
    final String newTable = "insert_select_cols3";
    try (AutoCloseable c = enableIcebergTables()) {
      String table = String.format("create table %s.%s(id int, name varchar)", TEMP_SCHEMA, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(id, id1, id2) select id, id, id from %s.%s", TEMP_SCHEMA, newTable, TEMP_SCHEMA, newTable);
      errorMsgTestHelper(insertTable, "Specified column(s) [id2, id1] not found in schema.");
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsSchemaMismatch() throws Exception {
    final String newTable = "insert_select_cols4";
    try (AutoCloseable c = enableIcebergTables()) {
      String table = String.format("create table %s.%s(id int, name varchar)", TEMP_SCHEMA, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(id) select name from %s.%s", TEMP_SCHEMA, newTable, TEMP_SCHEMA, newTable);
      errorMsgTestHelper(insertTable, "Table schema(id::int32) doesn't match with query schema(name::varchar)");
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsOrderedFields() throws Exception {
    final String newTable = "insert_select_cols5";
    try (AutoCloseable c = enableIcebergTables()) {
      String table = String.format("create table %s.%s(id int, name varchar, address varchar, pin int)", TEMP_SCHEMA, newTable);
      test(table);

      String insertTable = String.format("insert into %s.%s values(1, 'name1', 'address1', 1)", TEMP_SCHEMA, newTable);
      test(insertTable);
      Thread.sleep(1001);
      String insertTable2 = String.format("insert into %s.%s(name, pin) values('name2', 2)", TEMP_SCHEMA, newTable);
      test(insertTable2);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable)
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
  public void insertIntoFewColsUnorderedFields() throws Exception {
    final String newTable = "insert_select_cols005";
    try (AutoCloseable c = enableIcebergTables()) {
      String table = String.format("create table %s.%s(id int, name varchar)", TEMP_SCHEMA, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(name, id) values('name1', 1)", TEMP_SCHEMA, newTable);
      test(insertTable);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable)
          .baselineColumns("id", "name")
          .baselineValues(1, "name1")
          .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void insertIntoFewColsUppromotableTypes() throws Exception {
    final String newTable = "insert_select_cols6";
    try (AutoCloseable c = enableIcebergTables()) {
      String table = String.format("create table %s.%s(id float, name varchar)", TEMP_SCHEMA, newTable);
      test(table);
      Thread.sleep(1001);
      String insertTable = String.format("insert into %s.%s(id, name) values(1, 'name1')", TEMP_SCHEMA, newTable);
      test(insertTable);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable)
          .baselineColumns("id", "name")
          .baselineValues(1.0f, "name1")
          .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable));
    }
  }

  @Test
  public void testFewColsUpPromotableInsert() throws Exception {
    final String newTable = "insert_few_cols7";
    try (AutoCloseable c = enableIcebergTables()) {
      String table1 = "create table " + TEMP_SCHEMA + "." + newTable + "_1" +
          "(zcol1 boolean, zcol3 date, zcol4 float, zcol5 decimal(10,3), " +
          "zcol6 double, zcol7 int, zcol8 bigint, zcol9 time, zcol10 timestamp, zcol11 varchar)";
      test(table1);
      Thread.sleep(1001);
      String insertTable = "insert into " + TEMP_SCHEMA + "." + newTable + "_1(zcol1, zcol3, zcol4, zcol5, zcol6, " +
          "zcol7, zcol8, zcol9, zcol10, zcol11)" +  " select * from (" +
          "values(true, '2019-10-27', cast(0.3 as float), cast(12345.34 as decimal(7,2)), " +
          "cast(3.6 as double), 1, 123456, '12:00:34', 1230768000000, 'abcd'))";
      test(insertTable);
      Thread.sleep(1001);
      String table2 = "create table " + TEMP_SCHEMA + "." + newTable + "_2" +
          "(col1 bigint, col2 decimal(20,5), col3 double)";
      test(table2);
      Thread.sleep(1001);
      String insertUppromoting = "insert into " + TEMP_SCHEMA + "." + newTable + "_2(col1, col2, col3)" +
          " select zcol7, zcol5, zcol4 from " + TEMP_SCHEMA + "." + newTable + "_1";
      test(insertUppromoting);
      Thread.sleep(1001);
      testBuilder()
          .unOrdered()
          .sqlQuery("select * from " + TEMP_SCHEMA + "." + newTable + "_2")
          .baselineColumns("col1", "col2", "col3")
          .baselineValues(new Long("1"), new BigDecimal("12345.340"), new Double("0.3"))
          .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_1"));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTable + "_2"));
    }
  }

}
