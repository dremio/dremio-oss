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

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.LocalDateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.BaseTestQuery;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.io.file.Path;

public class TestIcebergSchemaEvolution extends BaseTestQuery {

  private ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  private void changeColumn(String table, String existingColumn, String newColumn, String type) throws Exception{
    String alterTableCmd = "alter table " + table + " change " + existingColumn + " " + newColumn + " " + type;
    test(alterTableCmd);
  }

  private void dropColumn(String table, String existingColumn) throws Exception{
    String alterTableCmd = "alter table " + table + " drop column " + existingColumn;
    test(alterTableCmd);
  }

  private void addColumn(String table, String newColumn, String type) throws Exception{
    String alterTableCmd = "alter table " + table + " add columns (" + newColumn + " " + type + ")";
    test(alterTableCmd);
  }

  @Test
  public void testColumnRenamePrimitive() throws Exception {
    final String primitive_column_rename_test = "primitive_column_rename_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + primitive_column_rename_test +
        " (col1 boolean, col2 int, col3 bigint, col4 float, col5 double, " +
        "col6 decimal(15,3), col7 date, col8 time, col9 timestamp, col10 varchar)";
      String insert1 = "insert into " + TEMP_SCHEMA + "." + primitive_column_rename_test +
        " select * from (values(true, 1, 1, cast(1.0 as float), cast(1.0 as double), " +
        "cast(1.0 as decimal(15,3)), cast('2019-12-25' as date), cast('12:00:00' as time), " +
        "cast('2019-12-25 12:00:00' as timestamp), 'abc'))";
      String insert2 = "insert into " + TEMP_SCHEMA + "." + primitive_column_rename_test +
        " select * from (values(false, 2, 2, cast(2.0 as float), cast(2.0 as double), " +
        "cast(2.0 as decimal(15,3)), cast('2019-12-26' as date), cast('13:00:00' as time), " +
        "cast('2019-12-26 13:00:00' as timestamp), 'def'))";
      String insert3 = "insert into " + TEMP_SCHEMA + "." + primitive_column_rename_test +
        " select * from (values(false, 2, 2, cast(2.0 as float), cast(2.0 as double), " +
        "cast(2.0 as decimal(15,3)), cast('2019-12-26' as date), cast('13:00:00' as time), " +
        "cast('2019-12-26 13:00:00' as timestamp), 'def'))";
      String insert4 = "insert into " + TEMP_SCHEMA + "." + primitive_column_rename_test +
        " select * from (values(false, 3, 3, cast(3.0 as float), cast(3.0 as double), " +
        "cast(3.0 as decimal(15,3)), cast('2019-12-27' as date), cast('14:00:00' as time), " +
        "cast('2019-12-27 14:00:00' as timestamp), 'ghi'))";
      String select1 = "select * from " +  TEMP_SCHEMA + "." + primitive_column_rename_test;
      test(createCommandSql);
      Thread.sleep(1001);
      test(insert1);
      Thread.sleep(1001);
      test(insert2);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(select1)
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10")
        .baselineValues(true, new Integer(1), new Long(1),
          new Float("1.0"), new Double("1.0"), new BigDecimal("1.000"),
          new LocalDateTime(Date.valueOf("2019-12-25").getTime()),
          new LocalDateTime(Time.valueOf("12:00:00").getTime()),
          new LocalDateTime(Timestamp.valueOf("2019-12-25 12:00:00").getTime()),
          "abc")
        .baselineValues(false, new Integer(2), new Long(2),
          new Float("2.0"), new Double("2.0"), new BigDecimal("2.000"),
          new LocalDateTime(Date.valueOf("2019-12-26").getTime()),
          new LocalDateTime(Time.valueOf("13:00:00").getTime()),
          new LocalDateTime(Timestamp.valueOf("2019-12-26 13:00:00").getTime()),
          "def")
        .build()
        .run();
      changeColumn(TEMP_SCHEMA + "." + primitive_column_rename_test, "col10", "col11", "varchar");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + primitive_column_rename_test, "col1", "col10", "boolean");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + primitive_column_rename_test, "col2", "col1", "int");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + primitive_column_rename_test, "col3", "col2", "bigint");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + primitive_column_rename_test, "col4", "col3", "float");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + primitive_column_rename_test, "col5", "col4", "double");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + primitive_column_rename_test, "col6", "col5", "decimal(15,3)");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + primitive_column_rename_test, "col7", "col6", "date");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + primitive_column_rename_test, "col8", "col7", "time");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + primitive_column_rename_test, "col9", "col8", "timestamp");
      Thread.sleep(1001);
      test(insert3);
      Thread.sleep(1001);
      test(insert4);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(select1)
        .unOrdered()
        .baselineColumns("col10", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col11")
        .baselineValues(true, new Integer(1), new Long(1),
          new Float("1.0"), new Double("1.0"), new BigDecimal("1.000"),
          new LocalDateTime(Date.valueOf("2019-12-25").getTime()),
          new LocalDateTime(Time.valueOf("12:00:00").getTime()),
          new LocalDateTime(Timestamp.valueOf("2019-12-25 12:00:00").getTime()),
          "abc")
        .baselineValues(false, new Integer(2), new Long(2),
          new Float("2.0"), new Double("2.0"), new BigDecimal("2.000"),
          new LocalDateTime(Date.valueOf("2019-12-26").getTime()),
          new LocalDateTime(Time.valueOf("13:00:00").getTime()),
          new LocalDateTime(Timestamp.valueOf("2019-12-26 13:00:00").getTime()),
          "def")
        .baselineValues(false, new Integer(2), new Long(2),
          new Float("2.0"), new Double("2.0"), new BigDecimal("2.000"),
          new LocalDateTime(Date.valueOf("2019-12-26").getTime()),
          new LocalDateTime(Time.valueOf("13:00:00").getTime()),
          new LocalDateTime(Timestamp.valueOf("2019-12-26 13:00:00").getTime()),
          "def")
        .baselineValues(false, new Integer(3), new Long(3),
          new Float("3.0"), new Double("3.0"), new BigDecimal("3.000"),
          new LocalDateTime(Date.valueOf("2019-12-27").getTime()),
          new LocalDateTime(Time.valueOf("14:00:00").getTime()),
          new LocalDateTime(Timestamp.valueOf("2019-12-27 14:00:00").getTime()),
          "ghi")
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), primitive_column_rename_test));
    }
  }

  @Test
  public void testColumnRenameComplex() throws Exception {
    final String complex_column_rename_test = "complex_column_rename_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + complex_column_rename_test +
        " as select * from cp.\"/parquet/very_complex.parquet\"";

      String selectQuery1 = "SELECT col2, " +
        "col1[0][0][0].\"f1\"[0][0][0] f1, " +
        "col1[0][0][0].\"f2\".\"sub_f1\"[0][0][0] sub_f1, " +
        "col1[0][0][0].\"f2\".\"sub_f2\"[0][0][0].\"sub_sub_f1\" sub_sub_f1, " +
        "col1[0][0][0].\"f2\".\"sub_f2\"[0][0][0].\"sub_sub_f2\" sub_sub_f2 " +
        "FROM " + TEMP_SCHEMA + "." + complex_column_rename_test;

      String selectQuery2 = "SELECT c2, " +
        "c1[0][0][0].\"f1\"[0][0][0] f1, " +
        "c1[0][0][0].\"f2\".\"sub_f1\"[0][0][0] sub_f1, " +
        "c1[0][0][0].\"f2\".\"sub_f2\"[0][0][0].\"sub_sub_f1\" sub_sub_f1, " +
        "c1[0][0][0].\"f2\".\"sub_f2\"[0][0][0].\"sub_sub_f2\" sub_sub_f2 " +
        "FROM " + TEMP_SCHEMA + "." + complex_column_rename_test;

      String selectQuery3 = "SELECT * " +
        "FROM " + TEMP_SCHEMA + "." + complex_column_rename_test;

      test(createCommandSql);
      Thread.sleep(1001);

      testBuilder()
        .sqlQuery(selectQuery1)
        .ordered()
        .baselineColumns("col2", "f1", "sub_f1", "sub_sub_f1", "sub_sub_f2")
        .baselineValues(3, 1, 2, 1, "abc")
        .build()
        .run();

      changeColumn(TEMP_SCHEMA + "." + complex_column_rename_test, "col2", "c2", "int");
      Thread.sleep(1001);

      IcebergOperation.renameColumn(complex_column_rename_test,
              Path.of(getDfsTestTmpSchemaLocation()).resolve(complex_column_rename_test), "col1",
              "c1", new Configuration());
      Thread.sleep(1001);

      String metadataRefresh = "alter table " + TEMP_SCHEMA + "." + complex_column_rename_test +
        " refresh metadata force update";
      test(metadataRefresh);

      testBuilder()
        .sqlQuery(selectQuery2)
        .ordered()
        .baselineColumns("c2", "f1", "sub_f1", "sub_sub_f1", "sub_sub_f2")
        .baselineValues(3, 1, 2, 1, "abc")
        .build()
        .run();

      dropColumn(TEMP_SCHEMA + "." + complex_column_rename_test, "c1");
      Thread.sleep(1001);
      String insertQuery = "insert into " + TEMP_SCHEMA + "." + complex_column_rename_test +
        " select * from (values(4))";
      test(insertQuery);

      testBuilder()
        .sqlQuery(selectQuery3)
        .unOrdered()
        .baselineColumns("c2")
        .baselineValues(3)
        .baselineValues(4)
        .build()
        .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), complex_column_rename_test));
    }
  }

  @Test
  public void testDropColumn() throws Exception {
    final String drop_column_test = "drop_column_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + drop_column_test +
        " (col1 int, col2 int) ";
      String insertCommand1 = "insert into " +  TEMP_SCHEMA + "." + drop_column_test +
        " select * from ( values (1, 1))";
      String selectCommand = "select * from " +  TEMP_SCHEMA + "." + drop_column_test;
      test(createCommandSql);
      Thread.sleep(1001);
      test(insertCommand1);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, 1)
        .build()
        .run();
      dropColumn(TEMP_SCHEMA + "." + drop_column_test, "col1");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  TEMP_SCHEMA + "." + drop_column_test +
        " select * from ( values (2))";
      test(insertCommand2);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .baselineValues(2)
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), drop_column_test));
    }
  }

  @Test
  public void testAddColumn() throws Exception {
    final String add_column_test = "add_column_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + add_column_test +
        " (col1 int) ";
      String insertCommand1 = "insert into " +  TEMP_SCHEMA + "." + add_column_test +
        " select * from ( values (1))";
      String selectCommand = "select * from " +  TEMP_SCHEMA + "." + add_column_test;
      test(createCommandSql);
      Thread.sleep(1001);
      test(insertCommand1);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .build()
        .run();
      addColumn(TEMP_SCHEMA + "." + add_column_test, "col2", "int");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  TEMP_SCHEMA + "." + add_column_test +
        " select * from ( values (2, 2))";
      test(insertCommand2);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, null)
        .baselineValues(2, 2)
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), add_column_test));
    }
  }

  @Test
  public void testDropAndAddColumn() throws Exception {
    final String drop_and_add_test = "drop_and_add_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + drop_and_add_test +
        " (col1 int, col2 int) ";
      String insertCommand1 = "insert into " +  TEMP_SCHEMA + "." + drop_and_add_test +
        " select * from ( values (1, 1))";
      String selectCommand = "select * from " +  TEMP_SCHEMA + "." + drop_and_add_test;
      test(createCommandSql);
      Thread.sleep(1001);
      test(insertCommand1);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, 1)
        .build()
        .run();
      dropColumn(TEMP_SCHEMA + "." + drop_and_add_test, "col1");
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .build()
        .run();
      addColumn(TEMP_SCHEMA + "." + drop_and_add_test, "col3", "int");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  TEMP_SCHEMA + "." + drop_and_add_test +
        " select * from ( values (2, 2))";
      test(insertCommand2);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2", "col3")
        .baselineValues(1, null)
        .baselineValues(2, 2)
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), drop_and_add_test));
    }
  }

  @Test
  public void testDropAndAddSameColumn() throws Exception {
    final String drop_and_add_same_column_test = "drop_and_add_same_column_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + drop_and_add_same_column_test +
        " (col1 int, col2 int) ";
      String insertCommand1 = "insert into " +  TEMP_SCHEMA + "." + drop_and_add_same_column_test +
        " select * from ( values (1, 1))";
      String selectCommand = "select * from " +  TEMP_SCHEMA + "." + drop_and_add_same_column_test;
      test(createCommandSql);
      Thread.sleep(1001);
      test(insertCommand1);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, 1)
        .build()
        .run();
      dropColumn(TEMP_SCHEMA + "." + drop_and_add_same_column_test, "col1");
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .build()
        .run();
      addColumn(TEMP_SCHEMA + "." + drop_and_add_same_column_test, "col1", "int");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  TEMP_SCHEMA + "." + drop_and_add_same_column_test +
        " select * from ( values (2, 2))";
      test(insertCommand2);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2", "col1")
        .baselineValues(1, null)
        .baselineValues(2, 2)
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), drop_and_add_same_column_test));
    }
  }

  @Test
  public void testSimplePushDownWithRename() throws Exception {
    final String simple_pushdown_test = "simple_pushdown_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + simple_pushdown_test +
        " (col1 int, col2 int) ";
      String insertCommand1 = "insert into " +  TEMP_SCHEMA + "." + simple_pushdown_test +
        " select * from ( values (1, 1))";
      String selectCommand = "select * from " +  TEMP_SCHEMA + "." + simple_pushdown_test;
      test(createCommandSql);
      Thread.sleep(1001);
      test(insertCommand1);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, 1)
        .build()
        .run();
      dropColumn(TEMP_SCHEMA + "." + simple_pushdown_test, "col1");
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .build()
        .run();
      addColumn(TEMP_SCHEMA + "." + simple_pushdown_test, "col1", "int");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  TEMP_SCHEMA + "." + simple_pushdown_test +
        " select * from ( values (2, 2))";
      test(insertCommand2);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2", "col1")
        .baselineValues(1, null)
        .baselineValues(2, 2)
        .build()
        .run();
      changeColumn(TEMP_SCHEMA + "." + simple_pushdown_test, "col2", "c2", "int");
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery("select col1, c2 from " + TEMP_SCHEMA + "." + simple_pushdown_test + " where c2 = 1")
        .unOrdered()
        .baselineColumns("col1", "c2")
        .baselineValues(null, 1)
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), simple_pushdown_test));
    }
  }

  @Test
  public void testUpPromote() throws Exception {
    final String up_promote_test = "up_promote_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + up_promote_test +
        " (col1 int, col2 float, col3 decimal(4, 3)) ";
      String insertCommand1 = "insert into " +  TEMP_SCHEMA + "." + up_promote_test +
        " select * from ( values (1, cast(1.0 as float), cast(1.0 as decimal(4,3))))";
      String selectCommand = "select * from " +  TEMP_SCHEMA + "." + up_promote_test;
      test(createCommandSql);
      Thread.sleep(1001);
      test(insertCommand1);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col1", "col2", "col3")
        .baselineValues(1, new Float("1.0"), new BigDecimal("1.000"))
        .build()
        .run();
      changeColumn(TEMP_SCHEMA + "." + up_promote_test, "col1", "c1", "bigint");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + up_promote_test, "col2", "c2", "double");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + up_promote_test, "col3", "c3", "decimal(5,3)");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  TEMP_SCHEMA + "." + up_promote_test +
        " select * from ( values (2, cast(2.0 as double), cast(22.0 as decimal(5,3))))";
      test(insertCommand2);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("c1", "c2", "c3")
        .baselineValues(1L, new Double("1.0"), new BigDecimal("1.000"))
        .baselineValues(2L, new Double("2.0"), new BigDecimal("22.000"))
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), up_promote_test));
    }
  }

  @Test
  public void testUpPromoteAndRename() throws Exception {
    final String up_promote_and_rename_test = "up_promote_and_rename_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + up_promote_and_rename_test +
        " (col1 int, col2 float, col3 decimal(4, 3)) ";
      String insertCommand1 = "insert into " +  TEMP_SCHEMA + "." + up_promote_and_rename_test +
        " select * from ( values (1, cast(1.0 as float), cast(1.0 as decimal(4,3))))";
      String selectCommand = "select * from " +  TEMP_SCHEMA + "." + up_promote_and_rename_test;
      test(createCommandSql);
      Thread.sleep(1001);
      test(insertCommand1);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col1", "col2", "col3")
        .baselineValues(1, new Float("1.0"), new BigDecimal("1.000"))
        .build()
        .run();
      changeColumn(TEMP_SCHEMA + "." + up_promote_and_rename_test, "col1", "temp", "int");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + up_promote_and_rename_test, "col2", "col1", "float");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + up_promote_and_rename_test, "temp", "col2", "int");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  TEMP_SCHEMA + "." + up_promote_and_rename_test +
        " select * from ( values (2, cast(2.0 as float), cast(2.0 as decimal(4,3))))";
      test(insertCommand2);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2", "col1", "col3")
        .baselineValues(1, new Float("1.0"), new BigDecimal("1.000"))
        .baselineValues(2, new Float("2.0"), new BigDecimal("2.000"))
        .build()
        .run();

      changeColumn(TEMP_SCHEMA + "." + up_promote_and_rename_test, "col2", "col2", "bigint");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + up_promote_and_rename_test, "col1", "col1", "double");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + up_promote_and_rename_test, "col3", "col3", "decimal(5,3)");
      Thread.sleep(1001);
      String insertCommand3 = "insert into " +  TEMP_SCHEMA + "." + up_promote_and_rename_test +
        " select * from ( values (3, cast(3.0 as double), cast(33.0 as decimal(5,3))))";
      test(insertCommand3);
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2", "col1", "col3")
        .baselineValues(1L, new Double("1.0"), new BigDecimal("1.000"))
        .baselineValues(2L, new Double("2.0"), new BigDecimal("2.000"))
        .baselineValues(3L, new Double("3.0"), new BigDecimal("33.000"))
        .build()
        .run();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), up_promote_and_rename_test));
    }
  }

  @Test
  public void testCaseSensitiveRename() throws Exception {
    final String case_sensitive_test = "case_sensitive_test";

    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + case_sensitive_test +
        " (col1 int, col2 int) ";
      String insertCommandSql = "insert into  " + TEMP_SCHEMA + "." + case_sensitive_test +
        " values (1, 2) ";
      test(createCommandSql);
      Thread.sleep(1001);
      test(insertCommandSql);
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + case_sensitive_test, "col1", "COL1", "int");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + case_sensitive_test, "col2", "COL2", "int");
      Thread.sleep(1001);
      test(insertCommandSql);
      Thread.sleep(1001);
      String selectCommand = "select * from " + TEMP_SCHEMA + "." + case_sensitive_test;
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("COL1", "COL2")
        .baselineValues(1, 2)
        .baselineValues(1,2)
        .build()
        .run();
      changeColumn(TEMP_SCHEMA + "." + case_sensitive_test, "COL1", "temp", "int");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + case_sensitive_test, "COL2", "Col1", "int");
      Thread.sleep(1001);
      changeColumn(TEMP_SCHEMA + "." + case_sensitive_test, "temp", "Col2", "int");
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("Col2", "Col1")
        .baselineValues(1, 2)
        .baselineValues(1,2)
        .build()
        .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), case_sensitive_test));
    }
  }
}
