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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.math.BigDecimal;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.ImmutableList;

public class TestIcebergSchemaEvolution extends BaseTestQuery {

  private ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

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

  private void testColumnRenameComplex(String complex_column_rename_test, String testSchema, IcebergCatalogType catalogType) throws Exception {
    try {
      String createCommandSql = "create table " + testSchema + "." + complex_column_rename_test +
        " as select * from cp.\"/parquet/very_complex.parquet\"";

      String selectQuery1 = "SELECT col2, " +
        "col1[0][0][0].\"f1\"[0][0][0] f1, " +
        "col1[0][0][0].\"f2\".\"sub_f1\"[0][0][0] sub_f1, " +
        "col1[0][0][0].\"f2\".\"sub_f2\"[0][0][0].\"sub_sub_f1\" sub_sub_f1, " +
        "col1[0][0][0].\"f2\".\"sub_f2\"[0][0][0].\"sub_sub_f2\" sub_sub_f2 " +
        "FROM " + testSchema + "." + complex_column_rename_test;

      String selectQuery2 = "SELECT c2, " +
        "c1[0][0][0].\"f1\"[0][0][0] f1, " +
        "c1[0][0][0].\"f2\".\"sub_f1\"[0][0][0] sub_f1, " +
        "c1[0][0][0].\"f2\".\"sub_f2\"[0][0][0].\"sub_sub_f1\" sub_sub_f1, " +
        "c1[0][0][0].\"f2\".\"sub_f2\"[0][0][0].\"sub_sub_f2\" sub_sub_f2 " +
        "FROM " + testSchema + "." + complex_column_rename_test;

      String selectQuery3 = "SELECT * " +
        "FROM " + testSchema + "." + complex_column_rename_test;

      test(createCommandSql);
      Thread.sleep(1001);

      testBuilder()
        .sqlQuery(selectQuery1)
        .ordered()
        .baselineColumns("col2", "f1", "sub_f1", "sub_sub_f1", "sub_sub_f2")
        .baselineValues(3, 1, 2, 1, "abc")
        .build()
        .run();

      changeColumn(testSchema + "." + complex_column_rename_test, "col2", "c2", "int");
      Thread.sleep(1001);

      File rootFolder = new File(getDfsTestTmpSchemaLocation(), complex_column_rename_test);
      IcebergModel icebergModel = getIcebergModel(rootFolder, catalogType);
      icebergModel.renameColumn(
              icebergModel.getTableIdentifier(
                      rootFolder.getPath()),
              "col1",
              "c1");
      Thread.sleep(1001);

      String metadataRefresh = "alter table " + testSchema + "." + complex_column_rename_test +
        " refresh metadata force update";
      test(metadataRefresh);

      testBuilder()
        .sqlQuery(selectQuery2)
        .ordered()
        .baselineColumns("c2", "f1", "sub_f1", "sub_sub_f1", "sub_sub_f2")
        .baselineValues(3, 1, 2, 1, "abc")
        .build()
        .run();

      dropColumn(testSchema + "." + complex_column_rename_test, "c1");
      Thread.sleep(1001);
      String insertQuery = "insert into " + testSchema + "." + complex_column_rename_test +
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
  public void testColumnRenameComplex() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testColumnRenameComplex("complex_column_rename_test", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
    }
  }

  private void testDropColumn(String drop_column_test, String testSchema) throws Exception {
    try {
      String createCommandSql = "create table " + testSchema + "." + drop_column_test +
        " (col1 int, col2 int) ";
      String insertCommand1 = "insert into " +  testSchema + "." + drop_column_test +
        " select * from ( values (1, 1))";
      String selectCommand = "select * from " +  testSchema + "." + drop_column_test;
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
      dropColumn(testSchema + "." + drop_column_test, "col1");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  testSchema + "." + drop_column_test +
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
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), drop_column_test));
    }
  }

  @Test
  public void testDropColumn() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testDropColumn("drop_column_test", TEMP_SCHEMA_HADOOP);
    }
  }

  @Test
  public void testDropPartitionColumn() throws Exception {
    final String tableName = "test_drop_partition_col";
    try (AutoCloseable c = enableIcebergTables()) {
      runSQL(String.format("create table %s.%s (c1 int, p1 int) PARTITION by (p1)", TEMP_SCHEMA_HADOOP, tableName));
      runSQL(String.format("insert into %s.%s values (1,1), (2,2)", TEMP_SCHEMA_HADOOP, tableName));
      runSQL(String.format("alter table %s.%s drop PARTITION FIELD p1", TEMP_SCHEMA_HADOOP, tableName));
      NamespaceService namespaceService = getSabotContext().getNamespaceService(SYSTEM_USERNAME);
      DatasetConfig datasetConfig = namespaceService.getDataset(new NamespaceKey(ImmutableList.of(TEMP_SCHEMA_HADOOP, tableName)));

      assertThat(datasetConfig.getReadDefinition().getPartitionColumnsList()).isNullOrEmpty();
    } finally {
      runSQL(String.format("drop table %s.%s", TEMP_SCHEMA_HADOOP, tableName));
    }
  }

  private void testAddColumn(String add_column_test, String testSchema) throws Exception {
    try {
      String createCommandSql = "create table " + testSchema + "." + add_column_test +
        " (col1 int) ";
      String insertCommand1 = "insert into " +  testSchema + "." + add_column_test +
        " select * from ( values (1))";
      String selectCommand = "select * from " +  testSchema + "." + add_column_test;
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
      addColumn(testSchema + "." + add_column_test, "col2", "int");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  testSchema + "." + add_column_test +
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
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), add_column_test));
    }
  }

  @Test
  public void testAddColumn() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testAddColumn("add_column_test", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testDropAndAddColumn(String drop_and_add_test, String testSchema) throws Exception {
    try {
      String createCommandSql = "create table " + testSchema + "." + drop_and_add_test +
        " (col1 int, col2 int) ";
      String insertCommand1 = "insert into " +  testSchema + "." + drop_and_add_test +
        " select * from ( values (1, 1))";
      String selectCommand = "select * from " +  testSchema + "." + drop_and_add_test;
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
      dropColumn(testSchema + "." + drop_and_add_test, "col1");
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .build()
        .run();
      addColumn(testSchema + "." + drop_and_add_test, "col3", "int");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  testSchema + "." + drop_and_add_test +
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
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), drop_and_add_test));
    }
  }

  @Test
  public void testDropAndAddColumn() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testDropAndAddColumn("drop_and_add_test", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testDropAndAddSameColumn(String drop_and_add_same_column_test, String testSchema) throws Exception {
    try {
      String createCommandSql = "create table " + testSchema + "." + drop_and_add_same_column_test +
        " (col1 int, col2 int) ";
      String insertCommand1 = "insert into " +  testSchema + "." + drop_and_add_same_column_test +
        " select * from ( values (1, 1))";
      String selectCommand = "select * from " +  testSchema + "." + drop_and_add_same_column_test;
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
      dropColumn(testSchema + "." + drop_and_add_same_column_test, "col1");
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .build()
        .run();
      addColumn(testSchema + "." + drop_and_add_same_column_test, "col1", "int");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  testSchema + "." + drop_and_add_same_column_test +
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
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), drop_and_add_same_column_test));
    }
  }

  @Test
  public void testDropAndAddSameColumn() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testDropAndAddSameColumn("drop_and_add_same_column_test", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testSimplePushDownWithRename(String simple_pushdown_test, String testSchema) throws Exception {
    try {
      String createCommandSql = "create table " + testSchema + "." + simple_pushdown_test +
        " (col1 int, col2 int) ";
      String insertCommand1 = "insert into " +  testSchema + "." + simple_pushdown_test +
        " select * from ( values (1, 1))";
      String selectCommand = "select * from " +  testSchema + "." + simple_pushdown_test;
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
      dropColumn(testSchema + "." + simple_pushdown_test, "col1");
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("col2")
        .baselineValues(1)
        .build()
        .run();
      addColumn(testSchema + "." + simple_pushdown_test, "col1", "int");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  testSchema + "." + simple_pushdown_test +
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
      changeColumn(testSchema + "." + simple_pushdown_test, "col2", "c2", "int");
      Thread.sleep(1001);
      testBuilder()
        .sqlQuery("select col1, c2 from " + testSchema + "." + simple_pushdown_test + " where c2 = 1")
        .unOrdered()
        .baselineColumns("col1", "c2")
        .baselineValues(null, 1)
        .build()
        .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), simple_pushdown_test));
    }
  }

  @Test
  public void testSimplePushDownWithRename() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testSimplePushDownWithRename("simple_pushdown_test", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testUpPromote(String up_promote_test, String testSchema) throws Exception {
    try {
      String createCommandSql = "create table " + testSchema + "." + up_promote_test +
        " (col1 int, col2 float, col3 decimal(4, 3)) ";
      String insertCommand1 = "insert into " +  testSchema + "." + up_promote_test +
        " select * from ( values (1, cast(1.0 as float), cast(1.0 as decimal(4,3))))";
      String selectCommand = "select * from " +  testSchema + "." + up_promote_test;
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
      changeColumn(testSchema + "." + up_promote_test, "col1", "c1", "bigint");
      Thread.sleep(1001);
      changeColumn(testSchema + "." + up_promote_test, "col2", "c2", "double");
      Thread.sleep(1001);
      changeColumn(testSchema + "." + up_promote_test, "col3", "c3", "decimal(5,3)");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  testSchema + "." + up_promote_test +
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
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), up_promote_test));
    }
  }

  @Test
  public void testUpPromote() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testUpPromote("up_promote_test", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testUpPromoteAndRename(String up_promote_and_rename_test, String testSchema) throws Exception {
    try {
      String createCommandSql = "create table " + testSchema + "." + up_promote_and_rename_test +
        " (col1 int, col2 float, col3 decimal(4, 3)) ";
      String insertCommand1 = "insert into " +  testSchema + "." + up_promote_and_rename_test +
        " select * from ( values (1, cast(1.0 as float), cast(1.0 as decimal(4,3))))";
      String selectCommand = "select * from " +  testSchema + "." + up_promote_and_rename_test;
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
      changeColumn(testSchema + "." + up_promote_and_rename_test, "col1", "temp", "int");
      Thread.sleep(1001);
      changeColumn(testSchema + "." + up_promote_and_rename_test, "col2", "col1", "float");
      Thread.sleep(1001);
      changeColumn(testSchema + "." + up_promote_and_rename_test, "temp", "col2", "int");
      Thread.sleep(1001);
      String insertCommand2 = "insert into " +  testSchema + "." + up_promote_and_rename_test +
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

      changeColumn(testSchema + "." + up_promote_and_rename_test, "col2", "col2", "bigint");
      Thread.sleep(1001);
      changeColumn(testSchema + "." + up_promote_and_rename_test, "col1", "col1", "double");
      Thread.sleep(1001);
      changeColumn(testSchema + "." + up_promote_and_rename_test, "col3", "col3", "decimal(5,3)");
      Thread.sleep(1001);
      String insertCommand3 = "insert into " +  testSchema + "." + up_promote_and_rename_test +
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
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), up_promote_and_rename_test));
    }
  }

  @Test
  public void testUpPromoteAndRename() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testUpPromoteAndRename("up_promote_and_rename_test", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testCaseSensitiveRename(String case_sensitive_test, String testSchema) throws Exception {
    try {
      String createCommandSql = "create table " + testSchema + "." + case_sensitive_test +
        " (col1 int, col2 int) ";
      String insertCommandSql = "insert into  " + testSchema + "." + case_sensitive_test +
        " values (1, 2) ";
      test(createCommandSql);
      Thread.sleep(1001);
      test(insertCommandSql);
      Thread.sleep(1001);
      changeColumn(testSchema + "." + case_sensitive_test, "col1", "COL1", "int");
      Thread.sleep(1001);
      changeColumn(testSchema + "." + case_sensitive_test, "col2", "COL2", "int");
      Thread.sleep(1001);
      test(insertCommandSql);
      Thread.sleep(1001);
      String selectCommand = "select * from " + testSchema + "." + case_sensitive_test;
      testBuilder()
        .sqlQuery(selectCommand)
        .unOrdered()
        .baselineColumns("COL1", "COL2")
        .baselineValues(1, 2)
        .baselineValues(1,2)
        .build()
        .run();
      changeColumn(testSchema + "." + case_sensitive_test, "COL1", "temp", "int");
      Thread.sleep(1001);
      changeColumn(testSchema + "." + case_sensitive_test, "COL2", "Col1", "int");
      Thread.sleep(1001);
      changeColumn(testSchema + "." + case_sensitive_test, "temp", "Col2", "int");
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

  @Test
  public void testCaseSensitiveRename() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      testCaseSensitiveRename("case_sensitive_test", TEMP_SCHEMA_HADOOP);
    }
  }
}
