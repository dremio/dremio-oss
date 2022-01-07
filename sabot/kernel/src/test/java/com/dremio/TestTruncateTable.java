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
package com.dremio;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;

public class TestTruncateTable extends PlanTestBase {

  protected static final String TEMP_SCHEMA = "dfs_test";

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void truncateInvalidSQL() throws Exception {
    String truncSql = "TRUNCATE";
    expectedEx.expect(UserException.class);
    expectedEx.expectMessage("PARSE ERROR: Failure parsing the query.");
    test(truncSql);
  }

  @Test
  public void icebergNotEnabledShouldThrowError() throws Exception {
    String truncSql = "TRUNCATE TABLE truncTable7";
    expectedEx.expect(UserException.class);
    expectedEx.expectMessage("Please contact customer support for steps to enable the iceberg tables feature.");
    test(truncSql);
  }

  @Test
  public void tableDoesNotExistShouldThrowError() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String truncSql = "TRUNCATE TABLE " + testSchema + ".truncTable6";
      try (AutoCloseable c = enableIcebergTables()) {
        expectedEx.expect(UserException.class);
        expectedEx.expectMessage("Table [" + testSchema + ".truncTable6] not found");
        test(truncSql);
      }
    }
  }

  @Test
  public void tableDoesNotExistWithExistenceCheck() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String truncSql = "TRUNCATE TABLE IF EXISTS " + testSchema + ".truncTable6";
      try (AutoCloseable c = enableIcebergTables()) {
        testBuilder()
          .sqlQuery(truncSql)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, "Table [" + testSchema + ".truncTable6] does not exist.")
          .build()
          .run();
      }
    }
  }

  @Test
  public void nonIcebergTableShouldThrowError() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String ctas = "create table " + testSchema + ".truncTable5 as SELECT * FROM INFORMATION_SCHEMA.CATALOGS";
      test(ctas);
      String truncSql = "TRUNCATE TABLE " + testSchema + ".truncTable5";
      try (AutoCloseable c = enableIcebergTables()) {
        expectedEx.expect(UserException.class);
        expectedEx.expectMessage("Table [" + testSchema + ".truncTable5] is not configured to support DML operations");
        test(truncSql);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), "ctasTable1"));
      }
    }
  }

  @Test
  public void truncateEmptyTable() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "truncTable2";
      try (AutoCloseable c = enableIcebergTables()) {
        String ctas = String.format("create table %s.%s(id int, name varchar)", testSchema, tableName);
        test(ctas);
        String truncSql = String.format("TRUNCATE TABLE %s.%s", testSchema, tableName);
        test(truncSql);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void truncateAndSelect() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "truncTable3";
      try (AutoCloseable c = enableIcebergTables()) {
        String ctas = String.format("create table %s.%s as SELECT * FROM INFORMATION_SCHEMA.CATALOGS", testSchema, tableName);
        test(ctas);

        testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", testSchema, tableName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(1L)
          .build()
          .run();

        Thread.sleep(1001);
        String truncSql = String.format("TRUNCATE %s.%s", testSchema, tableName);
        test(truncSql);

        testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", testSchema, tableName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(0L)
          .build()
          .run();

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void truncateInsertSelect() throws Exception {
    for (String testSchema: SCHEMAS_FOR_TEST) {
      String tableName = "truncTable4";
      try (AutoCloseable c = enableIcebergTables()) {
        String ctas = String.format("create table %s.%s as SELECT * FROM INFORMATION_SCHEMA.CATALOGS", testSchema, tableName);
        test(ctas);
        Thread.sleep(1001);

        String insertSql = String.format("INSERT INTO %s.%s select * FROM INFORMATION_SCHEMA.CATALOGS", testSchema, tableName);
        test(insertSql);
        Thread.sleep(1001);
        test(insertSql);
        Thread.sleep(1001);
        test(insertSql);
        Thread.sleep(1001);

        testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", testSchema, tableName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(4L)
          .build()
          .run();

        String truncSql = String.format("TRUNCATE TABLE %s.%s", testSchema, tableName);
        test(truncSql);
        Thread.sleep(1001);

        testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", testSchema, tableName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(0L)
          .build()
          .run();

        test(insertSql);
        Thread.sleep(1001);

        testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", testSchema, tableName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(1L)
          .build()
          .run();

      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

}
