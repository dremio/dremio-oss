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
package com.dremio.exec.planner.logical;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.math.BigDecimal;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.common.exceptions.UserRemoteException;

public class TestValuesRewrite extends PlanTestBase {

  protected static String finalIcebergMetadataLocation;
  private static AutoCloseable closeable;

  @BeforeClass
  public static void setup() {
    finalIcebergMetadataLocation = getDfsTestTmpSchemaLocation();
    closeable = enableIcebergTables();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation()));
    closeable.close();
  }

  @Test
  public void testSingleDecimalValue() throws Exception {
    String sql = "select * from (values(1.23))";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("1.23"))
      .go();
  }

  @Test
  public void testDecimalValue1() throws Exception {
    String sql = "select * from (values (1.2, 2), (3.2, 4))";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1")
      .baselineValues(new BigDecimal("1.2"), 2)
      .baselineValues(new BigDecimal("3.2"), 4)
      .go();
  }

  @Test
  public void testDecimalValue2() throws Exception {
    String sql = "select * from (values (1.2, 2), (3, 4))";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1")
      .baselineValues(new BigDecimal("1.2"), 2)
      .baselineValues(new BigDecimal("3.0"), 4)
      .go();
  }

  @Test
  public void testDecimalValue3() throws Exception {
    String sql = "select * from (values " +
      "(1.23, 1.2), " +
      "(4.56, 4.5)," +
      "(cast (6.78 as decimal(3,2)), 6.7))";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1")
      .baselineValues(new BigDecimal("1.23"), new BigDecimal("1.2"))
      .baselineValues(new BigDecimal("4.56"), new BigDecimal("4.5"))
      .baselineValues(new BigDecimal("6.78"), new BigDecimal("6.7"))
      .go();
  }

  @Test
  public void testDecimalValue4() throws Exception {
    String sql = "select * from (values " +
      "(1.23, 1.2), " +
      "(4.56, cast (4.5 as decimal(2,1)))," +
      "(cast (6.78 as decimal(3,2)), 6.7))";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1")
      .baselineValues(new BigDecimal("1.23"), new BigDecimal("1.2"))
      .baselineValues(new BigDecimal("4.56"), new BigDecimal("4.5"))
      .baselineValues(new BigDecimal("6.78"), new BigDecimal("6.7"))
      .go();
  }

  @Test
  public void testDecimalValue5() throws Exception {
    String sql = "select * from (values " +
      "(cast (1.23 as decimal(3,2)), cast (4.5 as decimal(2,1))))";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1")
      .baselineValues(new BigDecimal("1.23"), new BigDecimal("4.5"))
      .go();
  }

  @Test
  public void testInsertDecimalValue() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "decimaltest";
      try {
        final String createTableQuery = String.format("create table %s.%s (d decimal(20,10))",
          testSchema, tableName);
        test(createTableQuery);

        Thread.sleep(1001);
        final String insertTableQuery = String.format("insert into %s.%s values(1.23)",
          testSchema, tableName);
        test(insertTableQuery);

        Thread.sleep(1001);
        final String selectQuery = String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
          .sqlQuery(selectQuery)
          .unOrdered()
          .baselineColumns("d")
          .baselineValues(new BigDecimal("1.23"))
          .go();
      } finally {
        deleteQuietly(testSchema, tableName);
      }
    }
  }

  @Test
  public void testInsertMultipleValues() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "insertMultipleValues";
      try {
        final String createTableQuery = String.format("create table %s.%s (col1 int, col2 int)",
          testSchema, tableName);
        test(createTableQuery);

        Thread.sleep(1001);
        final String insertTableQuery = String.format("insert into %s.%s (" +
            "values" +
            "(1, 2)," +
            "(3, null)" +
            ")",
          testSchema, tableName);
        test(insertTableQuery);

        Thread.sleep(1001);
        final String selectQuery = String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
          .sqlQuery(selectQuery)
          .unOrdered()
          .baselineColumns("col1", "col2")
          .baselineValues(1, 2)
          .baselineValues(3, null)
          .go();
      } finally {
        deleteQuietly(testSchema, tableName);
      }
    }
  }

  @Test
  public void testCreateTableAsIcebergWithValues() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "createTableAsIcebergWithValues";
      try {
        final String createTableQuery = String.format("create table %s.%s " +
            "(col1 int, col2 int) store as (type => 'iceberg') as values(1, 2)",
          testSchema, tableName);
        test(createTableQuery);

        Thread.sleep(1001);
        final String selectQuery = String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
          .sqlQuery(selectQuery)
          .unOrdered()
          .baselineColumns("col1", "col2")
          .baselineValues(1, 2)
          .go();
      } finally {
        deleteQuietly(testSchema, tableName);
      }
    }
  }

  @Test
  public void testInsertSomeColumnValues() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "insertSomeColumnValues";
      try {
        final String createTableQuery = String.format("" +
            "create table %s.%s " +
            "(c1 int, c2 varchar, c3 int)",
          testSchema, tableName);
        test(createTableQuery);

        Thread.sleep(1001);
        String insertTableQuery = String.format("insert into %s.%s (values(1))", // c2 and c3 should be null
          testSchema, tableName);
        test(insertTableQuery);
        Thread.sleep(1001);

        insertTableQuery = String.format("insert into %s.%s (values(2,3))", // c3 should be null
          testSchema, tableName);
        test(insertTableQuery);
        Thread.sleep(1001);

        insertTableQuery = String.format("insert into %s.%s (c1, c3) (values(4,5))", // c2 should be null
          testSchema, tableName);
        test(insertTableQuery);
        Thread.sleep(1001);

        assertThatThrownBy( // user exception should be thrown
          () -> test(String.format("insert into %s.%s (values(2,'3',1, 2))",
            testSchema, tableName)))
          .isInstanceOf(UserRemoteException.class);
        Thread.sleep(1001);

        final String selectQuery = String.format("select * from %s.%s", testSchema, tableName);
        testBuilder()
          .sqlQuery(selectQuery)
          .unOrdered()
          .baselineColumns("c1", "c2", "c3")
          .baselineValues(1, null, null)
          .baselineValues(2, "3", null)
          .baselineValues(4, null, 5)
          .go();
      } finally {
        deleteQuietly(testSchema, tableName);
      }
    }
  }

  private void deleteQuietly(String testSchema, String tableName) {
    try {
      test(String.format("drop table %s.%s", testSchema, tableName));
    } catch (Exception ignored) {}
    FileUtils.deleteQuietly(new File(finalIcebergMetadataLocation, tableName));
  }
}
