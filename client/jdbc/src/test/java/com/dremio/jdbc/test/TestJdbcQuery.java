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
package com.dremio.jdbc.test;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.google.common.base.Function;

public class TestJdbcQuery extends JdbcTestQueryBase {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJdbcQuery.class);
  @Rule
  public TestRule TIMEOUT = TestTools.getTimeoutRule(70, TimeUnit.SECONDS);

  // TODO:  Purge nextUntilEnd(...) and calls when remaining fragment race
  // conditions are fixed (not just DRILL-2245 fixes).
  ///**
  // * Calls {@link ResultSet#next} on given {@code ResultSet} until it returns
  // * false.  (For TEMPORARY workaround for query cancelation race condition.)
  // */
  //private void nextUntilEnd(final ResultSet resultSet) throws SQLException {
  //  while (resultSet.next()) {
  //  }
  //}

  @Test // DRILL-3635
  public void testFix3635() throws Exception {
    // When we run a CTAS query, executeQuery() should return after the table has been flushed to disk even though we
    // didn't yet receive a terminal message. To test this, we run CTAS then immediately run a query on the newly
    // created table.

    final String tableName = "dfs_test.\"testDDLs\"";

    try (Connection conn = connect(sabotNode.getJDBCConnectionString())) {
      Statement s = conn.createStatement();
      s.executeQuery(String.format("CREATE TABLE %s AS SELECT * FROM cp.\"employee.json\"", tableName));
    }

    testQuery(String.format("SELECT * FROM %s LIMIT 1", tableName));
  }

  @Test
  @Ignore
  public void testJsonQuery() throws Exception{
    testQuery("select * from cp.\"employee.json\"");
  }

  @Test
  @Ignore
  public void testCast() throws Exception{
    testQuery(String.format("select R_REGIONKEY, cast(R_NAME as varchar(15)) as region, cast(R_COMMENT as varchar(255)) as comment from dfs.\"%s/../../sample-data/region.parquet\"", WORKING_PATH));
  }

  @Test
  @Ignore
  public void testWorkspace() throws Exception{
    testQuery(String.format("select * from dfs_test.home.\"%s/../../sample-data/region.parquet\"", WORKING_PATH));
  }

  @Test
  @Ignore
  public void testWildcard() throws Exception{
    testQuery(String.format("select * from dfs_test.\"%s/../../sample-data/region.parquet\"", WORKING_PATH));
  }

  @Test
  public void testCharLiteral() throws Exception {
    testQuery("select 'test literal' from INFORMATION_SCHEMA.\"TABLES\" LIMIT 1");
  }

  @Test
  public void testDateLiteral() throws Exception {
    // Test result without changing time zone.
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
      .sql("SELECT TO_DATE('2009-07-06', 'YYYY-MM-DD', 1), TO_DATE('12.16.17', 'MM.DD.YY', 1)," +
        " TO_DATE('21/01/00', 'DD/MM/YY', 1), TO_DATE('DEC 25, 1999', 'MON DD, YYYY', 1), DATE '2012-12-23' FROM (VALUES(1))")
      .returns("EXPR$0=2009-07-06; EXPR$1=2017-12-16; EXPR$2=2000-01-21; EXPR$3=1999-12-25; EXPR$4=2012-12-23");
  }

  @Test
  public void testDateWithTimezoneChange() throws Exception {
    // Test result with changing time zones.
    TimeZone currentTZ = TimeZone.getDefault();

    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT TO_DATE('2009-07-06', 'YYYY-MM-DD', 1), TO_DATE('12.16.17', 'MM.DD.YY', 1)," +
          " TO_DATE('21/01/00', 'DD/MM/YY', 1), TO_DATE('DEC 25, 1999', 'MON DD, YYYY', 1) FROM (VALUES(1))")
        .returns("EXPR$0=2009-07-06; EXPR$1=2017-12-16; EXPR$2=2000-01-21; EXPR$3=1999-12-25");

      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT TO_DATE('2009-07-06', 'YYYY-MM-DD', 1), TO_DATE('12.16.17', 'MM.DD.YY', 1)," +
          " TO_DATE('21/01/00', 'DD/MM/YY', 1), TO_DATE('DEC 25, 1999', 'MON DD, YYYY', 1) FROM (VALUES(1))")
        .returns("EXPR$0=2009-07-06; EXPR$1=2017-12-16; EXPR$2=2000-01-21; EXPR$3=1999-12-25");
    } finally {
      TimeZone.setDefault(currentTZ);
    }
  }

  @Test
  public void testTimeLiteral() throws Exception {
    // Test result without changing time zone.
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
      .sql("SELECT TO_TIME('09:15', 'HH:MI', 1)," +
        " TO_TIME('16:20', 'HH24:MI', 1), TO_TIME('16:30:59', 'HH24:MI:SS', 1), TIME '23:00:59'  FROM (VALUES(1))")
      .returns("EXPR$0=09:15:00; EXPR$1=16:20:00; EXPR$2=16:30:59; EXPR$3=23:00:59");
  }

  @Test
  public void testTimeWithTimezoneChange() throws Exception {
    // Test result with changing time zones.
    TimeZone currentTZ = TimeZone.getDefault();

    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT TO_TIME('09:15', 'HH:MI', 1)," +
          " TO_TIME('16:20', 'HH24:MI', 1), TO_TIME('16:30:59', 'HH24:MI:SS', 1)  FROM (VALUES(1))")
        .returns("EXPR$0=09:15:00; EXPR$1=16:20:00; EXPR$2=16:30:59");

      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT TO_TIME('09:15', 'HH:MI', 1)," +
          " TO_TIME('16:20', 'HH24:MI', 1), TO_TIME('16:30:59', 'HH24:MI:SS', 1)  FROM (VALUES(1))")
        .returns("EXPR$0=09:15:00; EXPR$1=16:20:00; EXPR$2=16:30:59");
    } finally {
      TimeZone.setDefault(currentTZ);
    }
  }

  @Test
  public void testTimestampLiteral() throws Exception {
    // Test result without changing time zone.
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
      .sql("SELECT TO_TIMESTAMP('2014-05-10 01:59:10', 'YYYY-MM-DD HH24:MI:SS', 1)," +
        " TIMESTAMP '1990-12-26 12:33:01' FROM (VALUES(1))")
      .returns("EXPR$0=2014-05-10 01:59:10.0; EXPR$1=1990-12-26 12:33:01.0");
  }

  @Test
  public void testTimestampWithTimezoneChange() throws Exception {
    // Test result with changing time zones.
    TimeZone currentTZ = TimeZone.getDefault();

    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT TO_TIMESTAMP('2014-05-10 01:59:10', 'YYYY-MM-DD HH24:MI:SS', 1) FROM (VALUES(1))")
        .returns("EXPR$0=2014-05-10 01:59:10.0");

      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT TO_TIMESTAMP('2014-05-10 01:59:10', 'YYYY-MM-DD HH24:MI:SS', 1) FROM (VALUES(1))")
        .returns("EXPR$0=2014-05-10 01:59:10.0");
    } finally {
      TimeZone.setDefault(currentTZ);
    }
  }

  @Test
  public void testVarCharLiteral() throws Exception {
    testQuery("select cast('test literal' as VARCHAR) from INFORMATION_SCHEMA.\"TABLES\" LIMIT 1");
  }

  @Test
  @Ignore
  public void testLogicalExplain() throws Exception{
    testQuery(String.format("EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR select * from dfs_test.\"%s/../../sample-data/region.parquet\"", WORKING_PATH));
  }

  @Test
  @Ignore
  public void testPhysicalExplain() throws Exception{
    testQuery(String.format("EXPLAIN PLAN FOR select * from dfs_test.\"%s/../../sample-data/region.parquet\"", WORKING_PATH));
  }

  @Test
  @Ignore
  public void checkUnknownColumn() throws Exception{
    testQuery(String.format("SELECT unknownColumn FROM dfs_test.\"%s/../../sample-data/region.parquet\"", WORKING_PATH));
  }

  @Test
  public void testLikeNotLike() throws Exception{
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
      .sql("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
        "WHERE TABLE_NAME NOT LIKE 'C%' AND COLUMN_NAME LIKE 'TABLE_%E'")
      .returns(
        "TABLE_NAME=TABLES; COLUMN_NAME=TABLE_NAME\n" +
        "TABLE_NAME=TABLES; COLUMN_NAME=TABLE_TYPE\n" +
        "TABLE_NAME=VIEWS; COLUMN_NAME=TABLE_NAME\n"
      );
  }

  @Test
  public void testSimilarNotSimilar() throws Exception{
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
      .sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.\"TABLES\" "+
        "WHERE TABLE_NAME SIMILAR TO '%(H|I)E%' AND TABLE_NAME NOT SIMILAR TO 'C%'")
      .returns(
        "TABLE_NAME=SCHEMATA\n" +
        "TABLE_NAME=VIEWS\n"
      );
  }


  @Test
  public void testIntegerLiteral() throws Exception{
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
      .sql("select substring('asd' from 1 for 2) from INFORMATION_SCHEMA.\"TABLES\" limit 1")
      .returns("EXPR$0=as\n");
  }

  @Test
  public void testNullOpForNullableType() throws Exception{
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT * FROM cp.\"test_null_op.json\" WHERE intType IS NULL AND varCharType IS NOT NULL")
        .returns("intType=null; varCharType=val2");
  }

  @Test
  public void testNullOpForNonNullableType() throws Exception{
    // output of (intType IS NULL) is a non-nullable type
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT * FROM cp.\"test_null_op.json\" "+
            "WHERE (intType IS NULL) IS NULL AND (varCharType IS NOT NULL) IS NOT NULL")
        .returns("");
  }

  @Test
  public void testTrueOpForNullableType() throws Exception{
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT data FROM cp.\"test_true_false_op.json\" WHERE booleanType IS TRUE")
        .returns("data=set to true");

    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT data FROM cp.\"test_true_false_op.json\" WHERE booleanType IS FALSE")
        .returns("data=set to false");

    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT data FROM cp.\"test_true_false_op.json\" WHERE booleanType IS NOT TRUE")
        .returns(
            "data=set to false\n" +
            "data=not set"
        );

    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT data FROM cp.\"test_true_false_op.json\" WHERE booleanType IS NOT FALSE")
        .returns(
            "data=set to true\n" +
            "data=not set"
        );
  }

  @Test
  public void testTrueOpForNonNullableType() throws Exception{
    // Output of IS TRUE (and others) is a Non-nullable type
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT data FROM cp.\"test_true_false_op.json\" WHERE (booleanType IS TRUE) IS TRUE")
        .returns("data=set to true");
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT data FROM cp.\"test_true_false_op.json\" WHERE (booleanType IS FALSE) IS FALSE")
        .returns(
            "data=set to true\n" +
            "data=not set"
        );
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT data FROM cp.\"test_true_false_op.json\" WHERE (booleanType IS NOT TRUE) IS NOT TRUE")
        .returns("data=set to true");

    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT data FROM cp.\"test_true_false_op.json\" WHERE (booleanType IS NOT FALSE) IS NOT FALSE")
        .returns(
            "data=set to true\n" +
            "data=not set"
        );
  }

  @Test
  public void testDateTimeAccessors() throws Exception{
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString()).withConnection(new Function<Connection, Void>() {
      @Override
      public Void apply(Connection connection) {
        try {
          final Statement statement = connection.createStatement();

          // show tables on view
          final ResultSet resultSet = statement.executeQuery(
              "select date '2008-2-23', time '12:23:34', timestamp '2008-2-23 12:23:34.456', " +
              "interval '1' year, interval '2' day, " +
              "date_add(date '2008-2-23', interval '1 10:20:30' day to second), " +
              "date_add(date '2010-2-23', 1) " +
              "from cp.\"employee.json\" limit 1");

          resultSet.next();
          final java.sql.Date date = resultSet.getDate(1);
          final java.sql.Time time = resultSet.getTime(2);
          final java.sql.Timestamp ts = resultSet.getTimestamp(3);
          final String intervalYear = resultSet.getString(4);
          final String intervalDay  = resultSet.getString(5);
          final java.sql.Timestamp ts1 = resultSet.getTimestamp(6);
          final java.sql.Date date1 = resultSet.getDate(7);

          final java.sql.Timestamp result = java.sql.Timestamp.valueOf("2008-2-24 10:20:30");
          final java.sql.Date result1 = java.sql.Date.valueOf("2010-2-24");
          assertEquals(ts1, result);
          assertEquals(date1, result1);

          System.out.println("Date: " + date.toString() + " time: " + time.toString() + " timestamp: " + ts.toString() +
                             "\ninterval year: " + intervalYear + " intervalDay: " + intervalDay +
                             " date_interval_add: " + ts1.toString() + "date_int_add: " + date1.toString());

          // TODO:  Purge nextUntilEnd(...) and calls when remaining fragment
          // race conditions are fixed (not just DRILL-2245 fixes).
          // nextUntilEnd(resultSet);
          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testVerifyMetadata() throws Exception{
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString()).withConnection(new Function<Connection, Void>() {
      @Override
      public Void apply(Connection connection) {
        try {
          final Statement statement = connection.createStatement();

          // show files
          final ResultSet resultSet = statement.executeQuery(
              "select timestamp '2008-2-23 12:23:23', date '2001-01-01' from cp.\"employee.json\" limit 1");

          assertEquals( Types.TIMESTAMP, resultSet.getMetaData().getColumnType(1) );
          assertEquals( Types.DATE, resultSet.getMetaData().getColumnType(2) );

          System.out.println(JdbcAssert.toString(resultSet));
          resultSet.close();
          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Test
  public void testCaseWithNoElse() throws Exception {
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT employee_id, CASE WHEN employee_id < 100 THEN first_name END from cp.\"employee.json\" " +
            "WHERE employee_id = 99 OR employee_id = 100")
        .returns(
            "employee_id=99; EXPR$1=Elizabeth\n" +
            "employee_id=100; EXPR$1=null\n"
        );
  }

  @Test
  public void testCaseWithElse() throws Exception {
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT employee_id, CASE WHEN employee_id < 100 THEN first_name ELSE 'Test' END from cp.\"employee.json\" " +
            "WHERE employee_id = 99 OR employee_id = 100")
        .returns(
            "employee_id=99; EXPR$1=Elizabeth\n" +
            "employee_id=100; EXPR$1=Test"
        );
  }

  @Test
  public void testCaseWith2ThensAndNoElse() throws Exception {
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT employee_id, CASE WHEN employee_id < 100 THEN first_name WHEN employee_id = 100 THEN last_name END " +
            "from cp.\"employee.json\" " +
            "WHERE employee_id = 99 OR employee_id = 100 OR employee_id = 101")
        .returns(
            "employee_id=99; EXPR$1=Elizabeth\n" +
            "employee_id=100; EXPR$1=Hunt\n" +
            "employee_id=101; EXPR$1=null"
        );
  }

  @Test
  public void testCaseWith2ThensAndElse() throws Exception {
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT employee_id, CASE WHEN employee_id < 100 THEN first_name WHEN employee_id = 100 THEN last_name ELSE 'Test' END " +
            "from cp.\"employee.json\" " +
            "WHERE employee_id = 99 OR employee_id = 100 OR employee_id = 101")
        .returns(
            "employee_id=99; EXPR$1=Elizabeth\n" +
            "employee_id=100; EXPR$1=Hunt\n" +
            "employee_id=101; EXPR$1=Test\n"
        );
  }

  @Test
  public void testAggWithDremioFunc() throws Exception {
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT extract(year from max(to_timestamp(hire_date, 'YYYY-MM-DD HH24:MI:SS.FFF' ))) as MAX_YEAR " +
            "from cp.\"employee.json\" ")
        .returns(
            "MAX_YEAR=1998\n"
        );
  }

  @Test
  public void testLeftRightReplace() throws Exception {
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("SELECT \"left\"('abcdef', 2) as LEFT_STR, \"right\"('abcdef', 2) as RIGHT_STR, \"replace\"('abcdef', 'ab', 'zz') as REPLACE_STR " +
            "from cp.\"employee.json\" limit 1")
        .returns(
            "LEFT_STR=ab; " +
                "RIGHT_STR=ef; " +
                "REPLACE_STR=zzcdef\n"
        );
  }

  @Test
  public void testLengthUTF8VarCharInput() throws Exception {
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("select length('Sheri', 'UTF8') as L_UTF8 " +
            "from cp.\"employee.json\" where employee_id = 1")
        .returns(
            "L_UTF8=5\n"
       );
  }

  @Test
  public void testTimeIntervalAddOverflow() throws Exception {
    JdbcAssert.withNoDefaultSchema(sabotNode.getJDBCConnectionString())
        .sql("select extract(hour from (interval '10 20' day to hour + time '10:00:00')) as TIME_INT_ADD " +
            "from cp.\"employee.json\" where employee_id = 1")
        .returns(
            "TIME_INT_ADD=6\n"
        );
  }
}
