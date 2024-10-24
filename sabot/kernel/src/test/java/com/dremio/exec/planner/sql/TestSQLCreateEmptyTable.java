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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.parser.SqlCreateEmptyTable;
import com.dremio.exec.proto.UserBitShared;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

public class TestSQLCreateEmptyTable {
  private ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100);

  @Test
  public void testCreateTableNoColumns() {
    String sql = "CREATE TABLE newTbl";
    try {
      SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    } catch (UserException ex) {
      Assert.assertEquals("Columns/Fields not specified for table.", ex.getMessage());
    }
  }

  @Test
  public void testCreateTableNoDatatypes() {
    String sql = "CREATE TABLE newTbl(id , name)";
    try {
      SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    } catch (UserException ex) {
      Assert.assertEquals("Datatype not specified for some columns.", ex.getMessage());
    }

    sql = "CREATE TABLE newTbl(id int, name)";
    try {
      SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    } catch (UserException ex) {
      Assert.assertEquals("Datatype not specified for some columns.", ex.getMessage());
    }
  }

  @Test
  public void testCreateTableValidDatatypes() {
    String sql = "CREATE TABLE newTbl(id int, name varchar)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER_DDL)));
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;
    Assert.assertArrayEquals(
        new String[] {"id", "name"}, sqlCreateEmptyTable.getFieldNames().toArray(new String[0]));
  }

  @Test
  public void testCreateTableValidDatatypesWithPartitions() {
    String sql = "CREATE TABLE newTbl(id int, name varchar) PARTITION BY (name)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER_DDL)));
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;
    Assert.assertArrayEquals(
        new String[] {"id", "name"}, sqlCreateEmptyTable.getFieldNames().toArray(new String[0]));
    Assert.assertArrayEquals(
        new String[] {"name"},
        sqlCreateEmptyTable.getPartitionColumns(null).toArray(new String[0]));
  }

  @Test
  public void testCreateTableDecimalDatatype() {
    String sql = "CREATE TABLE newTbl(id DECIMAL(38, 2), name varchar) PARTITION BY (name)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER_DDL)));
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;
    Assert.assertArrayEquals(
        new String[] {"id", "name"}, sqlCreateEmptyTable.getFieldNames().toArray(new String[0]));
    Assert.assertArrayEquals(
        new String[] {"name"},
        sqlCreateEmptyTable.getPartitionColumns(null).toArray(new String[0]));
  }

  @Test
  public void testIdentityPartitionTransform() {
    String sql = "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (identity(name))";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;

    List<PartitionTransform> expected =
        ImmutableList.of(new PartitionTransform("name", PartitionTransform.Type.IDENTITY));
    List<PartitionTransform> partitionTransforms = sqlCreateEmptyTable.getPartitionTransforms(null);

    assertThat(expected).usingRecursiveComparison().isEqualTo(partitionTransforms);
  }

  @Test
  public void testYearPartitionTransform() {
    String sql = "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (year(date1))";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;

    List<PartitionTransform> expected =
        ImmutableList.of(new PartitionTransform("date1", PartitionTransform.Type.YEAR));
    List<PartitionTransform> partitionTransforms = sqlCreateEmptyTable.getPartitionTransforms(null);

    assertThat(expected).usingRecursiveComparison().isEqualTo(partitionTransforms);
  }

  @Test
  public void testMonthPartitionTransform() {
    String sql = "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (month(date1))";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;

    List<PartitionTransform> expected =
        ImmutableList.of(new PartitionTransform("date1", PartitionTransform.Type.MONTH));
    List<PartitionTransform> partitionTransforms = sqlCreateEmptyTable.getPartitionTransforms(null);

    assertThat(expected).usingRecursiveComparison().isEqualTo(partitionTransforms);
  }

  @Test
  public void testDayPartitionTransform() {
    List<PartitionTransform> expected =
        ImmutableList.of(new PartitionTransform("date1", PartitionTransform.Type.DAY));
    List<PartitionTransform> partitionTransforms =
        parseAndGetPartitionTransforms(
            "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (day(date1))");

    assertThat(expected).usingRecursiveComparison().isEqualTo(partitionTransforms);
  }

  @Test
  public void testHourPartitionTransform() {
    List<PartitionTransform> expected =
        ImmutableList.of(new PartitionTransform("ts1", PartitionTransform.Type.HOUR));
    List<PartitionTransform> partitionTransforms =
        parseAndGetPartitionTransforms(
            "CREATE TABLE t1 (ts1 TIMESTAMP, name VARCHAR) PARTITION BY (hour(ts1))");

    assertThat(expected).usingRecursiveComparison().isEqualTo(partitionTransforms);
  }

  @Test
  public void testBucketPartitionTransform() {
    List<PartitionTransform> expected =
        ImmutableList.of(
            new PartitionTransform("name", PartitionTransform.Type.BUCKET, ImmutableList.of(42)));
    List<PartitionTransform> partitionTransforms =
        parseAndGetPartitionTransforms(
            "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (bucket(42, name))");

    assertThat(expected).usingRecursiveComparison().isEqualTo(partitionTransforms);
  }

  @Test
  public void testTruncatePartitionTransform() {
    List<PartitionTransform> expected =
        ImmutableList.of(
            new PartitionTransform("name", PartitionTransform.Type.TRUNCATE, ImmutableList.of(42)));
    List<PartitionTransform> partitionTransforms =
        parseAndGetPartitionTransforms(
            "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (truncate(42, name))");

    assertThat(expected).usingRecursiveComparison().isEqualTo(partitionTransforms);
  }

  @Test
  public void testBucketPartitionTransformWithBadArgsFails() {
    assertThatThrownBy(
            () ->
                parseAndGetPartitionTransforms(
                    "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (bucket(name))"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments for partition transform");

    assertThatThrownBy(
            () ->
                parseAndGetPartitionTransforms(
                    "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (bucket('asdf', name))"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments for partition transform");

    assertThatThrownBy(
            () ->
                parseAndGetPartitionTransforms(
                    "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (bucket(42, 'asdf', name))"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments for partition transform");
  }

  @Test
  public void testTruncatePartitionTransformWithBadArgsFails() {
    assertThatThrownBy(
            () ->
                parseAndGetPartitionTransforms(
                    "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (truncate(name))"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments for partition transform");

    assertThatThrownBy(
            () ->
                parseAndGetPartitionTransforms(
                    "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (truncate('asdf', name))"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments for partition transform");

    assertThatThrownBy(
            () ->
                parseAndGetPartitionTransforms(
                    "CREATE TABLE t1 (date1 DATE, name VARCHAR) PARTITION BY (truncate(42, 'asdf', name))"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments for partition transform");
  }

  @Test
  public void testParseMalformedQueriesWithTableProperties() throws Exception {
    List<String> malformedQueries =
        new ArrayList<String>() {
          {
            add("create table s (a BIGINT) TBLPROPERTIES ()");
            add("create table s (a BIGINT) TBLPROPERTIES");
            add(
                "create table s (a BIGINT) TBLPROPERTIES ('property_name' = 'property_value', 'property_name1' = )");
            add("create table s (a BIGINT) TBLPROPERTIES ('property_name')");
            add("create table s (a BIGINT) TBLPROPERTIES ('property_name', 'property_name1' )");
            add("create table s (a BIGINT) TBLPROPERTIES ('property_name', 'property_name1' = )");
            add(
                "create table s (a BIGINT) TBLPROPERTIES ('property_name' = 'property_value', 'property_name1')");
          }
        };

    for (String malformedQuery : malformedQueries) {
      parseAndVerifyMalFormat(malformedQuery);
    }
  }

  @Test
  public void testParseWellformedQueriesWithTableProperties() throws Exception {
    List<String> wellformedQueries =
        new ArrayList<String>() {
          {
            add("create table s (a BIGINT) TBLPROPERTIES ('property_name' = 'property_value')");
            add(
                "create table s (a BIGINT) TBLPROPERTIES ('property_name' = 'property_value', 'property_name1' = 'property_value1')");
          }
        };

    for (String wellformedQuery : wellformedQueries) {
      parseAndVerifyWellFormat(wellformedQuery);
    }
  }

  @Test
  public void testParseMalformedQueriesWithClusterKeys() throws Exception {
    List<String> malformedQueries =
        new ArrayList<String>() {
          {
            add("create table s (a BIGINT) CLUSTER BY ()");
            add("create table s (a BIGINT) CLUSTER BY");
            add("create table s (a BIGINT) CLUSTER BY (C1,)");
            add("create table s (a BIGINT) CLUSTER BY (,C2)");
          }
        };

    for (String malformedQuery : malformedQueries) {
      parseAndVerifyMalFormat(malformedQuery);
    }
  }

  @Test
  public void testParseWellformedQueriesWithClusterKey() throws Exception {
    List<String> wellformedQueries =
        new ArrayList<String>() {
          {
            add("create table s (a BIGINT) CLUSTER BY (C1)");
            add("create table s (a BIGINT) CLUSTER BY (C1,C2)");
          }
        };

    for (String wellformedQuery : wellformedQueries) {
      parseAndVerifyWellFormat(wellformedQuery);
    }
  }

  private void parseAndVerifyWellFormat(String sql) {
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode instanceof SqlCreateEmptyTable);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER_DDL)));
  }

  private void parseAndVerifyMalFormat(String sql) {
    try {
      SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    } catch (UserException ue) {
      Assert.assertEquals(ue.getErrorType(), UserBitShared.DremioPBError.ErrorType.PARSE);
    }
  }

  @Test
  public void testLocalSortNoTransforms() {
    List<String> expected = ImmutableList.of("col1");
    String sql = "CREATE TABLE t1 (col1 int, col2 date) LOCALSORT BY (col1)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;

    assertThat(expected).usingRecursiveComparison().isEqualTo(sqlCreateEmptyTable.getSortColumns());
  }

  @Test
  public void testClusterKeyNoTransforms() {
    List<String> expected = ImmutableList.of("col1");
    String sql = "CREATE TABLE t1 (col1 int, col2 date) CLUSTER BY (col1)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;

    assertThat(expected).usingRecursiveComparison().isEqualTo(sqlCreateEmptyTable.getClusterKeys());
  }

  private List<PartitionTransform> parseAndGetPartitionTransforms(String sql) {
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;
    return sqlCreateEmptyTable.getPartitionTransforms(null);
  }
}
