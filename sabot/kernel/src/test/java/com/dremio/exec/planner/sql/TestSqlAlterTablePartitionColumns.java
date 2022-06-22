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

import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.SqlAlterTablePartitionColumns;
import com.google.common.collect.ImmutableList;

public class TestSqlAlterTablePartitionColumns {
  private final ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100,
    PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  @Test
  public void testAddPartitionColumn() {
    String sql = "ALTER TABLE t1 ADD PARTITION FIELD x";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.IDENTITY);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.ADD);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testAddIdentityPartitionTransform() {
    String sql = "ALTER TABLE t1 ADD PARTITION FIELD identity(x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.IDENTITY);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.ADD);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testAddYearPartitionTransform() {
    String sql = "ALTER TABLE t1 ADD PARTITION FIELD year(x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.YEAR);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.ADD);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testAddMonthPartitionTransform() {
    String sql = "ALTER TABLE t1 ADD PARTITION FIELD month(x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.MONTH);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.ADD);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testAddDayPartitionTransform() {
    String sql = "ALTER TABLE t1 ADD PARTITION FIELD day(x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.DAY);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.ADD);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testAddHourPartitionTransform() {
    String sql = "ALTER TABLE t1 ADD PARTITION FIELD hour(x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.HOUR);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.ADD);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testAddBucketPartitionTransform() {
    String sql = "ALTER TABLE t1 ADD PARTITION FIELD bucket(42, x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.BUCKET,
      ImmutableList.of(42));

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.ADD);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testAddTruncatePartitionTransform() {
    String sql = "ALTER TABLE t1 ADD PARTITION FIELD truncate(42, x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.TRUNCATE,
      ImmutableList.of(42));

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.ADD);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testDropPartitionColumn() {
    String sql = "ALTER TABLE t1 DROP PARTITION FIELD x";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.IDENTITY);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.DROP);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testDropIdentityPartitionTransform() {
    String sql = "ALTER TABLE t1 DROP PARTITION FIELD identity(x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.IDENTITY);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.DROP);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testDropYearPartitionTransform() {
    String sql = "ALTER TABLE t1 DROP PARTITION FIELD year(x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.YEAR);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.DROP);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testDropMonthPartitionTransform() {
    String sql = "ALTER TABLE t1 DROP PARTITION FIELD month(x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.MONTH);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.DROP);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testDropDayPartitionTransform() {
    String sql = "ALTER TABLE t1 DROP PARTITION FIELD day(x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.DAY);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.DROP);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testDropHourPartitionTransform() {
    String sql = "ALTER TABLE t1 DROP PARTITION FIELD hour(x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.HOUR);

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.DROP);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testDropBucketPartitionTransform() {
    String sql = "ALTER TABLE t1 DROP PARTITION FIELD bucket(42, x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.BUCKET,
      ImmutableList.of(42));

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.DROP);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testDropTruncatePartitionTransform() {
    String sql = "ALTER TABLE t1 DROP PARTITION FIELD truncate(42, x)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    PartitionTransform expected = new PartitionTransform("x", PartitionTransform.Type.TRUNCATE,
      ImmutableList.of(42));

    assertThat(alter.getMode()).isEqualTo(SqlAlterTablePartitionColumns.Mode.DROP);
    assertThat(alter.getPartitionTransform()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void testBucketPartitionTransformWithBadArgsFails() {
    String sql ="ALTER TABLE t1 ADD PARTITION FIELD bucket('asdf', name)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    assertThatThrownBy(alter::getPartitionTransform)
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Invalid arguments for partition transform");
  }

  @Test
  public void testTruncatePartitionTransformWithBadArgsFails() {
    String sql ="ALTER TABLE t1 DROP PARTITION FIELD truncate('asdf', name)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    SqlAlterTablePartitionColumns alter = (SqlAlterTablePartitionColumns) sqlNode;

    assertThatThrownBy(alter::getPartitionTransform)
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Invalid arguments for partition transform");
  }
}
