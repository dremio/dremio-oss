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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.parser.SqlPartitionTransform;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

public class TestPartitionTransform {

  private static final RelDataTypeFactory TYPE_FACTORY = SqlTypeFactoryImpl.INSTANCE;
  private static final RelDataType ROW_TYPE =
      TYPE_FACTORY.createStructType(
          ImmutableList.of(
              TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
              TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
              TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY),
              TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE),
              TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL),
              TYPE_FACTORY.createSqlType(SqlTypeName.DATE),
              TYPE_FACTORY.createSqlType(SqlTypeName.TIME),
              TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP)),
          ImmutableList.of(
              "int_col",
              "varchar_col",
              "varbinary_col",
              "double_col",
              "decimal_col",
              "date_col",
              "time_col",
              "timestamp_col"));

  @Test
  public void testCreateIdentityTransform() {
    PartitionTransform transform = PartitionTransform.from(createSqlTransform("x"));
    Assert.assertEquals("IDENTITY(x)", transform.toString());
    assertThat(PartitionTransform.Type.IDENTITY).isEqualTo(transform.getType());
    assertThat("x").isEqualTo(transform.getColumnName());
  }

  @Test
  public void testCreateIdentityTransformFailsWithBadArgs() {
    assertThatThrownBy(
            () ->
                PartitionTransform.from(
                    createSqlTransform("x", "identity", ImmutableList.of(createLiteral("foo")))))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
  }

  @Test
  public void testCreateYearTransform() {
    PartitionTransform transform = PartitionTransform.from(createSqlTransform("x", "year"));
    Assert.assertEquals("YEAR(x)", transform.toString());
    assertThat(PartitionTransform.Type.YEAR).isEqualTo(transform.getType());
    assertThat("x").isEqualTo(transform.getColumnName());
  }

  @Test
  public void testCreateYearTransformFailsWithBadArgs() {
    assertThatThrownBy(
            () ->
                PartitionTransform.from(
                    createSqlTransform("x", "year", ImmutableList.of(createLiteral("foo")))))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
  }

  @Test
  public void testCreateMonthTransform() {
    PartitionTransform transform = PartitionTransform.from(createSqlTransform("x", "month"));
    Assert.assertEquals("MONTH(x)", transform.toString());
    assertThat(PartitionTransform.Type.MONTH).isEqualTo(transform.getType());
    assertThat("x").isEqualTo(transform.getColumnName());
  }

  @Test
  public void testCreateMonthTransformFailsWithBadArgs() {
    assertThatThrownBy(
            () ->
                PartitionTransform.from(
                    createSqlTransform("x", "month", ImmutableList.of(createLiteral("foo")))))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
  }

  @Test
  public void testCreateDayTransform() {
    PartitionTransform transform = PartitionTransform.from(createSqlTransform("x", "day"));
    Assert.assertEquals("DAY(x)", transform.toString());
    assertThat(PartitionTransform.Type.DAY).isEqualTo(transform.getType());
    assertThat("x").isEqualTo(transform.getColumnName());
  }

  @Test
  public void testCreateDayTransformFailsWithBadArgs() {
    assertThatThrownBy(
            () ->
                PartitionTransform.from(
                    createSqlTransform("x", "day", ImmutableList.of(createLiteral("foo")))))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
  }

  @Test
  public void testCreateHourTransform() {
    PartitionTransform transform = PartitionTransform.from(createSqlTransform("x", "hour"));
    Assert.assertEquals("HOUR(x)", transform.toString());
    assertThat(PartitionTransform.Type.HOUR).isEqualTo(transform.getType());
    assertThat("x").isEqualTo(transform.getColumnName());
  }

  @Test
  public void testCreateHourTransformFailsWithBadArgs() {
    assertThatThrownBy(
            () ->
                PartitionTransform.from(
                    createSqlTransform("x", "hour", ImmutableList.of(createLiteral("foo")))))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
  }

  @Test
  public void testCreateBucketTransform() {
    PartitionTransform transform =
        PartitionTransform.from(
            createSqlTransform("x", "bucket", ImmutableList.of(createLiteral(42))));
    Assert.assertEquals("BUCKET(42,x)", transform.toString());
    assertThat(PartitionTransform.Type.BUCKET).isEqualTo(transform.getType());
    assertThat("x").isEqualTo(transform.getColumnName());
    assertThat(42).isEqualTo((int) transform.getArgumentValue(0, Integer.class));
  }

  @Test
  public void testCreateBucketTransformFailsWithBadArgs() {
    assertThatThrownBy(
            () -> PartitionTransform.from(createSqlTransform("x", "bucket", ImmutableList.of())))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
    assertThatThrownBy(
            () ->
                PartitionTransform.from(
                    createSqlTransform("x", "bucket", ImmutableList.of(createLiteral("foo")))))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
    assertThatThrownBy(
            () ->
                PartitionTransform.from(
                    createSqlTransform(
                        "x", "bucket", ImmutableList.of(createLiteral(42), createLiteral("foo")))))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
  }

  @Test
  public void testCreateTruncateTransform() {
    PartitionTransform transform =
        PartitionTransform.from(
            createSqlTransform("x", "truncate", ImmutableList.of(createLiteral(42))));
    Assert.assertEquals("TRUNCATE(42,x)", transform.toString());
    assertThat(PartitionTransform.Type.TRUNCATE).isEqualTo(transform.getType());
    assertThat("x").isEqualTo(transform.getColumnName());
    assertThat(42).isEqualTo((int) transform.getArgumentValue(0, Integer.class));
  }

  @Test
  public void testCreateTruncateTransformFailsWithBadArgs() {
    assertThatThrownBy(
            () -> PartitionTransform.from(createSqlTransform("x", "truncate", ImmutableList.of())))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
    assertThatThrownBy(
            () ->
                PartitionTransform.from(
                    createSqlTransform("x", "truncate", ImmutableList.of(createLiteral("foo")))))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
    assertThatThrownBy(
            () ->
                PartitionTransform.from(
                    createSqlTransform(
                        "x",
                        "truncate",
                        ImmutableList.of(createLiteral(42), createLiteral("foo")))))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid arguments");
  }

  @Test
  public void testCaseInsensiveLookup() {
    PartitionTransform transform = PartitionTransform.from(createSqlTransform("x", "YEAR"));
    assertThat(PartitionTransform.Type.YEAR).isEqualTo(transform.getType());
    assertThat("x").isEqualTo(transform.getColumnName());
  }

  @Test
  public void testUnknownTransformFails() {
    assertThatThrownBy(() -> PartitionTransform.from(createSqlTransform("x", "badtransform")))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Unknown partition transform");
  }

  @Test
  public void testSchemaValidationWithYearTransform() {
    assertInvalidColumnForTransform("int_col", PartitionTransform.Type.YEAR);
    assertInvalidColumnForTransform("varchar_col", PartitionTransform.Type.YEAR);
    assertInvalidColumnForTransform("varbinary_col", PartitionTransform.Type.YEAR);
    assertInvalidColumnForTransform("decimal_col", PartitionTransform.Type.YEAR);
    assertInvalidColumnForTransform("double_col", PartitionTransform.Type.YEAR);
    assertValidColumnForTransform("date_col", PartitionTransform.Type.YEAR);
    assertInvalidColumnForTransform("time_col", PartitionTransform.Type.YEAR);
    assertValidColumnForTransform("timestamp_col", PartitionTransform.Type.YEAR);
  }

  @Test
  public void testSchemaValidationWithMonthTransform() {
    assertInvalidColumnForTransform("int_col", PartitionTransform.Type.MONTH);
    assertInvalidColumnForTransform("varchar_col", PartitionTransform.Type.MONTH);
    assertInvalidColumnForTransform("varbinary_col", PartitionTransform.Type.MONTH);
    assertInvalidColumnForTransform("decimal_col", PartitionTransform.Type.MONTH);
    assertInvalidColumnForTransform("double_col", PartitionTransform.Type.MONTH);
    assertValidColumnForTransform("date_col", PartitionTransform.Type.MONTH);
    assertInvalidColumnForTransform("time_col", PartitionTransform.Type.MONTH);
    assertValidColumnForTransform("timestamp_col", PartitionTransform.Type.MONTH);
  }

  @Test
  public void testSchemaValidationWithDayTransform() {
    assertInvalidColumnForTransform("int_col", PartitionTransform.Type.DAY);
    assertInvalidColumnForTransform("varchar_col", PartitionTransform.Type.DAY);
    assertInvalidColumnForTransform("varbinary_col", PartitionTransform.Type.DAY);
    assertInvalidColumnForTransform("decimal_col", PartitionTransform.Type.DAY);
    assertInvalidColumnForTransform("double_col", PartitionTransform.Type.DAY);
    assertValidColumnForTransform("date_col", PartitionTransform.Type.DAY);
    assertInvalidColumnForTransform("time_col", PartitionTransform.Type.DAY);
    assertValidColumnForTransform("timestamp_col", PartitionTransform.Type.DAY);
  }

  @Test
  public void testSchemaValidationWithHourTransform() {
    assertInvalidColumnForTransform("int_col", PartitionTransform.Type.HOUR);
    assertInvalidColumnForTransform("varchar_col", PartitionTransform.Type.HOUR);
    assertInvalidColumnForTransform("varbinary_col", PartitionTransform.Type.HOUR);
    assertInvalidColumnForTransform("decimal_col", PartitionTransform.Type.HOUR);
    assertInvalidColumnForTransform("double_col", PartitionTransform.Type.HOUR);
    assertInvalidColumnForTransform("date_col", PartitionTransform.Type.HOUR);
    assertInvalidColumnForTransform("time_col", PartitionTransform.Type.HOUR);
    assertValidColumnForTransform("timestamp_col", PartitionTransform.Type.HOUR);
  }

  @Test
  public void testSchemaValidationWithBucketTransform() {
    List<Object> args = ImmutableList.of(42);
    assertValidColumnForTransform("int_col", PartitionTransform.Type.BUCKET, args);
    assertValidColumnForTransform("varchar_col", PartitionTransform.Type.BUCKET, args);
    assertValidColumnForTransform("varbinary_col", PartitionTransform.Type.BUCKET, args);
    assertValidColumnForTransform("decimal_col", PartitionTransform.Type.BUCKET, args);
    assertInvalidColumnForTransform("double_col", PartitionTransform.Type.BUCKET, args);
    assertValidColumnForTransform("date_col", PartitionTransform.Type.BUCKET, args);
    assertValidColumnForTransform("time_col", PartitionTransform.Type.BUCKET, args);
    assertValidColumnForTransform("timestamp_col", PartitionTransform.Type.BUCKET, args);
  }

  @Test
  public void testSchemaValidationWithTruncateTransform() {
    List<Object> args = ImmutableList.of(42);
    assertValidColumnForTransform("int_col", PartitionTransform.Type.TRUNCATE, args);
    assertValidColumnForTransform("varchar_col", PartitionTransform.Type.TRUNCATE, args);
    assertInvalidColumnForTransform("varbinary_col", PartitionTransform.Type.TRUNCATE, args);
    assertValidColumnForTransform("decimal_col", PartitionTransform.Type.TRUNCATE, args);
    assertInvalidColumnForTransform("double_col", PartitionTransform.Type.TRUNCATE, args);
    assertInvalidColumnForTransform("date_col", PartitionTransform.Type.TRUNCATE, args);
    assertInvalidColumnForTransform("time_col", PartitionTransform.Type.TRUNCATE, args);
    assertInvalidColumnForTransform("timestamp_col", PartitionTransform.Type.TRUNCATE, args);
  }

  @Test
  public void testSchemaValidationWithNonexistentField() {
    PartitionTransform transform =
        new PartitionTransform("does_not_exist", PartitionTransform.Type.YEAR);
    assertThatThrownBy(() -> transform.validateWithSchema(ROW_TYPE))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid column name");
  }

  private void assertValidColumnForTransform(String columnName, PartitionTransform.Type type) {
    assertValidColumnForTransform(columnName, type, ImmutableList.of());
  }

  private void assertValidColumnForTransform(
      String columnName, PartitionTransform.Type type, List<Object> arguments) {
    PartitionTransform transform = new PartitionTransform(columnName, type, arguments);
    assertThatNoException().isThrownBy(() -> transform.validateWithSchema(ROW_TYPE));
  }

  private void assertInvalidColumnForTransform(String columnName, PartitionTransform.Type type) {
    assertInvalidColumnForTransform(columnName, type, ImmutableList.of());
  }

  private void assertInvalidColumnForTransform(
      String columnName, PartitionTransform.Type type, List<Object> arguments) {
    PartitionTransform transform = new PartitionTransform(columnName, type, arguments);
    assertThatThrownBy(() -> transform.validateWithSchema(ROW_TYPE))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid column type for partition transform");
  }

  SqlPartitionTransform createSqlTransform(String columnName) {
    return new SqlPartitionTransform(
        new SqlIdentifier(columnName, SqlParserPos.ZERO), SqlParserPos.ZERO);
  }

  SqlPartitionTransform createSqlTransform(String columnName, String transformName) {
    return createSqlTransform(columnName, transformName, ImmutableList.of());
  }

  SqlPartitionTransform createSqlTransform(
      String columnName, String transformName, List<SqlLiteral> args) {
    return new SqlPartitionTransform(
        new SqlIdentifier(columnName, SqlParserPos.ZERO),
        new SqlIdentifier(transformName, SqlParserPos.ZERO),
        args,
        SqlParserPos.ZERO);
  }

  SqlLiteral createLiteral(int value) {
    return SqlLiteral.createExactNumeric(Integer.toString(value), SqlParserPos.ZERO);
  }

  SqlLiteral createLiteral(String value) {
    return SqlLiteral.createCharString(value, SqlParserPos.ZERO);
  }
}
