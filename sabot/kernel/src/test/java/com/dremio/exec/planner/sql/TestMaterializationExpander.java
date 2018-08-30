/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static java.util.Arrays.asList;

import java.util.Collections;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.planner.types.SqlTypeFactoryImpl;

/**
 * Tests for MaterializationExpander
 */
public class TestMaterializationExpander {
  private final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;

  /**
   * row types that only differ by field names should be equal
   */
  @Test
  public void testAreRowTypesEqualIgnoresNames() {
    final RelDataType type1 = typeFactory.createStructType(
      asList(
        typeFactory.createSqlType(SqlTypeName.INTEGER),
        typeFactory.createSqlType(SqlTypeName.BIGINT),
        typeFactory.createSqlType(SqlTypeName.FLOAT),
        typeFactory.createSqlType(SqlTypeName.DOUBLE),
        typeFactory.createSqlType(SqlTypeName.DATE),
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        typeFactory.createSqlType(SqlTypeName.BOOLEAN)
      ),
      asList("intC", "bigIntC", "floatC", "doubleC", "dateC", "tsC", "varcharC", "boolC")
    );

    final RelDataType type2 = typeFactory.createStructType(
      asList(
        typeFactory.createSqlType(SqlTypeName.INTEGER),
        typeFactory.createSqlType(SqlTypeName.BIGINT),
        typeFactory.createSqlType(SqlTypeName.FLOAT),
        typeFactory.createSqlType(SqlTypeName.DOUBLE),
        typeFactory.createSqlType(SqlTypeName.DATE),
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        typeFactory.createSqlType(SqlTypeName.BOOLEAN)
      ),
      asList("intC2", "bigIntC2", "floatC2", "doubleC2", "dateC2", "tsC2", "varcharC2", "boolC2")
    );

    Assert.assertTrue(MaterializationExpander.areRowTypesEqual(type1, type2));
  }

  /**
   * row types that only differ by nullability should be equal
   */
  @Test
  public void testAreRowTypesEqualIgnoresNullability() {
    final RelDataType type1 = typeFactory.createStructType(
      asList(
        typeFactory.createSqlType(SqlTypeName.INTEGER),
        typeFactory.createSqlType(SqlTypeName.DOUBLE),
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
        typeFactory.createSqlType(SqlTypeName.BOOLEAN)
      ),
      asList("intC", "doubleC", "tsC", "boolC")
    );

    final RelDataType type2 = typeFactory.createStructType(
      asList(
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true),
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true),
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), true)
      ),
      asList("intC2", "doubleC2", "tsC2", "boolC2")
    );

    Assert.assertTrue(MaterializationExpander.areRowTypesEqual(type1, type2));
  }

  /**
   * row types that only differ for fields with ANY types should be equal
   */
  @Test
  public void testAreRowTypesEqualIgnoresAny() {
    final RelDataType type1 = typeFactory.createStructType(
      asList(
        typeFactory.createSqlType(SqlTypeName.INTEGER),
        typeFactory.createSqlType(SqlTypeName.BIGINT)
      ),
      asList("intC", "bigIntC")
    );

    final RelDataType type2 = typeFactory.createStructType(
      asList(
        typeFactory.createSqlType(SqlTypeName.INTEGER),
        typeFactory.createSqlType(SqlTypeName.ANY)
      ),
      asList("intC", "anyC")
    );

    Assert.assertTrue(MaterializationExpander.areRowTypesEqual(type1, type2));
  }

  /**
   * CHAR<>VARCHAR fields should be equivalent
   */
  @Test
  public void testAreRowTypesEqualMatchesCharVarchar() {
    final RelDataType type1 = typeFactory.createStructType(
      asList(
        typeFactory.createSqlType(SqlTypeName.CHAR),
        typeFactory.createSqlType(SqlTypeName.VARCHAR)
      ),
      asList("charC", "varcharC")
    );

    final RelDataType type2 = typeFactory.createStructType(
      asList(
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        typeFactory.createSqlType(SqlTypeName.CHAR)
      ),
      asList("varcharC", "charC")
    );

    Assert.assertTrue(MaterializationExpander.areRowTypesEqual(type1, type2));
  }

  /**
   * row types with TIMESTAMP fields with different precisions should not be equal
   */
  @Test
  public void testTimestampPrecisionMismatch() {
    // Don't use Dremio's type factory as it enforces a precision of 3
    final RelDataTypeFactory calciteFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    final RelDataType type1 = calciteFactory.createStructType(
      Collections.singletonList(calciteFactory.createSqlType(SqlTypeName.TIMESTAMP, 0)),
      Collections.singletonList("ts0")
    );

    final RelDataType type2 = typeFactory.createStructType(
      Collections.singletonList(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3)),
      Collections.singletonList("ts3")
    );

    Assert.assertFalse(MaterializationExpander.areRowTypesEqual(type1, type2));
  }
}
