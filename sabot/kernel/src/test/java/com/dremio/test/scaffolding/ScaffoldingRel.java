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
package com.dremio.test.scaffolding;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BINARY;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.types.JavaTypeFactoryImpl;

/**
 * Immutable global variables for RelNodes and RexNodes.
 */
public final class ScaffoldingRel {
  public static final RelDataTypeFactory TYPE_FACTORY = JavaTypeFactoryImpl.INSTANCE;
  public static final RelDataType VARCHAR_TYPE = TYPE_FACTORY.createSqlType(VARCHAR);
  public static final RelDataType ANY_TYPE = TYPE_FACTORY.createSqlType(ANY);
  public static final RelDataType VARCHAR_NULL_TYPE =
    TYPE_FACTORY.createTypeWithNullability(VARCHAR_TYPE, true);
  public static final RelDataType CHAR_NULL_TYPE =
    TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(CHAR), true);
  public static final RelDataType FLOAT_TYPE = TYPE_FACTORY.createSqlType(FLOAT);
  public static final RelDataType BIG_INT_TYPE = TYPE_FACTORY.createSqlType(BIGINT);
  public static final RelDataType BIG_INT_NULL_TYPE =
    TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(BIGINT), true);
  public static final RelDataType INT_TYPE = TYPE_FACTORY.createSqlType(INTEGER);
  public static final RelDataType INT_NULL_TYPE =
      TYPE_FACTORY.createTypeWithNullability(INT_TYPE, true);
  public static final RelDataType INT_NULL_ARRAY_NULL_COLUMN_TYPE =
    TYPE_FACTORY.createTypeWithNullability(
      TYPE_FACTORY.createArrayType(INT_NULL_TYPE, -1), true);
  public static final RelDataType INT_ARRAY_COLUMN_TYPE = createArrayType(TYPE_FACTORY,INT_TYPE,false);
  public static final RelDataType VARCHAR_ARRAY_COLUMN_TYPE = createArrayType(TYPE_FACTORY,VARCHAR_TYPE,false);
  public static final RelDataType SMALL_INT_NULL_TYPE =
    TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SMALLINT), true);
  public static final RelDataType TINY_INT_NULL_TYPE =
    TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(TINYINT), true);
  public static final RelDataType DATE_TYPE =
    TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
  public static final RelDataType DATE_NULL_TYPE =
      TYPE_FACTORY.createTypeWithNullability(DATE_TYPE, true);
  public static final RelDataType TIMESTAMP_TYPE =
    TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
  public static final RelDataType TIMESTAMP_NULL_TYPE =
    TYPE_FACTORY.createTypeWithNullability(TIMESTAMP_TYPE, true);
  public static final RelDataType BOOLEAN_TYPE = TYPE_FACTORY.createSqlType(BOOLEAN);
  public static final RelDataType BOOLEAN_NULL_TYPE =
      TYPE_FACTORY.createTypeWithNullability(BOOLEAN_TYPE, true);
  public static final RelDataType BINARY_TYPE = TYPE_FACTORY.createSqlType(BINARY);

  public static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  private ScaffoldingRel() {
  }
}
