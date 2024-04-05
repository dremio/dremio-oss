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

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import org.apache.calcite.sql.type.SqlTypeName;

/** Represents an operand in a calcite SqlFunction. This gets used for SqlOperatorFactory. */
public final class SqlOperand {
  public final String name;
  public final ImmutableSet<SqlTypeName> typeRange;
  public final Type type;

  private SqlOperand(String name, ImmutableSet<SqlTypeName> typeRange, Type type) {
    this.name = name;
    this.typeRange = typeRange;
    this.type = type;
  }

  public static SqlOperand regular(SqlTypeName sqlTypeName) {
    return new SqlOperand(null, ImmutableSet.of(sqlTypeName), Type.REGULAR);
  }

  public static SqlOperand regular(Collection<SqlTypeName> sqlTypeNames) {
    return new SqlOperand(null, ImmutableSet.copyOf(sqlTypeNames), Type.REGULAR);
  }

  public static SqlOperand optional(SqlOperand sqlOperand) {
    return optional(sqlOperand.typeRange);
  }

  public static SqlOperand optional(SqlTypeName sqlTypeName) {
    return new SqlOperand(null, ImmutableSet.of(sqlTypeName), Type.OPTIONAL);
  }

  public static SqlOperand optional(Collection<SqlTypeName> sqlTypeNames) {
    return new SqlOperand(null, ImmutableSet.copyOf(sqlTypeNames), Type.OPTIONAL);
  }

  public static SqlOperand variadic(SqlOperand sqlOperand) {
    return variadic(sqlOperand.typeRange);
  }

  public static SqlOperand variadic(SqlTypeName sqlTypeName) {
    return new SqlOperand(null, ImmutableSet.of(sqlTypeName), Type.VARIADIC);
  }

  public static SqlOperand variadic(Collection<SqlTypeName> sqlTypeNames) {
    return new SqlOperand(null, ImmutableSet.copyOf(sqlTypeNames), Type.VARIADIC);
  }

  public static SqlOperand union(SqlOperand sqlOperand1, SqlOperand sqlOperand2) {
    if (sqlOperand1.type != sqlOperand2.type) {
      throw new UnsupportedOperationException("SqlOperand types must match up.");
    }

    if ((sqlOperand1.name != null)
        && (sqlOperand2.name != null)
        && (sqlOperand1.name != sqlOperand2.name)) {
      throw new UnsupportedOperationException("SqlOperand names must match up.");
    }

    ImmutableSet<SqlTypeName> unionTypeRange =
        new ImmutableSet.Builder<SqlTypeName>()
            .addAll(sqlOperand1.typeRange)
            .addAll(sqlOperand2.typeRange)
            .build();

    return new SqlOperand(sqlOperand1.name, unionTypeRange, sqlOperand1.type);
  }

  public enum Type {
    REGULAR,
    OPTIONAL,
    VARIADIC
  }
}
