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

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class SqlFunctionImpl extends SqlFunction {
  private final Source source;
  private final boolean isDeterministic;
  private final boolean isDynamic;
  private final SqlSyntax sqlSyntax;

  protected SqlFunctionImpl(
      String name,
      SqlReturnTypeInference sqlReturnTypeInference,
      SqlOperandTypeChecker sqlOperandTypeChecker,
      Source source,
      boolean isDeterministic,
      boolean isDynamic,
      SqlSyntax sqlSyntax) {
    super(
        name,
        // We want these functions to behave like builtin and calcite denotes that by leaving the
        // identifier as null.
        null,
        SqlKind.OTHER_FUNCTION,
        sqlReturnTypeInference,
        null,
        sqlOperandTypeChecker,
        null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.source = source;
    this.isDeterministic = isDeterministic;
    this.isDynamic = isDynamic;
    this.sqlSyntax = sqlSyntax;
  }

  public static SqlFunction create(
      String name,
      SqlReturnTypeInference sqlReturnTypeInference,
      SqlOperandTypeChecker sqlOperandTypeChecker) {
    return SqlFunctionImpl.create(
        name, sqlReturnTypeInference, sqlOperandTypeChecker, Source.DREMIO);
  }

  public static SqlFunction create(
      String name,
      SqlReturnTypeInference sqlReturnTypeInference,
      SqlOperandTypeChecker sqlOperandTypeChecker,
      Source source) {
    return SqlFunctionImpl.create(
        name,
        sqlReturnTypeInference,
        sqlOperandTypeChecker,
        source,
        true,
        false,
        SqlSyntax.FUNCTION);
  }

  public static SqlFunction create(
      String name,
      SqlReturnTypeInference sqlReturnTypeInference,
      SqlOperandTypeChecker sqlOperandTypeChecker,
      Source source,
      boolean isDeterministic,
      boolean isDynamic,
      SqlSyntax sqlSyntax) {
    return new SqlFunctionImpl(
        name,
        sqlReturnTypeInference,
        sqlOperandTypeChecker,
        source,
        isDeterministic,
        isDynamic,
        sqlSyntax);
  }

  @Override
  public SqlSyntax getSyntax() {
    return sqlSyntax;
  }

  @Override
  public boolean isDeterministic() {
    return isDeterministic;
  }

  @Override
  public boolean isDynamicFunction() {
    return isDynamic;
  }

  public Source getSource() {
    return source;
  }

  public enum Source {
    DREMIO,
    JAVA,
    GANDIVA,
    HIVE
  }
}
