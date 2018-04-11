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

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class SqlOperatorImpl extends SqlFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlOperatorImpl.class);

  private final boolean isDeterministic;
  private final boolean isDynamic;
  private final SqlSyntax syntax;
  /**
   * This constructor exists for the legacy reason.
   *
   * It is because Dremio cannot access to OperatorTable at the place where this constructor is being called.
   * In principle, if Dremio needs a SqlOperatorImpl, it is supposed to go to OperatorTable for pickup.
   */
  @Deprecated
  public SqlOperatorImpl(final String name, final int argCount, final boolean isDeterministic) {
    this(name,
        argCount,
        argCount,
        isDeterministic,
        DynamicReturnType.INSTANCE);
  }

  public SqlOperatorImpl(String name, int argCountMin, int argCountMax, boolean isDeterministic,
      SqlReturnTypeInference sqlReturnTypeInference) {
    this(name,
        argCountMin,
        argCountMax,
        isDeterministic,
        false,
        DynamicReturnType.INSTANCE,
        SqlSyntax.FUNCTION);
  }

  public SqlOperatorImpl(String name, int argCountMin, int argCountMax, boolean isDeterministic,
      boolean isDynamic, SqlReturnTypeInference sqlReturnTypeInference, SqlSyntax syntax) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO),
        sqlReturnTypeInference,
        null,
        Checker.getChecker(argCountMin, argCountMax),
        null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.isDeterministic = isDeterministic;
    this.isDynamic = isDynamic;
    this.syntax = syntax;
  }
  @Override
  public SqlSyntax getSyntax() {
    return syntax;
  }

  @Override
  public boolean isDeterministic() {
    return isDeterministic;
  }

  @Override
  public boolean isDynamicFunction() {
    return isDynamic;
  }
}
