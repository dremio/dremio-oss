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
package com.dremio.exec.planner.sql.parser;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

/**
 * A condition that returns true if SqlNode has references to particular functions.
 */
final class FindFunCondition implements SqlNodeCondition {
  private final List<String> functionNames;
  FindFunCondition(List<String> functionNames) {
    this.functionNames = functionNames;
  }

  @Override
  public boolean test(SqlNode sqlNode) {
    return sqlNode instanceof SqlCall && checkOperator((SqlCall) sqlNode, functionNames, true);
  }

  /**
   * Checks recursively if operator and its operands are present in provided list of operators
   */
  private boolean checkOperator(SqlCall sqlCall, List<String> operators, boolean checkOperator) {
    if (checkOperator) {
      return operators.contains(sqlCall.getOperator().getName().toUpperCase()) || checkOperator(sqlCall, operators, false);
    }
    for (SqlNode sqlNode : sqlCall.getOperandList()) {
      if (!(sqlNode instanceof SqlCall)) {
        continue;
      }
      if (checkOperator((SqlCall) sqlNode, operators, true)) {
        return true;
      }
    }
    return false;
  }

}
