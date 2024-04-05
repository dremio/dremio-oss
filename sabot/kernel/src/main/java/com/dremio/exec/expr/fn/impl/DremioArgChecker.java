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
package com.dremio.exec.expr.fn.impl;

import static org.apache.calcite.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Check the calcite arguments to determine whether the function arguments are valid.
 * Simliar/inspired by FamilyOperandTypeChecker but more generic.
 */
public class DremioArgChecker implements SqlSingleOperandTypeChecker {

  private final ImmutableList<Checker> checkers;
  private final String signature;
  private final boolean allowAny;

  public DremioArgChecker(boolean allowAny, Checker... checkers) {
    this.checkers = ImmutableList.copyOf(checkers);
    this.signature =
        "%s("
            + this.checkers.stream().map(c -> c.signature()).collect(Collectors.joining(", "))
            + ")";
    this.allowAny = allowAny;
  }

  @Override
  public boolean isOptional(int i) {
    return false;
  }

  @Override
  public boolean checkSingleOperandType(
      SqlCallBinding callBinding, SqlNode node, int iFormalOperand, boolean throwOnFailure) {
    Checker checker = checkers.get(iFormalOperand);
    return checkOp(checker, callBinding, node, iFormalOperand, throwOnFailure);
  }

  public static Checker ofFloat(String argName) {
    return new Checker() {

      @Override
      public boolean check(RelDataType type) {
        return type.getSqlTypeName() == SqlTypeName.FLOAT;
      }

      @Override
      public String signature() {
        return argName + " <FLOAT>";
      }
    };
  }

  public static Checker ofDouble(String argName) {
    return new Checker() {

      @Override
      public boolean check(RelDataType type) {
        return type.getSqlTypeName() == SqlTypeName.DOUBLE;
      }

      @Override
      public String signature() {
        return argName + " <DOUBLE>";
      }
    };
  }

  public static interface Checker {
    boolean check(RelDataType type);

    String signature();
  }

  private boolean checkOp(
      Checker checker,
      SqlCallBinding callBinding,
      SqlNode node,
      int iFormalOperand,
      boolean throwOnFailure) {

    if (SqlUtil.isNullLiteral(node, false)) {
      if (throwOnFailure) {
        throw callBinding.getValidator().newValidationError(node, RESOURCE.nullIllegal());
      } else {
        return false;
      }
    }
    RelDataType type = callBinding.getValidator().deriveType(callBinding.getScope(), node);
    SqlTypeName typeName = type.getSqlTypeName();

    // Pass type checking for operators if it's of type 'ANY'.
    if (typeName.getFamily() == SqlTypeFamily.ANY && allowAny) {
      return true;
    }

    if (!checker.check(type)) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }
    return true;
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    if (checkers.size() != callBinding.getOperandCount()) {
      // assume this is an inapplicable sub-rule of a composite rule;
      // don't throw
      return false;
    }

    for (Ord<SqlNode> op : Ord.zip(callBinding.operands())) {
      if (!checkSingleOperandType(callBinding, op.e, op.i, throwOnFailure)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(0, checkers.size());
  }

  @Override
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return String.format(signature, opName);
  }

  @Override
  public Consistency getConsistency() {
    return Consistency.NONE;
  }
}
