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

import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.dremio.exec.planner.sql.parser.SqlVersionedTableMacroCall;

/**
 * Performs an independent validation & constant folding pass on versioned table expressions.  This ensures we
 * can provide a resolved TableVersionContext to the catalog API for all such versioned table references during
 * the core SQL validation pass.
 */
public class VersionedTableExpressionResolver {

  private final SqlValidator validator;
  private final RexBuilder rexBuilder;
  private final RexExecutor rexExecutor;

  public VersionedTableExpressionResolver(SqlValidator validator, RexBuilder rexBuilder) {
    this.validator = validator;
    this.rexBuilder = rexBuilder;
    this.rexExecutor = RexUtil.EXECUTOR;
  }

  public void resolve(SqlToRelConverter converter, SqlNode node) {
    if (node instanceof SqlVersionedTableMacroCall) {
      resolvedVersionedTableMacroCall(converter, (SqlVersionedTableMacroCall) node);
    } else if (node instanceof SqlCall) {
      SqlCall call = (SqlCall) node;
      final List<SqlNode> operands = call.getOperandList();
      for (SqlNode operand : operands) {
        resolve(converter, operand);
      }
    } else if (node instanceof SqlNodeList) {
      SqlNodeList list = (SqlNodeList) node;
      for (SqlNode operand : list) {
        resolve(converter, operand);
      }
    }
  }

  private void resolvedVersionedTableMacroCall(SqlToRelConverter converter, SqlVersionedTableMacroCall call) {
    call.getTableVersionSpec().resolve(validator, converter, rexExecutor, rexBuilder);
  }
}
