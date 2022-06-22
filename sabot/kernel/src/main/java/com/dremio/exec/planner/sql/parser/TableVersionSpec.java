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

import static org.apache.calcite.util.Static.RESOURCE;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.TimestampString;

import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.TableVersionType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Representation of a table version specification, which may be associated with a table identifier or a TABLE()
 * function call.  A TableVersionSpec must be resolved to convert constant expressions into literal values.
 */
public class TableVersionSpec {

  private final TableVersionType tableVersionType;
  private final SqlNode versionSpecifier;
  private SqlLiteral resolvedVersionSpecifier;

  public TableVersionSpec(TableVersionType tableVersionType, SqlNode versionSpecifier) {
    this.tableVersionType = tableVersionType;
    this.versionSpecifier = versionSpecifier;
  }

  public TableVersionContext getResolvedTableVersionContext() {
    Preconditions.checkNotNull(resolvedVersionSpecifier);

    Object value = null;
    switch (tableVersionType) {
      case BRANCH:
      case TAG:
      case COMMIT_HASH_ONLY:
      case REFERENCE:
      case SNAPSHOT_ID:
        Preconditions.checkState(resolvedVersionSpecifier instanceof SqlCharStringLiteral);
        value = resolvedVersionSpecifier.getValueAs(String.class);
        break;
      case TIMESTAMP:
        Preconditions.checkState(resolvedVersionSpecifier instanceof SqlTimestampLiteral);
        value = resolvedVersionSpecifier.getValueAs(Calendar.class).getTimeInMillis();
        break;
    }

    Preconditions.checkNotNull(value);
    return new TableVersionContext(tableVersionType, value);
  }

  /**
   * Resolves a TableVersionSpec by performing constant folding on the versionSpecifier.  An error will be reported
   * if the expression provided is not resolvable to a constant value of the appropriate type.
   */
  public void resolve(SqlValidator validator, SqlToRelConverter converter, RexExecutor rexExecutor,
                      RexBuilder rexBuilder) {
    SqlNode validatedSpecifier = validator.validate(versionSpecifier);

    // Nothing to resolve if the version specifier is already a literal - this should always be the
    // case for non-TIMESTAMP types
    if (validatedSpecifier instanceof SqlLiteral) {
      resolvedVersionSpecifier = (SqlLiteral) validatedSpecifier;
      return;
    }
    // TIMESTAMP is the only type that allows non-literal constant expressions, guaranteed by the grammar
    Preconditions.checkState(tableVersionType == TableVersionType.TIMESTAMP);

    RexNode expr = converter.convertExpression(validatedSpecifier, new HashMap<>());
    List<RexNode> reducedExprs = new ArrayList<>();
    rexExecutor.reduce(rexBuilder, ImmutableList.of(expr), reducedExprs);
    RexNode finalExpr = reducedExprs.get(0);

    if (finalExpr instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) finalExpr;
      if (literal.getTypeName() != SqlTypeName.TIMESTAMP) {
        throw resolveError("Expected timestamp literal or constant timestamp expression");
      }

      resolvedVersionSpecifier = SqlLiteral.createTimestamp(
        TimestampString.fromCalendarFields(literal.getValueAs(Calendar.class)), 3,
        versionSpecifier.getParserPosition());
    }
  }

  private CalciteContextException resolveError(String message) {
    SqlValidatorException validatorEx = new SqlValidatorException(message, null);
    SqlParserPos pos = versionSpecifier.getParserPosition();
    int line = pos.getLineNum();
    int col = pos.getColumnNum();
    int endLine = pos.getEndLineNum();
    int endCol = pos.getEndColumnNum();
    CalciteContextException contextEx =
      (line == endLine && col == endCol
        ? RESOURCE.validatorContextPoint(line, col)
        : RESOURCE.validatorContext(line, col, endLine, endCol)).ex(validatorEx);
    contextEx.setPosition(line, col, endLine, endCol);
    return contextEx;
  }
}
