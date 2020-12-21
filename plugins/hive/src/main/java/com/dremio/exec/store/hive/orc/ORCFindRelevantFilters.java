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
package com.dremio.exec.store.hive.orc;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.logical.RexToExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Go through the predicate expression and find the expression tree/sub tree that can be pushed into the ORC reader.
 */
class ORCFindRelevantFilters extends RexVisitorImpl<RexNode> {
  private final RexBuilder rexBuilder;
  private final RelDataType incomingRowType;
  private SchemaPath column;

  ORCFindRelevantFilters(final RexBuilder rexBuilder, RelDataType incomingRowType) {
    super(true);
    this.rexBuilder = rexBuilder;
    this.incomingRowType = incomingRowType;
  }

  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    if (incomingRowType != null) {
      column = new SchemaPath(incomingRowType.getFieldNames().get(inputRef.getIndex()));
    }
    return rexBuilder.copy(inputRef);
  }

  @Override
  public RexNode visitLocalRef(RexLocalRef localRef) {
    return null;
  }

  @Override
  public RexNode visitLiteral(RexLiteral literal) {
    if (RexToExpr.isLiteralNull(literal)) {
      return null; // TODO: ORC predicate doesn't support null??
    }

    switch (literal.getType().getSqlTypeName()) {
      case VARCHAR:
      case CHAR:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case DATE:
      case TIMESTAMP:
      case BOOLEAN:
        return rexBuilder.copy(literal);
      default:
        return null;
    }
  }

  @Override
  public RexNode visitOver(RexOver over) {
    return null;
  }

  @Override
  public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
    return null;
  }

  @Override
  public RexNode visitCall(RexCall call) {
    switch (call.getKind()) {
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case EQUALS:
      case NOT_EQUALS: {
        final List<RexNode> children = call.getOperands();
        final RexNode child1 = children.get(0);
        final RexNode child2 = children.get(1);
        if ((child1 instanceof RexInputRef && child2 instanceof RexLiteral) ||
            (child1 instanceof RexLiteral && child2 instanceof RexInputRef)) {

          if (child1.accept(this) != null && child2.accept(this) != null) {
            return rexBuilder.copy(call);
          }
        }
        return null;
      }

      case IS_NULL:
      case IS_NOT_NULL: {
        final List<RexNode> children = call.getOperands();
        final RexNode child1 = children.get(0);
        if (child1 instanceof RexInputRef && child1.accept(this) != null) {
          return rexBuilder.copy(call);
        }

        return null;
      }

      case NOT: {
        final RexNode result = call.getOperands().get(0).accept(this);
        if (result != null) {
          return rexBuilder.makeCall(call.getOperator(), result);
        }
        return null;
      }

      case AND: {
        // Extract out as many children that can be pushed into reader
        final List<RexNode> evaluatedChildren = Lists.newArrayList();
        for(RexNode child : call.getOperands()) {
          RexNode evaluatedChild = child.accept(this);
          if (evaluatedChild != null) {
            evaluatedChild = convertBooleanInputRefToFunctionCall(rexBuilder, evaluatedChild);
            evaluatedChildren.add(evaluatedChild);
          }
        }

        if (evaluatedChildren.isEmpty()) {
          return null;
        }

        if (evaluatedChildren.size() == 1) {
          return evaluatedChildren.get(0);
        }

        return rexBuilder.makeCall(call.getOperator(), evaluatedChildren);
      }

      case OR:
      case IN: {
        // We considered OR or IN only if all children can be pushed into reader
        final List<RexNode> evaluatedChildren = Lists.newArrayList();
        for(RexNode child : call.getOperands()) {
          RexNode evaluatedChild = child.accept(this);
          if (evaluatedChild == null) {
            return null;
          }
          evaluatedChild = convertBooleanInputRefToFunctionCall(rexBuilder, evaluatedChild);
          evaluatedChildren.add(evaluatedChild);
        }

        return rexBuilder.makeCall(call.getOperator(), evaluatedChildren);
      }

      default:
        return null;
    }
  }

  @Override
  public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
    return null;
  }

  @Override
  public RexNode visitRangeRef(RexRangeRef rangeRef) {
    return null;
  }

  @Override
  public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
    return null;
  }

  @Override
  public RexNode visitSubQuery(RexSubQuery subQuery) {
    return null;
  }

  /**
   * Convert expressions that contain boolean type column input refs as function call
   */
  public static RexNode convertBooleanInputRefToFunctionCall(RexBuilder rexBuilder, RexNode input) {
    if (input.getKind() == SqlKind.INPUT_REF) {
      // convert boolean type input references to col == true
      return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ImmutableList.of(input, rexBuilder.makeLiteral(true)));
    }

    return input;
  }

  public SchemaPath getColumn() {
    return column;
  }
}
