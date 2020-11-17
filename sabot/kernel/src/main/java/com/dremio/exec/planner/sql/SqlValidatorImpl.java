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


import static org.apache.calcite.util.Static.RESOURCE;

import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction;
import org.apache.calcite.sql.fun.SqlNtileAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlScopedShuttle;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

class SqlValidatorImpl extends org.apache.calcite.sql.validate.SqlValidatorImpl {

  private final FlattenOpCounter flattenCount;

  protected SqlValidatorImpl(
      FlattenOpCounter flattenCount,
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      SqlConformance conformance) {
    super(opTab, catalogReader, typeFactory, conformance);
    this.flattenCount = flattenCount;
  }

  @Override
  public void validateJoin(SqlJoin join, SqlValidatorScope scope) {
    SqlNode condition = join.getCondition();
    checkIfFlattenIsPartOfJoinCondition(condition);
    super.validateJoin(join, scope);
  }

  private void checkIfFlattenIsPartOfJoinCondition(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) node;
      SqlNode[] conditionOperands = call.getOperands();
      for (SqlNode operand : conditionOperands) {
        if (operand instanceof SqlBasicCall) {
          if (((SqlBasicCall) operand).getOperator().getName().equalsIgnoreCase("flatten")) {
            throwException(node.getParserPosition());
          }
        }
        checkIfFlattenIsPartOfJoinCondition(operand);
      }
    }
  }

  private void throwException(SqlParserPos parserPos) {
    throw new CalciteContextException("Failure parsing the query",
                                      new SqlValidatorException("Flatten is not supported as part of join condition", null),
                                      parserPos.getLineNum(), parserPos.getEndLineNum(),
                                      parserPos.getColumnNum(), parserPos.getEndColumnNum());
  }

  int nextFlattenIndex(){
    return flattenCount.nextFlattenIndex();
  }

  static class FlattenOpCounter {
    private int value;

    int nextFlattenIndex(){
      return value++;
    }
  }

  @Override
  public void validateWindow(
    SqlNode windowOrId,
    SqlValidatorScope scope,
    SqlCall call) {
    super.validateWindow(windowOrId, scope, call);
    final SqlWindow targetWindow;
    switch (windowOrId.getKind()) {
      case IDENTIFIER:
        targetWindow = getWindowByName((SqlIdentifier) windowOrId, scope);
        break;
      case WINDOW:
        targetWindow = (SqlWindow) windowOrId;
        break;
      default:
        return;
    }

    SqlNodeList orderList = targetWindow.getOrderList();
    SqlOperator operator = call.getOperator();
    Exception e = null;
    if (operator instanceof SqlLeadLagAggFunction || operator instanceof SqlNtileAggFunction) {
      if (orderList.size() == 0) {
        e = new SqlValidatorException("LAG, LEAD or NTILE functions require ORDER BY clause in window specification", null);
      }
    }

    if (orderList.getList().stream().anyMatch(f -> f instanceof SqlNumericLiteral)) {
      e = new SqlValidatorException("Dremio does not currently support order by with ordinals in over clause", null);
    }

    if (e != null) {
      SqlParserPos pos = targetWindow.getParserPosition();
      CalciteContextException ex = RESOURCE.validatorContextPoint(pos.getLineNum(), pos.getColumnNum()).ex(e);
      ex.setPosition(pos.getLineNum(), pos.getColumnNum());
      throw ex;
    }
  }

  @Override
  public void validateAggregateParams(SqlCall aggCall, SqlNode filter, SqlValidatorScope scope) {
    if (filter != null) {
      Exception e = new SqlValidatorException("Dremio does not currently support aggregate functions with a filter clause", null);
      SqlParserPos pos = filter.getParserPosition();
      CalciteContextException ex = RESOURCE.validatorContextPoint(pos.getLineNum(), pos.getColumnNum()).ex(e);
      ex.setPosition(pos.getLineNum(), pos.getColumnNum());
      throw ex;
    }
    super.validateAggregateParams(aggCall, filter, scope);
  }

  @Override
  public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
    Expander expander = new Expander(this, scope);
    SqlNode newExpr = (SqlNode)expr.accept(expander);
    if (expr != newExpr) {
      this.setOriginal(newExpr, expr);
    }

    return newExpr;
  }

  /**
   * Expander
   */
  private static class Expander extends SqlScopedShuttle {
    protected final org.apache.calcite.sql.validate.SqlValidatorImpl validator;

    Expander(org.apache.calcite.sql.validate.SqlValidatorImpl validator, SqlValidatorScope scope) {
      super(scope);
      this.validator = validator;
    }

    public SqlNode visit(SqlIdentifier id) {
      SqlCall call = SqlUtil.makeCall(this.validator.getOperatorTable(), id);
      if (call != null) {
        return (SqlNode)call.accept(this);
      } else {
        SqlIdentifier fqId = null;
        try {
          fqId = this.getScope().fullyQualify(id).identifier;
        } catch (CalciteContextException ex) {
          if(id.names.size() > 1) {
            SqlBasicCall itemCall = new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              new SqlNode[]{id.getComponent(0, id.names.size()-1), SqlLiteral.createCharString((String) Util.last(id.names), id.getParserPosition())}, id.getParserPosition());
            try {
              return itemCall.accept(this);
            } catch (Exception ignored) {}
          }
          throw ex;
        }
        SqlNode expandedExpr = fqId;
        if (DynamicRecordType.isDynamicStarColName((String) Util.last(fqId.names)) && !DynamicRecordType.isDynamicStarColName((String) Util.last(id.names))) {
          SqlNode[] inputs = new SqlNode[]{fqId, SqlLiteral.createCharString((String) Util.last(id.names), id.getParserPosition())};
          SqlBasicCall item_call = new SqlBasicCall(SqlStdOperatorTable.ITEM, inputs, id.getParserPosition());
          expandedExpr = item_call;
        }

        this.validator.setOriginal((SqlNode) expandedExpr, id);
        return (SqlNode) expandedExpr;
      }
    }

    protected SqlNode visitScoped(SqlCall call) {
      switch(call.getKind()) {
        case WITH:
        case SCALAR_QUERY:
        case CURRENT_VALUE:
        case NEXT_VALUE:
          return call;
        default:
          SqlCall newCall = call;
          if ("dot".equals(((SqlCall) call).getOperator().getName().toLowerCase())) {
            try {
              validator.deriveType(getScope(), call);
            } catch (Exception ex) {
              SqlNode left = call.getOperandList().get(0);
              SqlNode right = call.getOperandList().get(1);
              SqlNode[] inputs = new SqlNode[]{left, SqlLiteral.createCharString(right.toString(), call.getParserPosition())};
              newCall = new SqlBasicCall(SqlStdOperatorTable.ITEM, inputs, call.getParserPosition());
            }
          }
          ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler(newCall, false);
          newCall.getOperator().acceptCall(this, newCall, true, argHandler);
          SqlNode result = (SqlNode)argHandler.result();
          this.validator.setOriginal(result, newCall);
          return result;
      }
    }
  }
}
