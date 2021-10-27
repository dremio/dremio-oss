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

import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.calcite.util.Static.RESOURCE;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction;
import org.apache.calcite.sql.fun.SqlNtileAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.DremioEmptyScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlScopedShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

class SqlValidatorImpl extends org.apache.calcite.sql.validate.SqlValidatorImpl {
  private final FlattenOpCounter flattenCount;

  protected SqlValidatorImpl(
      FlattenOpCounter flattenCount,
      SqlOperatorTable sqlOperatorTable,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      SqlConformance conformance) {
    super(sqlOperatorTable, catalogReader, typeFactory, conformance);
    this.flattenCount = flattenCount;
  }

  @Override
  public SqlNode validate(SqlNode topNode) {
    final SqlValidatorScope scope = DremioEmptyScope.createBaseScope(this);
    final SqlNode topNode2 = validateScopedExpression(topNode, scope);
    final RelDataType type = getValidatedNodeType(topNode2);
    Util.discard(type);
    return topNode2;
  }

  @Override
  protected SqlNode performUnconditionalRewrites(SqlNode node, boolean underFrom) {
    if(node instanceof SqlBasicCall
        && ((SqlBasicCall)node).getOperator() instanceof SqlJdbcFunctionCall) {
      //Check for operator overrides in DremioSqlOperatorTable
      SqlBasicCall call = (SqlBasicCall) node;
      final SqlJdbcFunctionCall function = (SqlJdbcFunctionCall) call.getOperator();
      final List<SqlOperator> overloads = new ArrayList<>();
      //The name is in the format {fn operator_name}, so we need to remove the prefix '{fn ' and
      //the suffix '}' to get the original operators name.
      String functionName = function.getName().substring(4, function.getName().length()-1);
      //ROUND and TRUNCATE have been overridden in DremioSqlOperatorTable
      if(functionName.equalsIgnoreCase(DremioSqlOperatorTable.ROUND.getName())) {
        call.setOperator(DremioSqlOperatorTable.ROUND);
      } else if(functionName.equalsIgnoreCase(DremioSqlOperatorTable.TRUNCATE.getName())) {
        call.setOperator(DremioSqlOperatorTable.TRUNCATE);
      }
    }
    return super.performUnconditionalRewrites(node, underFrom);
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
  public void validateAggregateParams(SqlCall aggCall, SqlNode filter, SqlNodeList orderList, SqlValidatorScope scope) {
    super.validateAggregateParams(aggCall, filter, orderList, scope);
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

  /**Overriden to Handle the ITEM operator.*/
  @Override
  protected @Nullable SqlNode stripDot(@Nullable SqlNode node) {
    //Checking for Item operator which is similiar to the dot operator.
    if (null == node) {
      return null;
    } else if (node.getKind() == SqlKind.DOT) {
      return stripDot(((SqlCall) node).operand(0));
    } else if (node.getKind() == SqlKind.OTHER_FUNCTION
        && SqlStdOperatorTable.ITEM == ((SqlCall) node).getOperator()) {
      return stripDot(((SqlCall) node).operand(0));
    } else {
      return node;
    }
  }

  /**We are seeing nested AS nodes that reference SqlIdentifiers.*/
  @Override
  protected void checkRollUp(SqlNode grandParent,
      SqlNode parent,
      SqlNode current,
      SqlValidatorScope scope,
      String optionalClause) {
    current = stripAs(current);
    if (current instanceof SqlCall && !(current instanceof SqlSelect)) {
      // Validate OVER separately
      checkRollUpInWindow(getWindowInOver(current), scope);
      current = stripOver(current);

      SqlNode stripped =  stripAs(stripDot(current));

      if (stripped instanceof SqlCall) {
        List<SqlNode> children = ((SqlCall) stripped).getOperandList();
        for (SqlNode child : children) {
          checkRollUp(parent, current, child, scope, optionalClause);
        }
      } else {
        current = stripped;
      }
    }
    if (current instanceof SqlIdentifier) {
      SqlIdentifier id = (SqlIdentifier) current;
      if (!id.isStar() && isRolledUpColumn(id, scope)) {
        if (!isAggregation(parent.getKind())
            || !isRolledUpColumnAllowedInAgg(id, scope, (SqlCall) parent, grandParent)) {
          String context = optionalClause != null ? optionalClause : parent.getKind().toString();
          throw newValidationError(id,
              RESOURCE.rolledUpNotAllowed(deriveAlias(id, 0), context));
        }
      }
    }
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
      SqlValidator validator = getScope().getValidator();
      final SqlCall call = validator.makeNullaryCall(id);
      if (call != null) {
        return (SqlNode)call.accept(this);
      } else {
        SqlIdentifier fqId = null;
        try {
          fqId = this.getScope().fullyQualify(id).identifier;
        } catch (CalciteContextException ex) {
          // The failure here may be happening because the path references a field within ANY type column.
          // Check if the first derivable type in parents is ANY. If this is the case, fall back to ITEM operator.
          // Otherwise, throw the original exception.
          if(id.names.size() > 1 && checkAnyType(id)) {
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

    @Override
    protected SqlNode visitScoped(SqlCall call) {
      switch(call.getKind()) {
        case WITH:
        case SCALAR_QUERY:
        case CURRENT_VALUE:
        case NEXT_VALUE:
          return call;
        default:
          SqlCall newCall = call;
          if (call.getOperator() == SqlStdOperatorTable.DOT) {
            try {
              validator.deriveType(getScope(), call);
            } catch (Exception ex) {
              // The failure here may be happening because the dot operator was used within ANY type column.
              // Check if the first derivable type in parents is ANY. If this is the case, fall back to ITEM operator.
              // Otherwise, throw the original exception.
              if (checkAnyType(call)) {
                SqlNode left = call.getOperandList().get(0);
                SqlNode right = call.getOperandList().get(1);
                SqlNode[] inputs = new SqlNode[]{left, SqlLiteral.createCharString(right.toString(), call.getParserPosition())};
                newCall = new SqlBasicCall(SqlStdOperatorTable.ITEM, inputs, call.getParserPosition());
              } else {
                throw ex;
              }
            }
          }
          ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler(newCall, false);
          newCall.getOperator().acceptCall(this, newCall, true, argHandler);
          SqlNode result = (SqlNode)argHandler.result();
          this.validator.setOriginal(result, newCall);
          return result;
      }
    }

    private boolean checkAnyType(SqlIdentifier identifier) {
      List<String> names = identifier.names;
      for (int i = names.size(); i > 0; i--) {
        try {
          final RelDataType type = validator.deriveType(getScope(), new SqlIdentifier(names.subList(0, i), identifier.getParserPosition()));
          return SqlTypeName.ANY == type.getSqlTypeName();
        } catch (Exception ignored) {
        }
      }
      return false;
    }

    private boolean checkAnyType(SqlCall call) {
      if (call.getOperandList().size() == 0) {
        return false;
      }

      RelDataType type = null;
      final SqlNode operand = call.operand(0);
      try {
        type = validator.deriveType(getScope(), operand);
      } catch (Exception ignored) {
      }

      if(type != null) {
        return SqlTypeName.ANY == type.getSqlTypeName();
      }

      if(operand instanceof SqlCall) {
        return checkAnyType((SqlCall) operand);
      }

      if(operand instanceof SqlIdentifier) {
        return checkAnyType((SqlIdentifier) operand);
      }

      return false;
    }
  }
}
