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

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.UnsupportedOperatorCollector;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.google.common.collect.Lists;

public class UnsupportedOperatorsVisitor extends SqlShuttle {
  private QueryContext context;
  private static List<String> disabledType = Lists.newArrayList();
  private static List<String> disabledOperators = Lists.newArrayList();
  private static List<String> dirExplorers = Lists.newArrayList();
  private static List<String> flattenNames = Lists.newArrayList();

  static {
    disabledType.add(SqlTypeName.TINYINT.name());
    disabledType.add(SqlTypeName.SMALLINT.name());
    disabledType.add(SqlTypeName.REAL.name());
    dirExplorers.add("MAXDIR");
    dirExplorers.add("IMAXDIR");
    dirExplorers.add("MINDIR");
    dirExplorers.add("IMINDIR");
    flattenNames.add("FLATTEN");
  }

  private UnsupportedOperatorCollector unsupportedOperatorCollector;

  private UnsupportedOperatorsVisitor(QueryContext context) {
    this.context = context;
    this.unsupportedOperatorCollector = new UnsupportedOperatorCollector();
  }

  public static UnsupportedOperatorsVisitor createVisitor(QueryContext context) {
    return new UnsupportedOperatorsVisitor(context);
  }

  public void convertException() throws SqlUnsupportedException {
    unsupportedOperatorCollector.convertException();
  }

  @Override
  public SqlNode visit(SqlDataTypeSpec type) {
    for(String strType : disabledType) {
      if(type.getTypeName().getSimple().equalsIgnoreCase(strType)) {
        // see DRILL-1959
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.DATA_TYPE,
            type.getTypeName().getSimple() + " is not supported");
        throw new UnsupportedOperationException();
      }
    }

    return type;
  }

  @Override
  public SqlNode visit(SqlCall sqlCall) {
    // Inspect the window functions
    if(sqlCall instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlCall;

      checkGroupID((sqlSelect));

      for(SqlNode nodeInSelectList : sqlSelect.getSelectList()) {
        // If the window function is used with an alias,
        // enter the first operand of AS operator
        if(nodeInSelectList.getKind() == SqlKind.AS
            && (((SqlCall) nodeInSelectList).getOperandList().get(0).getKind() == SqlKind.OVER)) {
          nodeInSelectList = ((SqlCall) nodeInSelectList).getOperandList().get(0);
        }

        findAndValidateOverOperators(nodeInSelectList);
      }
    }

    // DRILL-3188
    // Disable frame which is other than the default
    // (i.e., BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    if(sqlCall instanceof SqlWindow) {
      SqlWindow window = (SqlWindow) sqlCall;

      SqlNode lowerBound = window.getLowerBound();
      SqlNode upperBound = window.getUpperBound();

      // If no frame is specified
      // it is a default frame
      boolean isSupported = (lowerBound == null && upperBound == null);

      // When OVER clause contain an ORDER BY clause the following frames are supported:
      // RANGE UNBOUNDED PRECEDING
      // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      if(window.getOrderList().size() != 0
          && !window.isRows()
          && SqlWindow.isUnboundedPreceding(lowerBound)
          && (upperBound == null || SqlWindow.isCurrentRow(upperBound) || SqlWindow.isUnboundedFollowing(upperBound))) {
        isSupported = true;
      }

      // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      // is supported with and without the ORDER BY clause
      if (window.isRows()
          && SqlWindow.isUnboundedPreceding(lowerBound)
          && (upperBound == null || SqlWindow.isCurrentRow(upperBound))) {
        isSupported = true;
      }

      // RANGE BETWEEN CURRENT ROW AND CURRENT ROW
      // is supported with and without an ORDER BY clause
      if (!window.isRows() &&
          SqlWindow.isCurrentRow(lowerBound) &&
          SqlWindow.isCurrentRow(upperBound)) {
        isSupported = true;
      }

      // When OVER clause doesn't contain an ORDER BY clause, the following are equivalent to the default frame:
      // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      if(window.getOrderList().size() == 0
          && SqlWindow.isUnboundedPreceding(lowerBound)
          && SqlWindow.isUnboundedFollowing(upperBound)) {
        isSupported = true;
      }

      if(!isSupported) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
            "This type of window frame is currently not supported");
        throw new UnsupportedOperationException();
      }

      // DRILL-3189: Disable DISALLOW PARTIAL
      if(!window.isAllowPartial()) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
            "Dremio doesn't currently support DISALLOW PARTIAL.");
        throw new UnsupportedOperationException();
      }
    }

    // DRILL-1921: Disable unsupported Except ALL
    if(sqlCall.getKind() == SqlKind.EXCEPT  && ((SqlSetOperator)sqlCall.getOperator()).isAll()) {
      unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
          "Dremio doesn't currently support " + sqlCall.getOperator().getName() + " operations.");
      throw new UnsupportedOperationException();
    }

    // DRILL-1921: Disable unsupported Intersect ALL
    if(sqlCall.getKind() == SqlKind.INTERSECT && ((SqlSetOperator)sqlCall.getOperator()).isAll()) {
      unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
          "Dremio doesn't currently support " + sqlCall.getOperator().getName() + " operations.");
      throw new UnsupportedOperationException();
    }

    // Disable unsupported JOINs
    if(sqlCall.getKind() == SqlKind.JOIN) {
      SqlJoin join = (SqlJoin) sqlCall;

      // DRILL-1986: Block Natural Join
      if(join.isNatural()) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
            "Dremio doesn't currently support NATURAL JOIN.");
        throw new UnsupportedOperationException();
      }

      // DRILL-1921: Block Cross Join
      if(join.getJoinType() == JoinType.CROSS && !context.getPlannerSettings().isCrossJoinEnabled()) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
            "Dremio doesn't currently support CROSS JOIN.");
        throw new UnsupportedOperationException();
      }
    }

    // DRILL-211: Disable Function
    for(String strOperator : disabledOperators) {
      if(sqlCall.getOperator().isName(strOperator, true)) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
            "Dremio doesn't currently support " + sqlCall.getOperator().getName() + ".");
        throw new UnsupportedOperationException();
      }
    }

    // DRILL-3944: Disable complex functions incorrect placement
    if(sqlCall instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlCall;

      for (SqlNode nodeInSelectList : sqlSelect.getSelectList()) {
        if (checkDirExplorers(nodeInSelectList)) {
          unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
              "Directory explorers " + dirExplorers + " functions are not supported in Select List");
          throw new UnsupportedOperationException();
        }
      }

      if (sqlSelect.hasWhere()) {
        if (checkDirExplorers(sqlSelect.getWhere()) && !context.getPlannerSettings().isConstantFoldingEnabled()) {
          unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
              "Directory explorers " + dirExplorers + " functions can not be used " +
                  "when " + PlannerSettings.CONSTANT_FOLDING.getOptionName() + " option is set to false");
          throw new UnsupportedOperationException();
        }
      }

      if(sqlSelect.hasOrderBy()) {
        for (SqlNode sqlNode : sqlSelect.getOrderList()) {
          if(containsFlatten(sqlNode)) {
            // DRILL-2181
            unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                "Dremio doesn't support using FLATTEN in the ORDER BY clause.");
            throw new UnsupportedOperationException();
          } else if (checkDirExplorers(sqlNode)) {
            unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                "Directory explorers " + dirExplorers + " functions are not supported in Order By");
            throw new UnsupportedOperationException();
          }
        }
      }

      if(sqlSelect.getGroup() != null) {
        for(SqlNode sqlNode : sqlSelect.getGroup()) {
          if(containsFlatten(sqlNode)) {
            // DRILL-2181
            unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                "Dremio doesn't support using FLATTEN in the GROUP BY clause.");
            throw new UnsupportedOperationException();
          } else if (checkDirExplorers(sqlNode)) {
                unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                "Directory explorers " + dirExplorers + " functions are not supported in Group By");
            throw new UnsupportedOperationException();
          }
        }
      }

      if(sqlSelect.isDistinct()) {
        for(SqlNode column : sqlSelect.getSelectList()) {
          if(column.getKind() ==  SqlKind.AS) {
            if(containsFlatten(((SqlCall) column).getOperandList().get(0))) {
              // DRILL-2181
              unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                 "Dremio doesn't support using FLATTEN within DISTINCT.");
              throw new UnsupportedOperationException();
            }
          } else {
            if(containsFlatten(column)) {
              // DRILL-2181
              unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                  "Dremio doesn't support using FLATTEN within DISTINCT.");
              throw new UnsupportedOperationException();
            }
          }
        }
      }
    }

    if(sqlCall.getOperator() instanceof SqlCountAggFunction) {
      for(SqlNode sqlNode : sqlCall.getOperandList()) {
        if(containsFlatten(sqlNode)) {
          // DRILL-2181
          unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
              "Dremio doesn't support using FLATTEN within an aggregation expression.");
          throw new UnsupportedOperationException();
        }
      }
    }

    // Disable EXTEND on SELECT
    if (sqlCall instanceof SqlSelect && !context.getOptions().getOption(ExecConstants.ENABLE_EXTEND_ON_SELECT)) {
      SqlSelect sqlSelect = (SqlSelect) sqlCall;
      if (sqlSelect.getFrom() != null && sqlSelect.getFrom().getKind() == SqlKind.EXTEND) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
          "Dremio doesn't currently support EXTEND.");
        throw new UnsupportedOperationException();
      }
    }

    return sqlCall.getOperator().acceptCall(this, sqlCall);
  }

  private void findAndValidateOverOperators(SqlNode node) {
    if (node.getKind() == SqlKind.OVER) {
      validateOverOperators(node);
    }
    if (node instanceof SqlCall) {
      for (SqlNode operand : ((SqlCall)node).getOperandList()) {
        if (operand != null) {
          findAndValidateOverOperators(operand);
        }
      }
    }
  }

  private void validateOverOperators(SqlNode node) {
    // Throw exceptions if window functions are disabled
    if(!context.getOptions().getOption(ExecConstants.ENABLE_WINDOW_FUNCTIONS).getBoolVal()) {
      // see DRILL-2559
      unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
        "Window functions are disabled");
      throw new UnsupportedOperationException();
    }

    // DRILL-3182, DRILL-3195
    SqlCall over = (SqlCall) node;
    if(over.getOperandList().get(0) instanceof SqlCall) {
      SqlCall function = (SqlCall) over.getOperandList().get(0);

      // DRILL-3182
      // Window function with DISTINCT qualifier is temporarily disabled
      if(function.getFunctionQuantifier() != null
        && function.getFunctionQuantifier().getValue() == SqlSelectKeyword.DISTINCT) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
          "DISTINCT for window aggregate functions is not currently supported");
        throw new UnsupportedOperationException();
      }

      // DRILL-3596: we only allow (<column-name>) or (<column-name>, 1)
      final String functionName = function.getOperator().getName().toUpperCase();
      if ("LEAD".equals(functionName) || "LAG".equals(functionName)) {
        boolean supported = true;
        if (function.operandCount() > 2) {
          // we don't support more than 2 arguments
          supported = false;
        } else if (function.operandCount() == 2) {
          SqlNode operand = function.operand(1);
          if (!(operand instanceof SqlNumericLiteral)) {
            // we only support offset as a numeric literal
            supported = false;
          }else if(SqlLiteral.unchain(operand).getValueAs(Integer.class) <= 0){
            supported = false;
          }
        }
        if (!supported) {
          unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
            "Function " + functionName + " only supports (<value expression>) or (<value expression>, offset) where 'offset' > 0");
          throw new UnsupportedOperationException();
        }
      }
    }
  }

  private void checkGroupID(SqlSelect sqlSelect) {
    final ExprFinder groupingFinder = new ExprFinder(GroupID);
    sqlSelect.accept(groupingFinder);
    if (groupingFinder.find()) {
      // DRILL-3962
      unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
          "Group_ID is not supported.");
      throw new UnsupportedOperationException();
    }
  }

  private boolean checkDirExplorers(SqlNode sqlNode) {
    final ExprFinder dirExplorersFinder = new ExprFinder(DIR_EXPLORERS_CONDITION);
    sqlNode.accept(dirExplorersFinder);
    return dirExplorersFinder.find();
  }

  /**
  /**
   * A condition that returns true if SqlNode has GROUP_ID.
   */
    private final SqlNodeCondition GroupID = new SqlNodeCondition() {
    @Override
    public boolean test(SqlNode sqlNode) {
      if (sqlNode instanceof SqlCall) {
        final SqlOperator operator = ((SqlCall) sqlNode).getOperator();
          if (operator == SqlStdOperatorTable.GROUP_ID) {
          return true;
        }
      }
      return false;
    }
  };

  private final SqlNodeCondition DIR_EXPLORERS_CONDITION = new FindFunCondition(dirExplorers);

  private final SqlNodeCondition FLATTEN_FINDER_CONDITION = new FindFunCondition(flattenNames);

  private boolean containsFlatten(SqlNode sqlNode) throws UnsupportedOperationException {
    final ExprFinder dirExplorersFinder = new ExprFinder(FLATTEN_FINDER_CONDITION);
    sqlNode.accept(dirExplorersFinder);
    return dirExplorersFinder.find();
  }

}
