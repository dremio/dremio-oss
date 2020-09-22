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
package com.dremio.service.jobs.metadata;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlVisitor;

import com.dremio.exec.calcite.SqlNodes;
import com.dremio.exec.planner.sql.BaseSqlVisitor;

/**
 * Visits a query AST to find its ancestors
 * if multiLevel is false, any views referred in the query won't be expanded
 */
public final class AncestorsVisitor implements SqlVisitor<List<SqlIdentifier>> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AncestorsVisitor.class);
  private boolean multiLevel;

  private AncestorsVisitor(boolean multiLevel) {
    this.multiLevel = multiLevel;
  }

  public static List<SqlIdentifier> extractAncestors(final SqlNode sqlNode) {
    return extractAncestors(sqlNode, false);
  }

  public static List<SqlIdentifier> extractAncestors(final SqlNode sqlNode, boolean multiLevel) {
    return sqlNode.accept(new AncestorsVisitor(multiLevel));
  }

  @Override
  public List<SqlIdentifier> visit(SqlLiteral literal) {
    return Collections.emptyList();
  }

  @Override
  public List<SqlIdentifier> visit(SqlCall call) {
    List<SqlIdentifier> result = new ArrayList<>();
    switch (call.getKind()) {
    case SELECT:
      SqlSelect select = (SqlSelect)call;
      final SqlNode from = select.getFrom();
      if (from != null) {
        result.addAll(extractAncestorsFromFrom(from));
      }
      break;
    default:
      result.addAll(visitAll(call.getOperandList()));
      break;
    }
    return result;
  }

  private List<SqlIdentifier> extractAncestorsFromFrom(SqlNode from) {
    return
        from.accept(new BaseSqlVisitor<List<SqlIdentifier>>() {
      @Override
      public List<SqlIdentifier> visit(SqlIdentifier id) {
        return Arrays.asList(id);
      }
      @Override
      public List<SqlIdentifier> visit(SqlCall call) {
        SqlOperator operator = call.getOperator();
        switch (operator.getKind()) {
        case AS:
          SqlNode sqlNode = call.getOperandList().get(0);
          switch (sqlNode.getKind()) {
          case IDENTIFIER:
            return asList((SqlIdentifier)sqlNode);
          case SELECT:
            return extractAncestors(sqlNode);
          case COLLECTION_TABLE: // table function
            SqlNode operand = ((SqlCall)sqlNode).operand(0);
            if (operand.getKind() == SqlKind.OTHER_FUNCTION) {
              SqlFunction tableFunction = (SqlFunction)((SqlCall)operand).getOperator();
              return asList(tableFunction.getSqlIdentifier());
            }
            // pass through
          case VALUES:
            return Collections.emptyList();
          default:
            logger.warn("Failure while extracting parents from sql. Unexpected 1st operand in AS: {}. SQL: \n {}", sqlNode.getKind() ,SqlNodes.toTreeString(sqlNode));
            return Collections.emptyList();
          }
        case JOIN:
          SqlJoin join = (SqlJoin)call;
          List<SqlIdentifier> result = new ArrayList<>();
          result.addAll(join.getLeft().accept(this));
          result.addAll(join.getRight().accept(this));
          return result;
        case SELECT:
          if (multiLevel) {
            if (((SqlSelect) call).getFrom() != null) {
              return extractAncestorsFromFrom(((SqlSelect) call).getFrom());
            } else {
              return Collections.emptyList();
            }
          }
        default:
          throw new UnsupportedOperationException("Unexpected operator in call: " + operator.getKind() + "\n" + SqlNodes.toTreeString(call));
        }
      }
    });
  }

  @Override
  public List<SqlIdentifier> visit(SqlNodeList nodeList) {
    return visitAll(nodeList.getList());
  }

  private List<SqlIdentifier> visitAll(List<SqlNode> list) {
    List<SqlIdentifier> result = new ArrayList<>();
    for (SqlNode sqlNode : list) {
      if (sqlNode != null) {
        result.addAll(sqlNode.accept(this));
      }
    }
    return result;
  }

  @Override
  public List<SqlIdentifier> visit(SqlIdentifier id) {
    return Collections.emptyList();
  }

  @Override
  public List<SqlIdentifier> visit(SqlDataTypeSpec type) {
    return Collections.emptyList();
  }

  @Override
  public List<SqlIdentifier> visit(SqlDynamicParam param) {
    return Collections.emptyList();
  }

  @Override
  public List<SqlIdentifier> visit(SqlIntervalQualifier intervalQualifier) {
    return Collections.emptyList();
  }
}
