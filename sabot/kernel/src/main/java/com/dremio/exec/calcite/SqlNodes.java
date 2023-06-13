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
package com.dremio.exec.calcite;

import static java.util.Arrays.asList;

import java.util.List;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlVisitor;

import com.dremio.common.utils.SqlUtils;

/**
 * Utilities to work with SqlNode
 *
 */
public class SqlNodes {
  private static SqlDialect DREMIO_DIALECT =
      new SqlDialect(DatabaseProduct.UNKNOWN, "Dremio", Character.toString(SqlUtils.QUOTE), NullCollation.FIRST);
  /**
   * @param sqlNode
   * @return SQL representation of the node
   */
  public static String toSQLString(SqlNode sqlNode) {
    SqlPrettyWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);
    writer.setSelectListItemsOnSeparateLines(false);
    writer.setIndentation(0);
    writer.setQuoteAllIdentifiers(false);
    sqlNode.unparse(writer, 0, 0);
    return writer.toString();
  }

  /**
   * @param node
   * @return tree representation of the node
   */
  public static String toTreeString(SqlNode node) {
    PrinterVisitor visitor = new PrinterVisitor();
    node.accept(visitor);
    return visitor.toString();
  }

}

/**
 * Visitor to print as a tree
 */
class PrinterVisitor implements SqlVisitor<Void> {
  private StringBuilder sb = new StringBuilder();
  private int indent = 0;

  private Void format(SqlNode parent) {
    return format(parent, SqlNodes.toSQLString(parent));
  }

  private Void format(SqlNode parent, String desc, SqlNode... nodes) {
    return format(parent, desc, asList(nodes));
  }

  private String label(SqlKind kind, int i) {
    switch (kind) {
    case SELECT:
      switch(i)  {
      case 0:
        return "keywords";
      case 1:
        return "select";
      case 2:
        return "from";
      case 3:
        return "where";
      case 4:
        return "groupBy";
      case 5:
        return "having";
      case 6:
        return "windowDecls";
      case 7:
        return "orderBy";
      case 8:
        return "offset";
      case 9:
        return "fetch";
      default:
        break;
      }
      break;
    case ORDER_BY:
      switch(i) {
      case 0:
        return "query";
      case 1:
        return "orderList";
      case 2:
        return "offset";
      case 3:
        return "fetch";
      default:
        break;
      }
      break;
    default:
      break;
    }
    return String.valueOf(i);
  }

  private Void format(SqlNode parent, String desc, List<SqlNode> children) {
    sb.append(parent.getKind()).append(": ").append(desc);
    if (children.size() > 0) {
      sb.append(" {\n");
      indent += 1;
      int i = 0;
      for (SqlNode sqlNode : children) {
        indent();
        sb.append(label(parent.getKind(), i)).append(": ");
        if (sqlNode == null) {
          sb.append("null");
        } else {
          sqlNode.accept(this);
        }
        sb.append(",\n");
        ++ i;
      }
      indent();
      sb.append("}");
      indent -= 1;
    }
    return null;
  }

  private void indent() {
    for (int i = 0; i < indent; i++) {
      sb.append("  ");
    }
  }

  @Override public Void visit(SqlLiteral literal) {
    return format(literal);
  }

  @Override public Void visit(SqlCall call) {
    return format(call, call.getOperator().toString(), call.getOperandList());
  }

  @Override public Void visit(SqlNodeList nodeList) {
    return format(nodeList, "list", nodeList.getList());
  }

  @Override public Void visit(SqlIdentifier id) {
    return format(id);
  }

  @Override public Void visit(SqlDataTypeSpec type) {
    return format(type);
  }

  @Override public Void visit(SqlDynamicParam param) {
    return format(param);
  }

  @Override public Void visit(SqlIntervalQualifier intervalQualifier) {
    return format(intervalQualifier);
  }

  @Override
  public String toString() {
    return sb.toString();
  }
}
