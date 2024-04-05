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

import com.dremio.exec.tablefunctions.TableMacroNames;
import com.dremio.exec.tablefunctions.VersionedTableMacro;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;

/** Visits a query AST to find its ancestors including temporary relations. */
public class AncestorsVisitor extends SqlBasicVisitor<Void> {
  private final Collection<SqlIdentifier> ancestorIds;
  private boolean extractIdentifiers = false;

  public AncestorsVisitor(final Collection<SqlIdentifier> ancestorIds) {
    this.ancestorIds = ancestorIds;
  }

  public static List<SqlIdentifier> extractAncestors(final SqlNode sqlNode) {
    final List<SqlIdentifier> ids = new ArrayList<>();
    sqlNode.accept(new AncestorsVisitor(ids));
    return ids;
  }

  @Override
  public Void visit(SqlCall call) {
    switch (call.getKind()) {
      case SELECT:
        final SqlSelect select = (SqlSelect) call;
        final SqlNode from = select.getFrom();
        // We expect to find ancestor identifiers when visiting a FROM node
        call.getOperandList().forEach(node -> visitNode(node, node == from));
        return null;
      case AS:
        // Only visit first child. Second child is always an alias.
        visitNode(call.getOperandList().get(0), extractIdentifiers);
        return null;
      case JOIN:
        // We expect to find ancestor identifiers when visiting the children of a join
        call.getOperandList().forEach(node -> visitNode(node, true));
        return null;
      case COLLECTION_TABLE: // table function
        SqlNode operand = call.getOperandList().get(0);
        if (operand.getKind() == SqlKind.OTHER_FUNCTION) {
          SqlFunction tableFunction = (SqlFunction) ((SqlCall) operand).getOperator();
          if (tableFunction.getNameAsId().names.equals(TableMacroNames.TIME_TRAVEL)) {
            SqlLiteral qualifiedName = (SqlLiteral) ((SqlCall) operand).getOperandList().get(0);
            List<String> nameParts =
                VersionedTableMacro.splitTableIdentifier(qualifiedName.getValueAs(String.class));
            ancestorIds.add(new SqlIdentifier(nameParts, SqlParserPos.ZERO));
            return null;
          }
          ancestorIds.add(tableFunction.getSqlIdentifier());
          return null;
        }
        return null;
      default:
        call.getOperandList().forEach(node -> visitNode(node, false));
        return null;
    }
  }

  @Override
  public Void visit(SqlNodeList nodeList) {
    nodeList.forEach(node -> visitNode(node, extractIdentifiers));
    return null;
  }

  @Override
  public Void visit(SqlIdentifier id) {
    if (extractIdentifiers) {
      ancestorIds.add(id);
    }
    return null;
  }

  private void visitNode(final SqlNode node, final boolean val) {
    if (node == null) {
      return;
    }
    boolean prev = extractIdentifiers;
    extractIdentifiers = val;
    node.accept(this);
    extractIdentifiers = prev;
  }
}
