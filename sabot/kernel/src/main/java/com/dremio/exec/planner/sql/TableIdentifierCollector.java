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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Litmus;

/**
 *
 *
 * <pre>
 * Collects table/view identifiers from a sql node tree.
 * Identifiers corresponding to temporary tables are excluded.
 * Identifiers referenced within table functions, e.g.
 * SELECT * FROM T AT BRANCH MAIN,
 * are excluded as well.
 * </pre>
 */
public class TableIdentifierCollector extends SqlBasicVisitor<Void> {

  private final Collection<SqlIdentifier> tableIds;
  private final Collection<SqlIdentifier> tempIds;
  private boolean extractIdentifiers = false;

  public TableIdentifierCollector(
      final Collection<SqlIdentifier> tableIds, final Collection<SqlIdentifier> tempIds) {
    this.tableIds = tableIds;
    this.tempIds = tempIds;
  }

  public static List<SqlIdentifier> collect(final SqlNode sqlNode) {
    // Collect all table identifiers including temporary relation identifiers
    final ArrayList<SqlIdentifier> tableIds = new ArrayList<>();
    final ArrayList<SqlIdentifier> tempIds = new ArrayList<>();
    sqlNode.accept(new TableIdentifierCollector(tableIds, tempIds));
    // Return only valid table/view identifiers
    return tableIds.stream()
        .filter(id -> tempIds.stream().noneMatch(tempId -> tempId.equalsDeep(id, Litmus.IGNORE)))
        .collect(Collectors.toList());
  }

  @Override
  public Void visit(SqlCall call) {
    switch (call.getKind()) {
      case SELECT:
        final SqlSelect select = (SqlSelect) call;
        final SqlNode from = select.getFrom();
        // We expect to find table identifiers when visiting a FROM node
        call.getOperandList().forEach(node -> visitNode(node, node == from));
        return null;
      case AS:
        // Only visit first child. Second child is always an alias.
        visitNode(call.getOperandList().get(0), extractIdentifiers);
        return null;
      case JOIN:
        // We expect to find table identifiers when visiting the children of a join
        call.getOperandList().forEach(node -> visitNode(node, true));
        return null;
      case WITH:
        // Collect temporary table identifiers
        final SqlWith with = (SqlWith) call;
        with.withList.forEach(
            sqlNode -> {
              final SqlWithItem withItem = (SqlWithItem) sqlNode;
              tempIds.add(withItem.name);
            });
        return super.visit(call);
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
      tableIds.add(id);
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
