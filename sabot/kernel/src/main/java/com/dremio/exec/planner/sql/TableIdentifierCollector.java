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
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
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
public class TableIdentifierCollector extends AncestorsVisitor {

  private final Collection<SqlIdentifier> tempIds;

  public TableIdentifierCollector(
      final Collection<SqlIdentifier> namespaceIds, final Collection<SqlIdentifier> tempIds) {
    super(namespaceIds);
    this.tempIds = tempIds;
  }

  public static List<SqlIdentifier> collect(final SqlNode sqlNode) {
    // Collect all namespace identifiers including temporary relation identifiers
    final ArrayList<SqlIdentifier> namespaceIds = new ArrayList<>();
    final ArrayList<SqlIdentifier> tempIds = new ArrayList<>();
    sqlNode.accept(new TableIdentifierCollector(namespaceIds, tempIds));
    // Return only valid table/view identifiers
    return namespaceIds.stream()
        .filter(id -> tempIds.stream().noneMatch(tempId -> tempId.equalsDeep(id, Litmus.IGNORE)))
        .collect(Collectors.toList());
  }

  @Override
  public Void visit(SqlCall call) {
    switch (call.getKind()) {
      case WITH:
        // Collect temporary table identifiers
        final SqlWith with = (SqlWith) call;
        with.withList.forEach(
            sqlNode -> {
              final SqlWithItem withItem = (SqlWithItem) sqlNode;
              tempIds.add(withItem.name);
            });
        return super.visit(call);
      case COLLECTION_TABLE:
        // Override superclass behavior. We don't want
        // to collect identifiers for table functions.
        return null;
    }
    return super.visit(call);
  }
}
