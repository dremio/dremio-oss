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
package com.dremio.service.autocomplete.columns;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import com.dremio.service.autocomplete.DremioToken;
import com.dremio.service.autocomplete.SqlQueryUntokenizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Resolves the schema given the context of the cursor in the query.
 */
public final class ColumnResolver {
  private final ColumnReader columnReader;
  private final DremioQueryParser queryParser;

  public ColumnResolver(
    ColumnReader columnReader,
    DremioQueryParser queryParser) {
    Preconditions.checkNotNull(columnReader);
    Preconditions.checkNotNull(queryParser);

    this.columnReader = columnReader;
    this.queryParser = queryParser;
  }

  public Map<List<String>, Set<Column>> resolve(
    final ImmutableList<DremioToken> fromClauseTokens) {
    Preconditions.checkNotNull(fromClauseTokens);

    // Take the FROM clause and append it with SELECT *
    // So that we have a query in the from:
    // SELECT * <FROM_CLAUSE>
    final String modifiedQuery = new StringBuilder("SELECT * ")
      .append(SqlQueryUntokenizer.untokenize(fromClauseTokens))
      .toString();
    final RelNode modifiedQueryPlan = queryParser.toRel(modifiedQuery);

    final ImmutableMap.Builder<List<String>, Set<Column>> tableToColumnsBuilder = new ImmutableMap.Builder<>();
    final List<RelOptTable> relOptTables = RelOptUtil.findAllTables(modifiedQueryPlan);
    for (RelOptTable relOptTable : relOptTables) {
      final List<String> qualifiedName = relOptTable.getQualifiedName();
      final Optional<Set<Column>> columns = columnReader.getColumnsForTableWithName(qualifiedName);
      if (columns.isPresent()) {
        tableToColumnsBuilder.put(qualifiedName, columns.get());
      }
    }

    return tableToColumnsBuilder.build();
  }
}
