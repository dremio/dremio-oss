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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.TableVersionType;
import com.dremio.exec.planner.sql.parser.SqlVersionedTableMacroCall;
import com.dremio.exec.planner.sql.parser.TableVersionSpec;
import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.catalog.AutocompleteSchemaProvider;
import com.dremio.service.autocomplete.parsing.SqlNodeParser;
import com.dremio.service.autocomplete.parsing.ValidatingParser;
import com.dremio.service.autocomplete.statements.grammar.FromClause;
import com.dremio.service.autocomplete.statements.grammar.SelectQueryStatement;
import com.dremio.service.autocomplete.statements.grammar.Statement;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

/**
 * Resolves the schema given the context of the cursor in the query.
 */
public final class ColumnResolver {
  private final SqlNodeParser queryParser;
  private final AutocompleteSchemaProvider schemaProvider;

  public ColumnResolver(
    AutocompleteSchemaProvider schemaProvider,
    SqlNodeParser queryParser) {
    Preconditions.checkNotNull(schemaProvider);
    Preconditions.checkNotNull(queryParser);
    this.schemaProvider = schemaProvider;
    this.queryParser = queryParser;
  }

  public Set<ColumnAndTableAlias> resolve(List<Statement> statementPath) {
    Preconditions.checkNotNull(statementPath);
    Set<ColumnAndTableAlias> columns = new TreeSet<>(ColumnAndTableAliasComparator.INSTANCE);
    for (int i = 0; i < statementPath.size(); i++) {
      Statement statement = statementPath.get(i);
      if (statement instanceof SelectQueryStatement) {
        SelectQueryStatement selectQueryStatement = (SelectQueryStatement) statement;
        // If we have a scenario like:
        // SELECT * FROM (SELECT ^ FROM EMP)
        // Then we don't want to use the from clause from the outer query, since it's an invalid query
        // But we do want to use a partial from clause if it's in the current query:
        // SELECT * FROM EMP JOIN DEPT ON EMP.ID = ^
        FromClause fromClause = selectQueryStatement.getFromClause();
        boolean invalidFromClause = (i != statementPath.size() - 1) && Cursor.tokensHasCursor(fromClause.getTokens());
        if (!invalidFromClause) {
          columns.addAll(resolve(fromClause));
        }
      }
    }

    return columns;
  }

  public Set<ColumnAndTableAlias> resolve(FromClause fromClause) {
    // Take the FROM clause and append it with SELECT *
    // So that we have a query in the from:
    // SELECT * <FROM_CLAUSE>
    String modifiedQuery = QueryBuilder.build(fromClause);
    SqlNode parsedQuery = queryParser.toSqlNode(modifiedQuery);
    SqlSelect sqlSelect = (SqlSelect) parsedQuery;

    return resolveFromSelectListAndFromNode(
      getSelectListWithoutAliases(sqlSelect.getSelectList()),
      sqlSelect.getFrom(),
      schemaProvider);
  }

  public static ColumnResolver create(
    final AutocompleteSchemaProvider autocompleteSchemaProvider,
    final SqlOperatorTable operatorTable,
    final SimpleCatalog<?> catalog) {
    ValidatingParser parser = ValidatingParser.create(
      operatorTable,
      catalog);

    final ColumnResolver columnResolver = new ColumnResolver(
      autocompleteSchemaProvider,
      parser);

    return columnResolver;
  }

  public static ColumnResolver create(AutocompleteEngineContext autocompleteEngineContext) {
    return create(
      autocompleteEngineContext.getAutocompleteSchemaProvider(),
      autocompleteEngineContext.getOperatorTable(),
      autocompleteEngineContext.getCatalog());
  }

  private static SqlNodeList getSelectListWithoutAliases(SqlNodeList selectList) {
    List<SqlNode> nodesWithoutAlias = new ArrayList<>();
    for (SqlNode selectItem : selectList) {
      SqlNode nodeWithoutAlias;
      switch (selectItem.getKind()) {
        case IDENTIFIER:
          // Add it as is
          nodeWithoutAlias = selectItem;
          break;

        case AS:
          // Add without the alias
          nodeWithoutAlias = ((SqlBasicCall) selectItem).operand(0);
          break;

        default:
          throw new RuntimeException("Unknown SqlKind: " + selectItem.getKind());
      }

      nodesWithoutAlias.add(nodeWithoutAlias);
    }

    return new SqlNodeList(nodesWithoutAlias, selectList.getParserPosition());
  }

  private static Set<ColumnAndTableAlias> resolveFromSelectListAndFromNode(
    SqlNodeList selectList,
    SqlNode fromNode,
    AutocompleteSchemaProvider schemaProvider) {
    final Map<String, Set<Column>> aliasToSchemaMapping = new HashMap<>();
    populateAliasToCatalogNodeMapFromNode(
      schemaProvider,
      fromNode,
      aliasToSchemaMapping);

    return createColumnAndPathSet(
      aliasToSchemaMapping,
      selectList);
  }

  private static void populateAliasToCatalogNodeMapFromNode(
    AutocompleteSchemaProvider schemaProvider,
    SqlNode fromNode,
    Map<String, Set<Column>> aliasToSchemaMapping) {
    if (fromNode == null) {
      return;
    }
    switch (fromNode.getKind()) {
      case AS:
        populateAliasToCatalogNodeMapAsNode(
          schemaProvider,
          fromNode,
          aliasToSchemaMapping);
        break;

      case JOIN:
        SqlJoin sqlJoin = (SqlJoin) fromNode;
        populateAliasToCatalogNodeMapFromNode(
          schemaProvider,
          sqlJoin.getLeft(),
          aliasToSchemaMapping);
        populateAliasToCatalogNodeMapFromNode(
          schemaProvider,
          sqlJoin.getRight(),
          aliasToSchemaMapping);
        break;

      default:
        throw new RuntimeException("Unknown SqlKind: " + fromNode.getKind());
    }
  }

  private static void populateAliasToCatalogNodeMapAsNode(
    AutocompleteSchemaProvider schemaProvider,
    SqlNode asNode,
    Map<String, Set<Column>> aliasToSchemaMapping) {
    assert asNode.getKind() == SqlKind.AS;

    SqlNode aliasNode = ((SqlBasicCall) asNode).getOperands()[1];
    ImmutableList<String> aliasPath = ((SqlIdentifier) aliasNode).names;
    assert aliasPath.size() == 1;
    String alias = aliasPath.get(0);
    SqlNode sourceNode = ((SqlBasicCall) asNode).getOperands()[0];
    Set<Column> columns;
    switch (sourceNode.getKind()) {
      case IDENTIFIER:
        ImmutableList<String> catalogPath = ((SqlIdentifier) sourceNode).names;
        columns = schemaProvider.getColumnsByFullPath(catalogPath);
        break;

      case SELECT:
        SqlSelect subSelectNode = (SqlSelect) sourceNode;
        Set<ColumnAndTableAlias> columnAndTableAliases = resolveFromSelectListAndFromNode(
          subSelectNode.getSelectList(),
          subSelectNode.getFrom(),
          schemaProvider);
        // A subquery creates a new table on the fly
        columns = columnAndTableAliases
          .stream()
          .map(ColumnAndTableAlias::getColumn)
          .collect(ImmutableSet.toImmutableSet());
        break;

      case COLLECTION_TABLE:
        SqlVersionedTableMacroCall sqlVersionedTableMacroCall = (SqlVersionedTableMacroCall) ((SqlBasicCall) sourceNode).getOperands()[0];
        TableVersionSpec tableVersionSpec = sqlVersionedTableMacroCall.getTableVersionSpec();
        TableVersionContext tableVersionContext = tableVersionSpec.getResolvedTableVersionContext();
        TableVersionType tableVersionType = tableVersionContext.getType();
        if (alias.startsWith("EXPR$")) {
          alias = sqlVersionedTableMacroCall.getAlias().names.get(0);
        }

        String[] pathTokens = ((SqlVersionedTableMacroCall) ((SqlBasicCall) sourceNode).getOperands()[0])
          .getOperands()[0]
          .toString()
          .replaceAll("\'", "")
          .split("\\.");
        pathTokens = Arrays.stream(pathTokens).map(PathUtils::removeQuotes).toArray(String[]::new);
        columns = schemaProvider.getColumnsByFullPath(ImmutableList.copyOf(pathTokens));
        break;

      default:
        throw new RuntimeException("Unknown SqlKind: " + sourceNode.getKind());
    }

    aliasToSchemaMapping.put(alias, columns);
  }

  private static Set<ColumnAndTableAlias> createColumnAndPathSet(
    Map<String, Set<Column>> aliasToSchemaMapping,
    SqlNodeList selectList) {
    boolean useHackForTimeTravel = true;
    for (SqlNode selectItem : selectList.getList()) {
      if (!(selectItem instanceof SqlIdentifier)) {
        useHackForTimeTravel = false;
        break;
      }

      if (!Iterables.getLast(((SqlIdentifier) selectItem).names).equals("id")) {
        useHackForTimeTravel = false;
        break;
      }
    }

    Set<ColumnAndTableAlias> columnAndTableAliases = new TreeSet<>(ColumnAndTableAliasComparator.INSTANCE);
    if (useHackForTimeTravel) {
      // Time Travel returns only the id column for the projection on the validation rewrite regardless of the schema
      // So in those cases we just return all the columns
      for (SqlNode selectItem : selectList.getList()) {
        SqlIdentifier columnPathIdentifier = (SqlIdentifier) selectItem;
        String tableAlias = columnPathIdentifier.names.get(0);
        Set<Column> columns = aliasToSchemaMapping.get(tableAlias);

        for (Column column : columns) {
          columnAndTableAliases.add(ColumnAndTableAlias.createWithTable(column, tableAlias));
        }
      }

      return columnAndTableAliases;
    }

    for (SqlNode selectItem : selectList.getList()) {
      SqlNode operand; // Alias in the from <table alias>.<column alias>
      String alias;
      switch (selectItem.getKind()) {
        case IDENTIFIER:
          operand = selectItem;
          alias = null;
          break;

        case AS:
          operand = ((SqlBasicCall) selectItem).operand(0);
          alias = ((SqlIdentifier) (((SqlBasicCall) selectItem).operand(1))).names.get(0);
          break;

        default:
          // Let's just ignore things like literals or function calls.
          // This only has effect on being able to figure out the return type.
          // TODO[AG]: Add support for typing later. Should be fairly easy
          continue;
      }

      if (operand instanceof SqlIdentifier) {
        SqlIdentifier columnPathIdentifier = (SqlIdentifier) operand; // Alias in the from <table alias>.<column alias>
        // Get the table alias (excluding the column)
        assert columnPathIdentifier.names.size() == 2;
        String tableAlias = columnPathIdentifier.names.get(0);
        Set<Column> columns = aliasToSchemaMapping.get(tableAlias);
        String columnName = Iterables.getLast(columnPathIdentifier.names);
        Column column = columns
          .stream()
          .filter(candidate -> candidate.getName().equalsIgnoreCase(columnName))
          .findAny()
          .orElseGet(() -> Column.anyTypeColumn(columnName));
        if (alias != null) {
          column = Column.typedColumn(alias, column.getType());
        }

        columnAndTableAliases.add(ColumnAndTableAlias.createWithTable(column, tableAlias));
      } else {
        // If it is not an identifier, assume any type for simplicity
        // TODO[AG]:This will mess up typing in the autocomplete, but let's treat that as a known issue.
        Column column = Column.anyTypeColumn(alias);
        columnAndTableAliases.add(ColumnAndTableAlias.createAnonymous(column));
      }
    }

    return columnAndTableAliases;
  }
}
