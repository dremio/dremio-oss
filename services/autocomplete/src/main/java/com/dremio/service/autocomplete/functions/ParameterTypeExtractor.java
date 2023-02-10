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
package com.dremio.service.autocomplete.functions;

import java.util.Optional;
import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.parsing.SqlNodeParser;
import com.dremio.service.autocomplete.statements.grammar.TableReference;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.QueryBuilder;
import com.dremio.service.functions.model.ParameterType;
import com.dremio.service.functions.model.SqlTypeNameToParameterType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class ParameterTypeExtractor {
  private final SqlValidator sqlValidator;
  private final SqlValidatorScope sqlValidatorScope;
  private final SqlNodeParser queryParser;

  public ParameterTypeExtractor(
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope,
    SqlNodeParser queryParser) {
    Preconditions.checkNotNull(sqlValidator);
    Preconditions.checkNotNull(sqlValidatorScope);
    Preconditions.checkNotNull(queryParser);

    this.sqlValidator = sqlValidator;
    this.sqlValidatorScope = sqlValidatorScope;
    this.queryParser = queryParser;
  }

  public Result extract(
    ParsedFunction parsedFunction,
    Set<ColumnAndTableAlias> columns,
    ImmutableList<TableReference> tableReferences) {
    int cursorIndex = Iterables.indexOf(
      parsedFunction.getParameters(),
      parameter -> Cursor.tokensHasCursor(parameter.getTokens()));

    ImmutableList<ParameterType> parameterTypesBeforeCursor = getParameterTypes(
      parsedFunction
        .getParameters()
        .subList(0, cursorIndex),
      tableReferences,
      columns);

    ImmutableList<ParameterType> parameterTypesAfterCursor = getParameterTypes(
      parsedFunction
        .getParameters()
        .subList(cursorIndex + 1, parsedFunction.getParameters().size()),
      tableReferences,
      columns);

    Result result = new Result(
      parameterTypesBeforeCursor,
      parameterTypesAfterCursor);

    return result;
  }

  public ImmutableList<ParameterType> getParameterTypes(
    ImmutableList<ParsedParameter> parsedParameters,
    ImmutableList<TableReference> tableReferences,
    Set<ColumnAndTableAlias> columns) {
    return parsedParameters
      .stream()
      .map(parsedParameter -> tryGetParameterType(
        parsedParameter,
        tableReferences,
        columns))
      .map(optional -> optional.orElse(ParameterType.ANY))
      .collect(ImmutableList.toImmutableList());
  }


  private Optional<ParameterType> tryGetParameterType(
    ParsedParameter parsedParameter,
    ImmutableList<TableReference> tableReferences,
    Set<ColumnAndTableAlias> columns) {
    try {
      return Optional.of(getParameterType(parsedParameter, tableReferences, columns));
    } catch (Exception ex) {
      return Optional.empty();
    }
  }

  private ParameterType getParameterType(
    ParsedParameter parsedParameter,
    ImmutableList<TableReference> tableReferences,
    Set<ColumnAndTableAlias> columns) {
    SqlNode sqlNode = convertParameterToSqlNode(parsedParameter, tableReferences);
    SqlTypeName sqlTypeName;
    if (sqlNode instanceof SqlLiteral) {
      SqlLiteral sqlLiteral = (SqlLiteral) sqlNode;
      sqlTypeName = sqlLiteral.getTypeName();
    } else if (sqlNode instanceof SqlCall) {
      SqlCall sqlCall = (SqlCall) sqlNode;
      sqlTypeName = sqlCall
        .getOperator()
        .deriveType(
          sqlValidator,
          sqlValidatorScope,
          sqlCall)
        .getSqlTypeName();
    } else if (sqlNode instanceof SqlIdentifier) {
      SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
      String name = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
      Optional<ColumnAndTableAlias> optionalColumn = columns
        .stream()
        .filter(column -> column.getColumn().getName().equalsIgnoreCase(name))
        .findAny();
      if (!optionalColumn.isPresent()) {
        throw new RuntimeException("FAILED TO FIND COLUMN WITH NAME: " + name);
      }

      sqlTypeName = optionalColumn.get().getColumn().getType();
    } else {
      throw new RuntimeException("Cannot derive SqlNode with kind: " + sqlNode.getKind());
    }

    return SqlTypeNameToParameterType.convert(sqlTypeName);
  }

  private SqlNode convertParameterToSqlNode(ParsedParameter parsedParameter, ImmutableList<TableReference> tableReferences) {
    String queryText = QueryBuilder.build(parsedParameter.getTokens(), tableReferences);
    SqlNode sqlNode = queryParser.parse(queryText);
    return ((SqlSelect) sqlNode).getSelectList().get(0);
  }

  public static final class Result {
    private final ImmutableList<ParameterType> beforeCursor;
    private final ImmutableList<ParameterType> afterCursor;

    private Result(
      ImmutableList<ParameterType> beforeCursor,
      ImmutableList<ParameterType> afterCursor) {
      this.beforeCursor = beforeCursor;
      this.afterCursor = afterCursor;
    }

    public ImmutableList<ParameterType> getBeforeCursor() {
      return beforeCursor;
    }

    public ImmutableList<ParameterType> getAfterCursor() {
      return afterCursor;
    }
  }
}
