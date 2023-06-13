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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class DremioCompositeSqlOperatorTable implements SqlOperatorTable {
  private static final DremioCompositeSqlOperatorTable INSTANCE = new DremioCompositeSqlOperatorTable();
  private static final SqlOperatorTable DREMIO_OT = DremioSqlOperatorTable.instance();

  private static final SqlOperatorTable STD_OT = FilteredSqlOperatorTable.create(
    SqlStdOperatorTable.instance(),
    // REPLACE just uses the precision of it's first argument, which is problematic if the string increases in length after replacement.
    SqlStdOperatorTable.REPLACE,
    // CARDINALITY in Calcite accepts MAP, LIST and STRUCT. In Dremio, we plan to support only MAP and LIST.
    SqlStdOperatorTable.CARDINALITY
  );
  private static final SqlOperatorTable ORACLE_OT = FilteredSqlOperatorTable.create(
    SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(SqlLibrary.ORACLE),
    SqlLibraryOperators.LTRIM,
    SqlLibraryOperators.RTRIM,
    SqlLibraryOperators.SUBSTR, // calcite does not support oracles substring CALCITE-4408
    SqlLibraryOperators.DECODE // Dremio currently uses hive decode
  );
  private static final List<SqlOperator> operators = ImmutableList.<SqlOperator>builder()
      .addAll(DREMIO_OT.getOperatorList())
      .addAll(STD_OT.getOperatorList().stream()
        .filter(op -> op != SqlStdOperatorTable.ROUND)
        .filter(op -> op != SqlStdOperatorTable.TRUNCATE)
        .filter(op -> op != SqlStdOperatorTable.CARDINALITY)
        .collect(ImmutableList.toImmutableList()))
      .addAll(ORACLE_OT.getOperatorList())
      .build();

  @Override
  public void lookupOperatorOverloads(
      SqlIdentifier sqlIdentifier,
      SqlFunctionCategory sqlFunctionCategory,
      SqlSyntax sqlSyntax,
      List<SqlOperator> list,
      SqlNameMatcher nameMatcher) {
    //Search until an operator is found.
    DREMIO_OT.lookupOperatorOverloads(sqlIdentifier, sqlFunctionCategory, sqlSyntax, list, nameMatcher);
    if (!list.isEmpty()) {
      return;
    }
    STD_OT.lookupOperatorOverloads(sqlIdentifier,sqlFunctionCategory, sqlSyntax, list, nameMatcher);
    if (!list.isEmpty()) {
      return;
    }
    ORACLE_OT.lookupOperatorOverloads(sqlIdentifier,sqlFunctionCategory, sqlSyntax, list, nameMatcher);
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return operators;
  }

  public static DremioCompositeSqlOperatorTable getInstance() {
    return INSTANCE;
  }

  /**
   * Takes a SqlOperatorTable, but ignores lookups for a select number of functions.
   * This is needed, since we don't want the default behavior or calcite's operator table,
   * but we also don't want to create a whole new operator table which may have different behavior.
   */
  private static final class FilteredSqlOperatorTable implements SqlOperatorTable {
    private final SqlOperatorTable sqlOperatorTable;
    private final ImmutableSet<String> functionsToFilter;

    private FilteredSqlOperatorTable(
      SqlOperatorTable sqlOperatorTable,
      ImmutableSet<String> functionsToFilter) {
      this.functionsToFilter = functionsToFilter;
      this.sqlOperatorTable = sqlOperatorTable;
    }

    @Override
    public void lookupOperatorOverloads(
      SqlIdentifier opName,
      SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList,
      SqlNameMatcher nameMatcher) {
      if (opName.isSimple() && functionsToFilter.contains(opName.getSimple().toUpperCase())) {
        return;
      }

      sqlOperatorTable.lookupOperatorOverloads(
        opName,
        category,
        syntax,
        operatorList,
        nameMatcher);
    }

    @Override
    public List<SqlOperator> getOperatorList() {
      return sqlOperatorTable
        .getOperatorList()
        .stream()
        .filter(operator -> !functionsToFilter.contains(operator.getName().toUpperCase()))
        .collect(Collectors.toList());
    }

    public static SqlOperatorTable create(SqlOperatorTable sqlOperatorTable, SqlOperator ... operatorsToFilter) {
      ImmutableSet<String> functionsToFilter = Arrays
        .stream(operatorsToFilter)
        .map(operator -> operator.getName().toUpperCase())
        .collect(ImmutableSet.toImmutableSet());

      return new FilteredSqlOperatorTable(sqlOperatorTable, functionsToFilter);
    }
  }
}
