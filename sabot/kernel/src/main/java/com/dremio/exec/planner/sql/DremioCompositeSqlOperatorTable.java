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

import java.util.List;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import com.google.common.collect.ImmutableList;

public class DremioCompositeSqlOperatorTable implements SqlOperatorTable {
  private static final DremioCompositeSqlOperatorTable INSTANCE = new DremioCompositeSqlOperatorTable();
  private static final SqlOperatorTable DREMIO_OT = DremioSqlOperatorTable.instance();
  private static final SqlOperatorTable STD_OT = SqlStdOperatorTable.instance();
  private static final SqlOperatorTable ORACLE_OT =
      new ListSqlOperatorTable(SqlLibraryOperatorTableFactory.INSTANCE
          .getOperatorTable(SqlLibrary.ORACLE).getOperatorList().stream()
          .filter(op -> op != SqlLibraryOperators.LTRIM)
          .filter(op -> op != SqlLibraryOperators.RTRIM)
          .filter(op -> op != SqlLibraryOperators.SUBSTR) // calcite does not support oracles substring CALCITE-4408
          .filter(op -> op != SqlLibraryOperators.DECODE) // Dremio currently uses hive decode
          .collect(ImmutableList.toImmutableList()));
  private static final List<SqlOperator> operators = ImmutableList.<SqlOperator>builder()
      .addAll(DREMIO_OT.getOperatorList())
      .addAll(STD_OT.getOperatorList().stream()
        .filter(op -> op != SqlStdOperatorTable.ROUND)
        .filter(op -> op != SqlStdOperatorTable.TRUNCATE)
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
}
