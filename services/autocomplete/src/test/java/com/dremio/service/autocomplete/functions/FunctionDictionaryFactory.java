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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;

import com.dremio.service.autocomplete.OperatorTableFactory;
import com.dremio.service.autocomplete.catalog.Metadata;
import com.dremio.service.autocomplete.catalog.MockCatalogFactory;

/**
 * Creates a SqlFunctionDictionary with all the SqlFunctions from production.
 */
public final class FunctionDictionaryFactory {
  public static FunctionDictionary create() {
    return create(Collections.emptyList());
  }

  public static FunctionDictionary create(List<SqlOperator> sqlFunctions) {
    SqlOperatorTable operatorTable = OperatorTableFactory.create(sqlFunctions);
    return fromOperatorTable(operatorTable);
  }

  public static FunctionDictionary createWithProductionFunctions(List<SqlOperator> sqlFunctions) {
    SqlOperatorTable operatorTable = OperatorTableFactory.createWithProductionFunctions(sqlFunctions);
    return fromOperatorTable(operatorTable);
  }

  private static FunctionDictionary fromOperatorTable(SqlOperatorTable sqlOperatorTable) {
    SqlValidatorAndScopeFactory.Result result = SqlValidatorAndScopeFactory.create(
      sqlOperatorTable,
      MockCatalogFactory.createFromNode(Metadata.DEFAULT));

    return FunctionDictionary.create(
      sqlOperatorTable
        .getOperatorList()
        .stream()
        .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
        .map(sqlOperator -> (SqlFunction) sqlOperator)
        .collect(Collectors.toList()),
      result.getSqlValidator(),
      result.getScope(),
      true);
  }
}
