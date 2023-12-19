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
package com.dremio.exec.ops;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import com.dremio.exec.catalog.udf.DremioScalarUserDefinedFunction;
import com.dremio.exec.catalog.udf.DremioTabularUserDefinedFunction;
import com.dremio.exec.catalog.udf.DremioUserDefinedFunction;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.exec.store.sys.udf.FunctionOperatorTable;
import com.google.common.collect.ImmutableList;

public class UserDefinedFunctionExpanderImpl implements UserDefinedFunctionExpander {

  private final SqlConverter sqlConverter;
  private final Map<DremioScalarUserDefinedFunction, RexNode> scalarCache;
  private final Map<DremioTabularUserDefinedFunction, RelNode> tabularCache;

  public UserDefinedFunctionExpanderImpl(SqlConverter sqlConverter) {
    this.sqlConverter = sqlConverter;
    this.scalarCache = new HashMap<>();
    this.tabularCache = new HashMap<>();
  }

  @Override
  public RexNode expandScalar(DremioScalarUserDefinedFunction dremioScalarUserDefinedFunction) {
    RexNode cachedValue = scalarCache.get(dremioScalarUserDefinedFunction);
    if (cachedValue != null) {
      return cachedValue;
    }

    RexNode scalar = expandScalarImplementation(dremioScalarUserDefinedFunction);
    scalarCache.put(dremioScalarUserDefinedFunction, scalar);
    return scalar;
  }

  private RexNode expandScalarImplementation(DremioScalarUserDefinedFunction dremioScalarUserDefinedFunction) {
    RelNode functionPlan = parseAndValidate(dremioScalarUserDefinedFunction, parse(dremioScalarUserDefinedFunction));

    if (functionPlan instanceof Project) {
      // We always convert scalar UDFs to SQL queries / plans
      // One quirk to this is that we only allow for a single (hence scalar) expression
      // But the plan returns a whole table (but with only 1 column ever)
      // So we just strip out the only project node.
      Project project = (Project) functionPlan;
      assert project.getProjects().size() == 1;
      return project.getProjects().get(0);
    }

    return RexSubQuery.scalar(functionPlan, null);
  }

  @Override
  public RelNode expandTabularFunction(DremioTabularUserDefinedFunction tabularUserDefinedFunction) {
    RelNode cachedValue = tabularCache.get(tabularUserDefinedFunction);
    if (cachedValue != null) {
      return cachedValue;
    }

    RelNode tabular = expandTabularFunctionImplementation(tabularUserDefinedFunction);
    tabularCache.put(tabularUserDefinedFunction, tabular);
    return tabular;
  }

  private RelNode expandTabularFunctionImplementation(DremioTabularUserDefinedFunction tabularUserDefinedFunction) {
    return parseAndValidate(
      tabularUserDefinedFunction,
      sqlConverter.parse(tabularUserDefinedFunction.getFunctionSql()));
  }

  private RelNode parseAndValidate(DremioUserDefinedFunction dremioUserDefinedFunction, SqlNode functionExpression) {
    // TODO: Use the cached function plan to avoid this reparsing logic
    return SqlValidatorAndToRelContext
      .builder(sqlConverter)
      .withSchemaPath(ImmutableList.of())
      .withUser(dremioUserDefinedFunction.getOwner())
      .withContextualSqlOperatorTable(new FunctionOperatorTable(
        dremioUserDefinedFunction.getName(),
        dremioUserDefinedFunction.getParameters()))
      .build()
      .validateAndConvertForExpression(functionExpression);
  }

  private SqlSelect parse(DremioUserDefinedFunction dremioScalarUserDefinedFunction) {
    // For a scalar udf the body can either be:
    // 1) a + b
    // 2) SELECT a + b
    // In the first scenario the query is not parseable,
    // so we catch the exception and try to make it like the second scenario
    String sqlQueryText = dremioScalarUserDefinedFunction.getFunctionSql();
    if (!sqlQueryText.toUpperCase().startsWith("SELECT ")) {
      sqlQueryText = "SELECT " + sqlQueryText;
    }

    SqlNode sqlNode = sqlConverter.parse(sqlQueryText);
    assert sqlNode instanceof SqlSelect;
    SqlSelect sqlSelect = (SqlSelect) sqlNode;
    return sqlSelect;
  }
}
