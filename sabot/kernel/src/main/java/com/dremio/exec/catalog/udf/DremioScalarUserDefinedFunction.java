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
package com.dremio.exec.catalog.udf;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.exec.store.sys.udf.FunctionOperatorTable;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.google.common.collect.ImmutableList;

public class DremioScalarUserDefinedFunction implements ScalarFunction {
  private final CatalogIdentity owner;
  private final UserDefinedFunction userDefinedFunction;

  public DremioScalarUserDefinedFunction(CatalogIdentity owner, UserDefinedFunction userDefinedFunction) {
    this.owner = owner;
    this.userDefinedFunction = userDefinedFunction;
  }

  public String getFunctionSql() {
    return userDefinedFunction.getFunctionSql();
  }

  @Override public List<FunctionParameter> getParameters() {
    return FunctionParameterImpl.createParameters(userDefinedFunction.getFunctionArgsList());
  }

  public CatalogIdentity getOwner() {
    return owner;
  }

  @Override public RelDataType getReturnType(RelDataTypeFactory relDataTypeFactory) {
    return CalciteArrowHelper.wrap(userDefinedFunction.getReturnType()).toCalciteType(relDataTypeFactory, true);
  }

  public String getName(){
    return userDefinedFunction.getName();
  }

  public RexNode extractExpression(SqlConverter sqlConverter) {
    // TODO: Use the cached function plan to avoid this reparsing logic
    SqlNode functionSqlNode = parse(sqlConverter);
    RelNode functionPlan = SqlValidatorAndToRelContext
      .builder(sqlConverter)
      .withSchemaPath(ImmutableList.of())
      .withUser(owner)
      .withContextualSqlOperatorTable(new FunctionOperatorTable(
        userDefinedFunction.getName(),
        getParameters()))
      .disallowSubqueryExpansion()
      .build()
      .getPlanForFunctionExpression(functionSqlNode);

    if (functionPlan instanceof Project) {
      // We always convert scalar UDFs to SQL queries / plans
      // One quirk to this is that we only allow for a single (hence scalar) expression
      // But the plan returns a whole table (but with only 1 column ever)
      // So we just strip out the only project node.
      Project project = (Project) functionPlan;
      assert project.getProjects().size() == 1;
      return project.getProjects().get(0);
    }

    // We have a scalar subquery
    return RexSubQuery.scalar(functionPlan, null);
  }

  private SqlSelect parse(SqlConverter sqlConverter) {
    // For a scalar udf the body can either be:
    // 1) a + b
    // 2) SELECT a + b
    // In the first scenario the query is not parseable,
    // so we catch the exception and try to make it like the second scenario
    String sqlQueryText = getFunctionSql();
    if (!sqlQueryText.toUpperCase().startsWith("SELECT ")) {
      sqlQueryText = "SELECT " + sqlQueryText;
    }

    SqlNode sqlNode = sqlConverter.parse(sqlQueryText);
    assert sqlNode instanceof SqlSelect;
    SqlSelect sqlSelect = (SqlSelect) sqlNode;
    return sqlSelect;
  }
}
