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

import com.dremio.exec.catalog.udf.DremioScalarUserDefinedFunction;
import com.dremio.exec.catalog.udf.DremioTabularUserDefinedFunction;
import com.dremio.exec.catalog.udf.DremioUserDefinedFunction;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.store.sys.udf.FunctionOperatorTable;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

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

  private RexNode expandScalarImplementation(
      DremioScalarUserDefinedFunction dremioScalarUserDefinedFunction) {
    RelNode functionPlan =
        parseAndValidate(dremioScalarUserDefinedFunction, parse(dremioScalarUserDefinedFunction));
    // We always convert scalar UDFs to SQL queries / plans
    // One quirk to this is that we only allow for a single (hence scalar) expression
    // But the plan returns a whole table (but with only 1 column ever)
    // and only 1 row (we check for this at creation time).

    // We have two scenarios:

    // 2) Queries With From Clause: SELECT AGG_FUNCTION() FROM blah WHERE blah2
    // For these other queries we can convert the whole plan into a RexNode
    // by wrapping it in a RexSubQuery.
    // The expansion and decorrelation logic will handle this node.
    RexNode subquery = RexSubQuery.scalar(functionPlan, null);

    // 1) Queries Without From Clause: SELECT a * b
    // The parser represents these this query as:
    // LogicalProject(EXPR$0=[*(a, b)])
    //  LogicalValues(tuples=[[{ 0 }]])
    // Basically it's a project with the single expression on top of a VALUES with single row.
    // We want to match on that and return just the "a * b" RexNode.
    if (!(functionPlan instanceof Project)) {
      return subquery;
    }

    Project project = (Project) functionPlan;
    if (!(project.getInput() instanceof Values)) {
      return subquery;
    }

    Values values = (Values) project.getInput();
    if (values.getTuples().size() != 1) {
      return subquery;
    }

    if (values.getTuples().get(0).size() != 1) {
      return subquery;
    }

    RexNode expr = project.getProjects().get(0);
    if (IllegalScalarExpression.isIllegalScalarExpression(expr)) {
      return subquery;
    }

    return expr;
  }

  @Override
  public RelNode expandTabularFunction(
      DremioTabularUserDefinedFunction tabularUserDefinedFunction) {
    RelNode cachedValue = tabularCache.get(tabularUserDefinedFunction);
    if (cachedValue != null) {
      return cachedValue;
    }

    RelNode tabular = expandTabularFunctionImplementation(tabularUserDefinedFunction);
    tabularCache.put(tabularUserDefinedFunction, tabular);
    return tabular;
  }

  private RelNode expandTabularFunctionImplementation(
      DremioTabularUserDefinedFunction tabularUserDefinedFunction) {
    return parseAndValidate(
        tabularUserDefinedFunction,
        sqlConverter.parse(tabularUserDefinedFunction.getFunctionSql()));
  }

  private RelNode parseAndValidate(
      DremioUserDefinedFunction dremioUserDefinedFunction, SqlNode functionExpression) {
    // TODO: Use the cached function plan to avoid this reparsing logic
    return sqlConverter
        .getUserQuerySqlValidatorAndToRelContextBuilderFactory()
        .builder()
        .withSchemaPath(ImmutableList.of())
        .withUser(dremioUserDefinedFunction.getOwner())
        .withContextualSqlOperatorTable(
            new FunctionOperatorTable(
                dremioUserDefinedFunction.getName(), dremioUserDefinedFunction.getParameters()))
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

  private static final class IllegalScalarExpression extends RexVisitorImpl<Boolean> {
    private static final IllegalScalarExpression INSTANCE = new IllegalScalarExpression();

    private IllegalScalarExpression() {
      // Call the parent constructor with a flag indicating that the visitor should go deep.
      super(true);
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
      // We could encounter a project expr like:
      // PROJECT parse_date($0)
      //   VALUES ("2010 05 02")
      // And we don't want to return the refinputref
      return true;
    }

    public static boolean isIllegalScalarExpression(RexNode expr) {
      Boolean value = expr.accept(INSTANCE);
      if (value == null) {
        return false;
      }

      return value;
    }
  }
}
