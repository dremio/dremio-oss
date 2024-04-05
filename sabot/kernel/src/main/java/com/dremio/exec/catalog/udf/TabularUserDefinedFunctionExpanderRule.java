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

import com.dremio.exec.ops.UserDefinedFunctionExpander;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

public final class TabularUserDefinedFunctionExpanderRule extends RelRule<RelRule.Config> {
  private final UserDefinedFunctionExpander userDefinedFunctionExpander;

  public TabularUserDefinedFunctionExpanderRule(
      UserDefinedFunctionExpander userDefinedFunctionExpander) {
    super(
        Config.EMPTY
            .withDescription("TabularUserDefinedFunctionExpanderRule")
            .withOperandSupplier(op1 -> op1.operand(TableFunctionScan.class).anyInputs()));
    this.userDefinedFunctionExpander = userDefinedFunctionExpander;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    TableFunctionScan tableFunctionScan = call.rel(0);
    RexCall rexCall = (RexCall) tableFunctionScan.getCall();
    SqlOperator operator = rexCall.getOperator();
    if (!(operator instanceof SqlUserDefinedFunction)) {
      return;
    }

    SqlUserDefinedFunction userDefinedFunction = (SqlUserDefinedFunction) operator;
    Function function = userDefinedFunction.getFunction();
    if (!(function instanceof DremioTabularUserDefinedFunction)) {
      return;
    }

    DremioTabularUserDefinedFunction tabularFunction = (DremioTabularUserDefinedFunction) function;
    RelNode tabularFunctionPlan =
        userDefinedFunctionExpander.expandTabularFunction(tabularFunction);
    tabularFunctionPlan =
        ParameterizedQueryParameterReplacer.replaceParameters(
            tabularFunctionPlan,
            tabularFunction.getParameters(),
            rexCall.operands,
            tableFunctionScan.getCluster().getRexBuilder());
    RelNode castProject =
        MoreRelOptUtil.createCastRel(tabularFunctionPlan, tableFunctionScan.getRowType());
    call.transformTo(castProject);
  }
}
