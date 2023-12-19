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
package com.dremio.exec.planner.normalizer;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;

import com.dremio.exec.catalog.udf.TabularUserDefinedFunctionExpanderRule;
import com.dremio.exec.ops.UserDefinedFunctionExpander;
import com.dremio.exec.planner.ExtractCommonConjunctionInFilterRule;
import com.dremio.exec.planner.ExtractCommonConjunctionInJoinRule;
import com.dremio.exec.planner.HepPlannerRunner;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.ReduceTrigFunctionsRule;
import com.dremio.exec.planner.normalizer.aggregaterewrite.AggregateFilterToCaseRule;
import com.dremio.exec.planner.normalizer.aggregaterewrite.ApproxPercentileRewriteRule;
import com.dremio.exec.planner.normalizer.aggregaterewrite.CollectToArrayAggRule;
import com.dremio.exec.planner.normalizer.aggregaterewrite.ConvertCountDistinctToHll;
import com.dremio.exec.planner.normalizer.aggregaterewrite.MedianRewriteRule;
import com.dremio.exec.planner.normalizer.aggregaterewrite.PercentileFunctionsRewriteRule;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.rules.DremioCoreRules;
import com.dremio.exec.planner.sql.convertlet.FunctionConverterRule;
import com.dremio.exec.planner.sql.evaluator.FunctionEvaluatorRule;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.ContextInformation;

public class NormalizerRuleSets {

  private final PlannerSettings plannerSettings;
  private final OptionResolver optionResolver;
  private final UserDefinedFunctionExpander userDefinedFunctionExpander;
  private final ContextInformation contextInformation;

  public NormalizerRuleSets(
    PlannerSettings plannerSettings,
    OptionResolver optionResolver,
    UserDefinedFunctionExpander userDefinedFunctionExpander, ContextInformation contextInformation) {
    this.plannerSettings = plannerSettings;
    this.optionResolver = optionResolver;
    this.userDefinedFunctionExpander = userDefinedFunctionExpander;
    this.contextInformation = contextInformation;
  }

  public HepPlannerRunner.HepPlannerRunnerConfig createEntityExpansion() {
    // These rules need to be clubbed together because they all can introduce a RexSubquery that needs to be expanded
    // For example a Tabular UDF with a RexSubquery that has a scalarUDF that has a Tabular UDF
    // We have to use a HepPlanner due to the circular nature of the rules.
    return new HepPlannerRunner.HepPlannerRunnerConfig()
      .setPlannerPhase(PlannerPhase.ENTITY_EXPANSION)
      .getHepMatchOrder(HepMatchOrder.ARBITRARY)
      .setRuleSet(new DremioRuleSetBuilder(optionResolver)
        .add(new TabularUserDefinedFunctionExpanderRule(userDefinedFunctionExpander))
        .add(new FunctionConverterRule(userDefinedFunctionExpander))
        // This one needs to be here, since we might want to run it before the RexSubquery gets expanded
        // to avoid having a dangling JOIN that doesn't get used.
        .add(new FunctionEvaluatorRule(contextInformation))
        // RexSubquery To Correlate Rules
        // These rules are needed since RelOptRules can't operate on the RelNode inside a RexSubquery
        .add(DremioCoreRules.CONVERT_FILTER_SUB_QUERY_TO_CORRELATE)
        .add(DremioCoreRules.CONVERT_JOIN_SUB_QUERY_TO_CORRELATE)
        .add(DremioCoreRules.CONVERT_PROJECT_SUB_QUERY_TO_CORRELATE)
        .build());
  }

  public HepPlannerRunner.HepPlannerRunnerConfig createOperatorExpansion() {
    return new HepPlannerRunner.HepPlannerRunnerConfig()
      .setPlannerPhase(PlannerPhase.OPERATOR_EXPANSION)
      .getHepMatchOrder(HepMatchOrder.ARBITRARY)
      .setRuleSet(new DremioRuleSetBuilder(optionResolver)
        .add(DremioCoreRules.CALC_REDUCE_EXPRESSIONS_CALCITE_RULE)
        .add(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
        .add(DremioCoreRules.REDUCE_FUNCTIONS_FOR_GROUP_SETS)
        .add(DremioCoreRules.GROUP_SET_TO_CROSS_JOIN_CASE_STATEMENT_RULE)
        .add(DremioCoreRules.REWRITE_PROJECT_TO_FLATTEN_RULE)
        .build());
  }

  public HepPlannerRunner.HepPlannerRunnerConfig createAggregateRewrite() {
    return new HepPlannerRunner.HepPlannerRunnerConfig()
      .setPlannerPhase(PlannerPhase.AGGREGATE_REWRITE)
      .getHepMatchOrder(HepMatchOrder.ARBITRARY)
      .setRuleSet(new DremioRuleSetBuilder(optionResolver)
        .add(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES, plannerSettings.isDistinctAggWithGroupingSetsEnabled())
        .add(ConvertCountDistinctToHll.INSTANCE)
        .add(AggregateFilterToCaseRule.INSTANCE)
        .add(MedianRewriteRule.INSTANCE)
        .add(PercentileFunctionsRewriteRule.INSTANCE)
        .add(ApproxPercentileRewriteRule.INSTANCE)
        .add(CollectToArrayAggRule.INSTANCE)
        .build());
  }

  public HepPlannerRunner.HepPlannerRunnerConfig createReduceExpression() {
    boolean isConstantFoldingEnabled = plannerSettings.isConstantFoldingEnabled();
    return new HepPlannerRunner.HepPlannerRunnerConfig()
      .setPlannerPhase(PlannerPhase.REDUCE_EXPRESSIONS)
      .getHepMatchOrder(HepMatchOrder.ARBITRARY)
      .setRuleSet(new DremioRuleSetBuilder(optionResolver)
        .add(ReduceTrigFunctionsRule.INSTANCE, PlannerSettings.REDUCE_ALGEBRAIC_EXPRESSIONS)
        .add(ExtractCommonConjunctionInFilterRule.INSTANCE, PlannerSettings.FILTER_EXTRACT_CONJUNCTIONS)
        .add(ExtractCommonConjunctionInJoinRule.INSTANCE, PlannerSettings.FILTER_EXTRACT_CONJUNCTIONS)
        .add(DremioCoreRules.PROJECT_PULLUP_CONST_TYPE_CAST_RULE, plannerSettings.isProjectPullUpConstTypeCastEnabled())
        .add(DremioCoreRules.JOIN_REDUCE_EXPRESSIONS_CALCITE_RULE,
          isConstantFoldingEnabled && plannerSettings.isReduceJoinExpressionsEnabled())
        .add(DremioCoreRules.PROJECT_REDUCE_EXPRESSIONS_CALCITE_RULE,
          isConstantFoldingEnabled && plannerSettings.isReduceProjectExpressionsEnabled())
        .add(DremioCoreRules.FILTER_REDUCE_EXPRESSIONS_CALCITE_RULE,
          isConstantFoldingEnabled && plannerSettings.isReduceFilterExpressionsEnabled())
        .add(DremioCoreRules.CALC_REDUCE_EXPRESSIONS_CALCITE_RULE,
          isConstantFoldingEnabled && plannerSettings.isReduceCalcExpressionsEnabled())
        .build());
  }
}
