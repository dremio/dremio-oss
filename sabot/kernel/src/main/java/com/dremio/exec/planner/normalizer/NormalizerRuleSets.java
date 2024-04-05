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

import static com.dremio.exec.planner.physical.PlannerSettings.ENABLE_PROJECT_REDUCE_CONST_TYPE_CAST;
import static com.dremio.exec.planner.physical.PlannerSettings.ENABLE_REDUCE_FILTER;
import static com.dremio.exec.planner.physical.PlannerSettings.ENABLE_REDUCE_JOIN;
import static com.dremio.exec.planner.physical.PlannerSettings.ENABLE_REDUCE_PROJECT;
import static com.dremio.exec.planner.rules.DremioCoreRules.CONVERT_FILTER_SUB_QUERY_TO_CORRELATE;
import static com.dremio.exec.planner.rules.DremioCoreRules.CONVERT_JOIN_SUB_QUERY_TO_CORRELATE;
import static com.dremio.exec.planner.rules.DremioCoreRules.CONVERT_PROJECT_SUB_QUERY_TO_CORRELATE;
import static com.dremio.exec.planner.rules.DremioCoreRules.FILTER_REDUCE_EXPRESSIONS_CALCITE_RULE;
import static com.dremio.exec.planner.rules.DremioCoreRules.FILTER_REDUCE_EXPRESSIONS_RULE;
import static com.dremio.exec.planner.rules.DremioCoreRules.GROUP_SET_TO_CROSS_JOIN_CASE_STATEMENT_RULE;
import static com.dremio.exec.planner.rules.DremioCoreRules.JOIN_REDUCE_EXPRESSIONS_CALCITE_RULE;
import static com.dremio.exec.planner.rules.DremioCoreRules.JOIN_REDUCE_EXPRESSIONS_RULE;
import static com.dremio.exec.planner.rules.DremioCoreRules.PROJECT_REDUCE_CONST_TYPE_CAST_RULE;
import static com.dremio.exec.planner.rules.DremioCoreRules.PROJECT_REDUCE_EXPRESSIONS_CALCITE_RULE;
import static com.dremio.exec.planner.rules.DremioCoreRules.PROJECT_REDUCE_EXPRESSIONS_RULE;
import static com.dremio.exec.planner.rules.DremioCoreRules.REDUCE_FUNCTIONS_FOR_GROUP_SETS;
import static com.dremio.exec.planner.rules.DremioCoreRules.REWRITE_PROJECT_TO_FLATTEN_RULE;

import com.dremio.exec.catalog.udf.TabularUserDefinedFunctionExpanderRule;
import com.dremio.exec.ops.UserDefinedFunctionExpander;
import com.dremio.exec.planner.HepPlannerRunner;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.normalizer.aggregaterewrite.AggregateCallRewriteRule;
import com.dremio.exec.planner.normalizer.aggregaterewrite.ArrayAggExpandDistinctAggregateRule;
import com.dremio.exec.planner.normalizer.aggregaterewrite.CollectToArrayAggRule;
import com.dremio.exec.planner.normalizer.aggregaterewrite.PercentileFunctionsRewriteRule;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.convertlet.FunctionConverterRule;
import com.dremio.exec.planner.sql.evaluator.FunctionEvaluatorRule;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.ContextInformation;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;

public class NormalizerRuleSets {

  private final OptionResolver optionResolver;
  private final UserDefinedFunctionExpander userDefinedFunctionExpander;
  private final ContextInformation contextInformation;

  public NormalizerRuleSets(
      OptionResolver optionResolver,
      UserDefinedFunctionExpander userDefinedFunctionExpander,
      ContextInformation contextInformation) {
    this.optionResolver = optionResolver;
    this.userDefinedFunctionExpander = userDefinedFunctionExpander;
    this.contextInformation = contextInformation;
  }

  public HepPlannerRunner.HepPlannerRunnerConfig createEntityExpansion() {
    // These rules need to be clubbed together because they all can introduce a RexSubquery that
    // needs to be expanded
    // For example a Tabular UDF with a RexSubquery that has a scalarUDF that has a Tabular UDF
    // We have to use a HepPlanner due to the circular nature of the rules.
    return new HepPlannerRunner.HepPlannerRunnerConfig()
        .setPlannerPhase(PlannerPhase.ENTITY_EXPANSION)
        .getHepMatchOrder(HepMatchOrder.ARBITRARY)
        .setRuleSet(
            new DremioRuleSetBuilder(optionResolver)
                .add(new TabularUserDefinedFunctionExpanderRule(userDefinedFunctionExpander))
                .add(new FunctionConverterRule(optionResolver, userDefinedFunctionExpander))
                // RexSubquery To Correlate Rules
                // These rules are needed since RelOptRules can't operate on the RelNode inside a
                // RexSubquery
                .add(CONVERT_FILTER_SUB_QUERY_TO_CORRELATE)
                .add(CONVERT_JOIN_SUB_QUERY_TO_CORRELATE)
                .add(CONVERT_PROJECT_SUB_QUERY_TO_CORRELATE)
                .add(UnionCastRule.INSTANCE)
                .build());
  }

  public HepPlannerRunner.HepPlannerRunnerConfig createOperatorExpansion() {
    return new HepPlannerRunner.HepPlannerRunnerConfig()
        .setPlannerPhase(PlannerPhase.OPERATOR_EXPANSION)
        .getHepMatchOrder(HepMatchOrder.ARBITRARY)
        .setRuleSet(
            new DremioRuleSetBuilder(optionResolver)
                .add(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
                .add(REDUCE_FUNCTIONS_FOR_GROUP_SETS)
                .add(GROUP_SET_TO_CROSS_JOIN_CASE_STATEMENT_RULE)
                .add(REWRITE_PROJECT_TO_FLATTEN_RULE)
                .build());
  }

  public HepPlannerRunner.HepPlannerRunnerConfig createAggregateRewrite() {
    return new HepPlannerRunner.HepPlannerRunnerConfig()
        .setPlannerPhase(PlannerPhase.AGGREGATE_REWRITE)
        .getHepMatchOrder(HepMatchOrder.ARBITRARY)
        .setRuleSet(
            new DremioRuleSetBuilder(optionResolver)
                .add(AggregateCallRewriteRule.INSTANCE)
                .add(
                    CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
                    PlannerSettings.ENABLE_DISTINCT_AGG_WITH_GROUPING_SETS)
                .add(PercentileFunctionsRewriteRule.INSTANCE)
                .add(CollectToArrayAggRule.INSTANCE)
                .add(ArrayAggExpandDistinctAggregateRule.INSTANCE)
                .build());
  }

  public HepPlannerRunner.HepPlannerRunnerConfig createReduceExpression() {
    final boolean useLegacyReduceExpression =
        optionResolver.getOption(PlannerSettings.USE_LEGACY_REDUCE_EXPRESSIONS);
    boolean isConstantFoldingEnabled = optionResolver.getOption(PlannerSettings.CONSTANT_FOLDING);
    final RuleSet ruleSet;
    if (isConstantFoldingEnabled && useLegacyReduceExpression) {
      ruleSet =
          new DremioRuleSetBuilder(optionResolver)
              .add(new FunctionEvaluatorRule(contextInformation))
              .add(PROJECT_REDUCE_CONST_TYPE_CAST_RULE, ENABLE_PROJECT_REDUCE_CONST_TYPE_CAST)
              .add(JOIN_REDUCE_EXPRESSIONS_CALCITE_RULE, ENABLE_REDUCE_JOIN)
              .add(PROJECT_REDUCE_EXPRESSIONS_CALCITE_RULE, ENABLE_REDUCE_PROJECT)
              .add(FILTER_REDUCE_EXPRESSIONS_CALCITE_RULE, ENABLE_REDUCE_FILTER)
              .build();
    } else if (isConstantFoldingEnabled) {
      ruleSet =
          new DremioRuleSetBuilder(optionResolver)
              .add(new FunctionEvaluatorRule(contextInformation))
              .add(PROJECT_REDUCE_CONST_TYPE_CAST_RULE, ENABLE_PROJECT_REDUCE_CONST_TYPE_CAST)
              .add(JOIN_REDUCE_EXPRESSIONS_RULE, ENABLE_REDUCE_JOIN)
              .add(PROJECT_REDUCE_EXPRESSIONS_RULE, ENABLE_REDUCE_PROJECT)
              .add(FILTER_REDUCE_EXPRESSIONS_RULE, ENABLE_REDUCE_FILTER)
              .build();
    } else {
      ruleSet =
          new DremioRuleSetBuilder(optionResolver)
              .add(new FunctionEvaluatorRule(contextInformation))
              .add(PROJECT_REDUCE_CONST_TYPE_CAST_RULE, ENABLE_PROJECT_REDUCE_CONST_TYPE_CAST)
              .build();
    }
    return new HepPlannerRunner.HepPlannerRunnerConfig()
        .setPlannerPhase(PlannerPhase.REDUCE_EXPRESSIONS)
        .getHepMatchOrder(HepMatchOrder.ARBITRARY)
        .setRuleSet(ruleSet);
  }
}
