/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule.CalcReduceExpressionsRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule.FilterReduceExpressionsRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule.ProjectReduceExpressionsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.AggregateRule;
import com.dremio.exec.planner.logical.Conditions;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.FilterFlattenTransposeRule;
import com.dremio.exec.planner.logical.FilterJoinRulesUtil;
import com.dremio.exec.planner.logical.FilterMergeCrule;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.FilterRule;
import com.dremio.exec.planner.logical.FlattenRule;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.JoinRule;
import com.dremio.exec.planner.logical.LimitRule;
import com.dremio.exec.planner.logical.MergeProjectForFlattenRule;
import com.dremio.exec.planner.logical.MergeProjectRule;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.ProjectRule;
import com.dremio.exec.planner.logical.PushFilterPastProjectRule;
import com.dremio.exec.planner.logical.PushFiltersProjectPastFlattenRule;
import com.dremio.exec.planner.logical.PushProjectForFlattenIntoScanRule;
import com.dremio.exec.planner.logical.PushProjectForFlattenPastProjectRule;
import com.dremio.exec.planner.logical.PushProjectIntoScanRule;
import com.dremio.exec.planner.logical.PushProjectPastFlattenRule;
import com.dremio.exec.planner.logical.PushProjectPastJoinRule;
import com.dremio.exec.planner.logical.RewriteProjectToFlattenRule;
import com.dremio.exec.planner.logical.SampleRule;
import com.dremio.exec.planner.logical.SortRule;
import com.dremio.exec.planner.logical.UnionAllRule;
import com.dremio.exec.planner.logical.ValuesRule;
import com.dremio.exec.planner.logical.WindowRule;
import com.dremio.exec.planner.physical.EmptyPrule;
import com.dremio.exec.planner.physical.FilterPrule;
import com.dremio.exec.planner.physical.FlattenPrule;
import com.dremio.exec.planner.physical.HashAggPrule;
import com.dremio.exec.planner.physical.HashJoinPrule;
import com.dremio.exec.planner.physical.LimitPrule;
import com.dremio.exec.planner.physical.LimitUnionExchangeTransposeRule;
import com.dremio.exec.planner.physical.MergeJoinPrule;
import com.dremio.exec.planner.physical.NestedLoopJoinPrule;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.ProjectPrule;
import com.dremio.exec.planner.physical.PushLimitToTopN;
import com.dremio.exec.planner.physical.SamplePrule;
import com.dremio.exec.planner.physical.SampleToLimitPrule;
import com.dremio.exec.planner.physical.ScreenPrule;
import com.dremio.exec.planner.physical.SortConvertPrule;
import com.dremio.exec.planner.physical.SortPrule;
import com.dremio.exec.planner.physical.StreamAggPrule;
import com.dremio.exec.planner.physical.UnionAllPrule;
import com.dremio.exec.planner.physical.ValuesPrule;
import com.dremio.exec.planner.physical.WindowPrule;
import com.dremio.exec.planner.physical.WriterPrule;
import com.google.common.collect.ImmutableSet;

public enum PlannerPhase {

  WINDOW_REWRITE("Window Function Rewrites") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(
          CALC_REDUCE_EXPRESSIONS_CALCITE_RULE,
          ProjectToWindowRule.PROJECT
          );
    }
  },

  JDBC_PUSHDOWN("JDBC Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(AggregateReduceFunctionsRule.NO_REDUCE_SUM);
          // Query 13 of TPCH pushdown fails:  There is a join condition with an expression (not like '%%somestring%%').
          // This join condition is turned into Filter(InputRef $X)-Project(expression(InputRef $Y not like '%%somestring%%').
          // Since Oracle does not support boolean expressions in select list, we do not allow a project with boolean
          // in select list to be pushed down.  But really, we should not need the project, and just translate the
          // join condition as Filter(InputRef $Y not like '%%somestring%%') and do without the project.  In this case,
          // we can pushdown the join condition since the boolean expression is a filter condition and Oracle allows this.
          // Below two rules were enabled to allow this pushdown, but it is still work in progress since adding these
          // two rules can interfere with subsequent planning phases, especially WINDOW_REWRITE.
          // JOIN_CONDITION_PUSH_CALCITE_RULE,
          // PushFilterPastProjectRule.CALCITE_INSTANCE,
    }
  },


  // fake for reporting purposes.
  FIELD_TRIMMING("Field Trimming") {

    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      throw new UnsupportedOperationException();
    }
  },

  FLATTEN_PUSHDOWN("Flatten Function Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(
          PushFiltersProjectPastFlattenRule.INSTANCE,
          PushProjectPastFlattenRule.INSTANCE,
          PushProjectForFlattenIntoScanRule.INSTANCE,
          PushProjectForFlattenPastProjectRule.INSTANCE,
          MergeProjectForFlattenRule.INSTANCE,
          PUSH_PROJECT_PAST_FILTER_INSTANCE,
          PushFilterPastProjectRule.INSTANCE,
          PushProjectPastJoinRule.LOGICAL_INSTANCE
      );
    }
  },

  PRE_LOGICAL("Pre-Logical Filter Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(
          PushFilterPastProjectRule.CALCITE_NO_CHILD_CHECK,

          // Add support for WHERE style joins.
          FILTER_INTO_JOIN_CALCITE_RULE,
          JOIN_CONDITION_PUSH_CALCITE_RULE,
          JOIN_PUSH_EXPRESSIONS_RULE,
          // End support for WHERE style joins.

          FILTER_SET_OP_TRANSPOSE_CALCITE_RULE,
          FILTER_AGGREGATE_TRANSPOSE_CALCITE_RULE,
          FILTER_MERGE_CALCITE_RULE,

          PushProjectPastJoinRule.CALCITE_INSTANCE,
          ProjectWindowTransposeRule.INSTANCE,
          ProjectSetOpTransposeRule.INSTANCE,
          MergeProjectRule.CALCITE_INSTANCE

          // This can't run here because even though it is heuristic, it causes acceleration matches to fail.
          // PushProjectIntoScanRule.INSTANCE
          );
    }
  },

  POST_SUBSTITUTION("Post-substitution normalization") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return PRE_LOGICAL.getRules(context);
    }
  },

  JOIN_PLANNING("LOPT Join Planning") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(
          JOIN_TO_MULTIJOIN_RULE,
          LOPT_OPTIMIZE_JOIN_RULE
          //ProjectRemoveRule.INSTANCE)
      );
    }
  },

  REDUCE_EXPRESSIONS("Reduce Expressions") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return PlannerPhase.getEnabledReduceExpressionsRules(context);
    }
  },

  LOGICAL("Logical Planning", true) {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {

      List<RelOptRule> moreRules = new ArrayList<>();
      if(context.getPlannerSettings().isTransitiveJoinEnabled()) {
        moreRules.add(new JoinPushTransitivePredicatesRule(JoinRel.class, DremioRelFactories.LOGICAL_BUILDER));
      }

      if(context.getPlannerSettings().isTransposeProjectFilterLogicalEnabled()) {
        moreRules.add(PUSH_PROJECT_PAST_FILTER_CALCITE_RULE);
      }

      if(context.getPlannerSettings().isFilterFlattenTransposeEnabled()){
        moreRules.add(FilterFlattenTransposeRule.INSTANCE);
      }

      if(context.getPlannerSettings().isProjectLogicalCleanupEnabled()) {
        moreRules.add(MergeProjectRule.CALCITE_INSTANCE);
        moreRules.add(ProjectRemoveRule.INSTANCE);

      }
      if(moreRules.isEmpty()) {
        return LOGICAL_RULE_SET;
      }

      return PlannerPhase.mergedRuleSets(LOGICAL_RULE_SET, RuleSets.ofList(moreRules));
    }

    @Override
    public boolean forceVerbose() {
      return true;
    }
  },

  PHYSICAL("Physical Planning") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return PlannerPhase.getPhysicalRules(context);
    }
  };

  private static final Logger logger = LoggerFactory.getLogger(PlannerPhase.class);

  public final String description;
  public final boolean useMaterializations;

  PlannerPhase(String description) {
    this(description, false);
  }

  PlannerPhase(String description, boolean useMaterializations) {
    this.description = description;
    this.useMaterializations = useMaterializations;
  }

  public abstract RuleSet getRules(OptimizerRulesContext context);

  public boolean forceVerbose() {
    return false;
  }

  // START ---------------------------------------------
  // Calcite's default RelBuilder (RelFactories.LOGICAL_BUILDER) uses Values for an empty expression; see
  // tools.RelBuilder#empty. But Dremio does not support Values for the purpose of empty expressions. Instead,
  // Dremio uses LIMIT(0, 0); see logical.RelBuilder#empty. These rules use this alternative builder.

  /**
   * Singleton rule that reduces constants inside a {@link LogicalFilter}.
   */
  public static final ReduceExpressionsRule FILTER_REDUCE_EXPRESSIONS_CALCITE_RULE =
    new FilterReduceExpressionsRule(LogicalFilter.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Singleton rule that reduces constants inside a {@link LogicalProject}.
   */
  public static final ReduceExpressionsRule PROJECT_REDUCE_EXPRESSIONS_CALCITE_RULE =
    new ProjectReduceExpressionsRule(LogicalProject.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Singleton rule that reduces constants inside a {@link LogicalCalc}.
   */
  public static final ReduceExpressionsRule CALC_REDUCE_EXPRESSIONS_CALCITE_RULE =
    new CalcReduceExpressionsRule(LogicalCalc.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Planner rule that combines two {@link Filter}s.
   */
  static final FilterMergeCrule FILTER_MERGE_CALCITE_RULE = new FilterMergeCrule(LogicalFilter.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);
  static final FilterMergeCrule FILTER_MERGE_DRULE = new FilterMergeCrule(FilterRel.class, DremioRelFactories.LOGICAL_BUILDER);

  /**
   * Planner rule that pushes a {@link Filter} past a
   * {@link org.apache.calcite.rel.core.SetOp}.
   */
  static final FilterSetOpTransposeRule FILTER_SET_OP_TRANSPOSE_CALCITE_RULE =
    new FilterSetOpTransposeRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Planner rule that pushes predicates from a Filter into the Join below.
   */

  public static final FilterJoinRule FILTER_INTO_JOIN_CALCITE_RULE = new LogicalFilterJoinRule();

  private static final class LogicalFilterJoinRule extends FilterJoinRule {
    private LogicalFilterJoinRule() {
      super(RelOptRule.operand(LogicalFilter.class, RelOptRule.operand(LogicalJoin.class, RelOptRule.any())),
          "FilterJoinRule:filter", true, DremioRelFactories.CALCITE_LOGICAL_BUILDER,
          FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      Join join = call.rel(1);
      perform(call, filter, join);
    }

  }

  /**
   * Planner rule that pushes predicates in a Join into the inputs to the Join.
   */
  public static final FilterJoinRule JOIN_CONDITION_PUSH_CALCITE_RULE = new JoinConditionPushRule();

  private static class JoinConditionPushRule extends FilterJoinRule {
    public JoinConditionPushRule() {
      super(RelOptRule.operand(LogicalJoin.class, RelOptRule.any()),
          "FilterJoinRule:no-filter", true, DremioRelFactories.CALCITE_LOGICAL_BUILDER,
          FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Join join = call.rel(0);
      perform(call, null, join);
    }
  }

  private static final JoinPushExpressionsRule JOIN_PUSH_EXPRESSIONS_RULE = new JoinPushExpressionsRule(LogicalJoin.class, RelFactories.LOGICAL_BUILDER);


  /**
   * Planner rule that pushes a {@link LogicalProject} past a {@link LogicalFilter}.
   */
  public static final ProjectFilterTransposeRule PUSH_PROJECT_PAST_FILTER_CALCITE_RULE =
    new ProjectFilterTransposeRule(LogicalProject.class, LogicalFilter.class,
      DremioRelFactories.CALCITE_LOGICAL_BUILDER, Conditions.PRESERVE_ITEM_CASE);


  /**
   * Planner rule that pushes a {@link org.apache.calcite.rel.core.Filter}
   * past a {@link org.apache.calcite.rel.core.Aggregate}.
   */
  static final FilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE_CALCITE_RULE = new FilterAggregateTransposeRule(LogicalFilter.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER, LogicalAggregate.class);

  static final FilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE_DRULE = new FilterAggregateTransposeRule(FilterRel.class, DremioRelFactories.LOGICAL_BUILDER, AggregateRel.class);

  /*
   * Planner rules that pushes a {@link LogicalFilter} past a {@link LogicalProject}.
   *
   * See {@link PushFilterPastProjectRule#CALCITE_INSTANCE}.
   */

  /*
   * Planner rule that pushes a {@link org.apache.calcite.rel.core.Project}
   * past a {@link org.apache.calcite.rel.core.Join} by splitting the projection into a projection
   * on top of each child of the join.
   *
   * See {@link PushProjectPastJoinRule#CALCITE_INSTANCE}.
   */

  // END ---------------------------------------------

  static final RelOptRule JOIN_TO_MULTIJOIN_RULE = new JoinToMultiJoinRule(JoinRel.class);
  static final RelOptRule LOPT_OPTIMIZE_JOIN_RULE = new LoptOptimizeJoinRule(DremioRelFactories.LOGICAL_BUILDER, false);

  final static RelOptRule PUSH_PROJECT_PAST_FILTER_INSTANCE = new ProjectFilterTransposeRule(
    ProjectRel.class,
    FilterRel.class,
    DremioRelFactories.LOGICAL_PROPAGATE_BUILDER, Conditions.PRESERVE_ITEM_CASE);

  /**
   * Get the list of enabled reduce expression (logical) rules. These rules are enabled using session/system options.
   *
   * @param optimizerRulesContext used to get the list of planner settings, other rules may
   *                              also in the future need to get other query state from this,
   *                              such as the available list of UDFs (as is used by the
   *                              MergeProjectRule)
   * @return list of enabled reduce expression (logical) rules
   */
  static RuleSet getEnabledReduceExpressionsRules(OptimizerRulesContext optimizerRulesContext) {
    final PlannerSettings ps = optimizerRulesContext.getPlannerSettings();

    // This list is used to store rules that can be turned on an off
    // by user facing planning options
    final ImmutableSet.Builder<RelOptRule> userConfigurableRules = ImmutableSet.builder();
    if (ps.isConstantFoldingEnabled()) {
      // TODO - DRILL-2218, DX-2319
      if (ps.isReduceProjectExpressionsEnabled()) {
        userConfigurableRules.add(PROJECT_REDUCE_EXPRESSIONS_CALCITE_RULE);
      }
      if (ps.isReduceFilterExpressionsEnabled()) {
        userConfigurableRules.add(FILTER_REDUCE_EXPRESSIONS_CALCITE_RULE);
      }
      if (ps.isReduceCalcExpressionsEnabled()) {
        userConfigurableRules.add(CALC_REDUCE_EXPRESSIONS_CALCITE_RULE);
      }
    }
    return RuleSets.ofList(userConfigurableRules.build());
  }

  // These logical rules don't require any context, so singleton instances can be used.
  static final RuleSet LOGICAL_RULE_SET = RuleSets.ofList(ImmutableSet.<RelOptRule>builder()
    .add(
      /*
       * Aggregate optimization rules
       */
      UnionToDistinctRule.INSTANCE,
      AggregateRemoveRule.INSTANCE,
      AggregateReduceFunctionsRule.INSTANCE,
      AggregateExpandDistinctAggregatesRule.JOIN,

      /*
       * Filter push-down related rules.
       * Do these as part of Drel, not Crel optimization (to avoid interplay with project crel rules).
       *
       * Note that these are unlikely to make any changes unless
       */
      PushFilterPastProjectRule.CALCITE_NO_CHILD_CHECK,

      // Add support for WHERE style joins.
      FILTER_INTO_JOIN_CALCITE_RULE,
      JOIN_CONDITION_PUSH_CALCITE_RULE,
      JOIN_PUSH_EXPRESSIONS_RULE,
      // End support for WHERE style joins.

      FILTER_SET_OP_TRANSPOSE_CALCITE_RULE,
      FILTER_AGGREGATE_TRANSPOSE_CALCITE_RULE,
      FILTER_MERGE_CALCITE_RULE,

      /*
       * Project pushdown rules.
       */
      PushProjectIntoScanRule.INSTANCE,
      MergeProjectRule.LOGICAL_INSTANCE,


      // Disabled as it causes infinite loops with MergeProjectRule, ProjectFilterTranspose (with Expression preservation) and FilterProjectTranspose
      // PushProjectPastJoinRule.CALCITE_INSTANCE,

      // Not used.
      //SortRemoveRule.INSTANCE,

      /*
       * Trait Conversion Rules
       */
      ExpandConversionRule.INSTANCE,

      /*
       Rewrite flatten rules
       */
      RewriteProjectToFlattenRule.INSTANCE,

      /*
       * Crel => Drel
       */
      ProjectRule.INSTANCE,
      FilterRule.INSTANCE,
      WindowRule.INSTANCE,
      AggregateRule.INSTANCE,
      LimitRule.INSTANCE,
      SampleRule.INSTANCE,
      SortRule.INSTANCE,
      JoinRule.INSTANCE,
      UnionAllRule.INSTANCE,
      ValuesRule.INSTANCE,
      FlattenRule.INSTANCE

      ).build());

  static final RuleSet getPhysicalRules(OptimizerRulesContext optimizerRulesContext) {
    final List<RelOptRule> ruleList = new ArrayList<>();
    final PlannerSettings ps = optimizerRulesContext.getPlannerSettings();

    ruleList.add(SortConvertPrule.INSTANCE);
    ruleList.add(SortPrule.INSTANCE);
    ruleList.add(ProjectPrule.INSTANCE);
    ruleList.add(FlattenPrule.INSTANCE);
    ruleList.add(ScreenPrule.INSTANCE);
    ruleList.add(ExpandConversionRule.INSTANCE);
    ruleList.add(FilterPrule.INSTANCE);
    ruleList.add(LimitPrule.INSTANCE);
    ruleList.add(SamplePrule.INSTANCE);
    ruleList.add(SampleToLimitPrule.INSTANCE);
    ruleList.add(WriterPrule.INSTANCE);
    ruleList.add(WindowPrule.INSTANCE);
    ruleList.add(PushLimitToTopN.INSTANCE);
    ruleList.add(LimitUnionExchangeTransposeRule.INSTANCE);
    ruleList.add(UnionAllPrule.INSTANCE);
    ruleList.add(ValuesPrule.INSTANCE);
    ruleList.add(EmptyPrule.INSTANCE);

    if (ps.isHashAggEnabled()) {
      ruleList.add(HashAggPrule.INSTANCE);
    }

    if (ps.isStreamAggEnabled()) {
      ruleList.add(StreamAggPrule.INSTANCE);
    }

    if (ps.isHashJoinEnabled()) {
      ruleList.add(HashJoinPrule.DIST_INSTANCE);

      if(ps.isBroadcastJoinEnabled()){
        ruleList.add(HashJoinPrule.BROADCAST_INSTANCE);
      }
    }

    if (ps.isMergeJoinEnabled()) {
      ruleList.add(MergeJoinPrule.DIST_INSTANCE);

      if(ps.isBroadcastJoinEnabled()){
        ruleList.add(MergeJoinPrule.BROADCAST_INSTANCE);
      }

    }

    // NLJ plans consist of broadcasting the right child, hence we need
    // broadcast join enabled.
    if (ps.isNestedLoopJoinEnabled() && ps.isBroadcastJoinEnabled()) {
      ruleList.add(NestedLoopJoinPrule.INSTANCE);
    }

    return RuleSets.ofList(ImmutableSet.copyOf(ruleList));
  }

  public static RuleSet mergedRuleSets(RuleSet... ruleSets) {
    final ImmutableSet.Builder<RelOptRule> relOptRuleSetBuilder = ImmutableSet.builder();
    for (final RuleSet ruleSet : ruleSets) {
      relOptRuleSetBuilder.addAll(ruleSet);
    }
    return RuleSets.ofList(relOptRuleSetBuilder.build());
  }
}
