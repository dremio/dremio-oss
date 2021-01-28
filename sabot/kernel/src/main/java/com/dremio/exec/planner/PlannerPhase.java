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
package com.dremio.exec.planner;

import static org.apache.calcite.plan.RelOptRule.any;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.some;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.MultiJoinOptimizeBushyRule;
import org.apache.calcite.rel.rules.MultiJoinProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PushProjector.ExprCondition;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule.CalcReduceExpressionsRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule.FilterReduceExpressionsRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule.ProjectReduceExpressionsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.expr.fn.hll.ConvertCountDistinctToHll;
import com.dremio.exec.expr.fn.hll.RewriteNdvAsHll;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.AggregateRule;
import com.dremio.exec.planner.logical.CompositeFilterJoinRule;
import com.dremio.exec.planner.logical.Conditions;
import com.dremio.exec.planner.logical.CorrelateRule;
import com.dremio.exec.planner.logical.DremioAggregateReduceFunctionsRule;
import com.dremio.exec.planner.logical.DremioProjectJoinTransposeRule;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.DremioSortMergeRule;
import com.dremio.exec.planner.logical.EmptyRule;
import com.dremio.exec.planner.logical.ExpansionDrule;
import com.dremio.exec.planner.logical.FilterFlattenTransposeRule;
import com.dremio.exec.planner.logical.FilterJoinRulesUtil;
import com.dremio.exec.planner.logical.FilterMergeCrule;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.FilterRule;
import com.dremio.exec.planner.logical.FilterWindowTransposeRule;
import com.dremio.exec.planner.logical.FlattenRule;
import com.dremio.exec.planner.logical.JoinFilterCanonicalizationRule;
import com.dremio.exec.planner.logical.JoinNormalizationRule;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.JoinRule;
import com.dremio.exec.planner.logical.LimitRule;
import com.dremio.exec.planner.logical.MergeProjectForFlattenRule;
import com.dremio.exec.planner.logical.MergeProjectRule;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.ProjectRule;
import com.dremio.exec.planner.logical.PushFilterPastProjectRule;
import com.dremio.exec.planner.logical.PushProjectForFlattenIntoScanRule;
import com.dremio.exec.planner.logical.PushProjectForFlattenPastProjectRule;
import com.dremio.exec.planner.logical.PushProjectIntoScanRule;
import com.dremio.exec.planner.logical.PushProjectPastFlattenRule;
import com.dremio.exec.planner.logical.RemoveEmptyScansRule;
import com.dremio.exec.planner.logical.RewriteProjectToFlattenRule;
import com.dremio.exec.planner.logical.SampleRule;
import com.dremio.exec.planner.logical.SimpleFilterJoinRule;
import com.dremio.exec.planner.logical.SortRule;
import com.dremio.exec.planner.logical.UnionAllRule;
import com.dremio.exec.planner.logical.ValuesRule;
import com.dremio.exec.planner.logical.WindowRule;
import com.dremio.exec.planner.physical.EmptyPrule;
import com.dremio.exec.planner.physical.FilterNLJMergeRule;
import com.dremio.exec.planner.physical.FilterProjectNLJRule;
import com.dremio.exec.planner.physical.FilterPrule;
import com.dremio.exec.planner.physical.FlattenPrule;
import com.dremio.exec.planner.physical.HashAggPrule;
import com.dremio.exec.planner.physical.HashJoinPrule;
import com.dremio.exec.planner.physical.LimitPrule;
import com.dremio.exec.planner.physical.LimitUnionExchangeTransposeRule;
import com.dremio.exec.planner.physical.MergeJoinPrule;
import com.dremio.exec.planner.physical.NestedLoopJoinPrule;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.ProjectNLJMergeRule;
import com.dremio.exec.planner.physical.ProjectPrule;
import com.dremio.exec.planner.physical.PushLimitToTopN;
import com.dremio.exec.planner.physical.SamplePrule;
import com.dremio.exec.planner.physical.SampleToLimitPrule;
import com.dremio.exec.planner.physical.ScreenPrule;
import com.dremio.exec.planner.physical.SimplifyNLJConditionRule;
import com.dremio.exec.planner.physical.SortConvertPrule;
import com.dremio.exec.planner.physical.SortPrule;
import com.dremio.exec.planner.physical.StreamAggPrule;
import com.dremio.exec.planner.physical.UnionAllPrule;
import com.dremio.exec.planner.physical.ValuesPrule;
import com.dremio.exec.planner.physical.WindowPrule;
import com.dremio.exec.planner.physical.WriterPrule;
import com.dremio.exec.planner.tablefunctions.ExternalQueryScanPrule;
import com.dremio.exec.planner.tablefunctions.ExternalQueryScanRule;
import com.google.common.collect.ImmutableList;
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
      final ImmutableList.Builder<RelOptRule> rules = ImmutableList.builder();
      rules.add(AggregateReduceFunctionsRule.NO_REDUCE_SUM);

      if (context.getPlannerSettings()
          .getOptions()
          .getOption(PlannerSettings.JDBC_PUSH_DOWN_PLUS)) {
        rules.add(
            SimpleFilterJoinRule.CALCITE_INSTANCE,
            JOIN_CONDITION_PUSH_CALCITE_RULE,
            PushFilterPastProjectRule.CALCITE_INSTANCE
        );
      }

      return RuleSets.ofList(rules.build());
    }
  },

  // fake for reporting purposes.
  FIELD_TRIMMING("Field Trimming") {

    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      throw new UnsupportedOperationException();
    }
  },

  // fake for reporting purposes.
  TRANSITIVE_PREDICATE_PULLUP("Transitive Predicate Pullup") {

    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      throw new UnsupportedOperationException();
    }
  },

  FLATTEN_PUSHDOWN("Flatten Function Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(
          PushProjectPastFlattenRule.INSTANCE,
          PushProjectForFlattenIntoScanRule.INSTANCE,
          PushProjectForFlattenPastProjectRule.INSTANCE,
          MergeProjectForFlattenRule.INSTANCE,
          PUSH_PROJECT_PAST_FILTER_INSTANCE,
          PushFilterPastProjectRule.INSTANCE,
          PUSH_PROJECT_PAST_JOIN_RULE
      );
    }
  },

  PROJECT_PUSHDOWN("Project Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(
        PushProjectIntoScanRule.INSTANCE
      );
    }
  },

  PRE_LOGICAL("Pre-Logical Filter Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      ImmutableList.Builder<RelOptRule> b = ImmutableList.builder();
      PlannerSettings ps = context.getPlannerSettings();
      b.add(
        ConvertCountDistinctToHll.INSTANCE,
        RewriteNdvAsHll.INSTANCE,

        PushFilterPastProjectRule.CALCITE_NO_CHILD_CHECK,

        JoinFilterCanonicalizationRule.INSTANCE,

        FILTER_SET_OP_TRANSPOSE_CALCITE_RULE,
        FILTER_AGGREGATE_TRANSPOSE_CALCITE_RULE,
        FILTER_MERGE_CALCITE_RULE,

        DremioSortMergeRule.INSTANCE,

        PlannerPhase.PUSH_PROJECT_PAST_JOIN_CALCITE_RULE,
        ProjectWindowTransposeRule.INSTANCE,
        ProjectSetOpTransposeRule.INSTANCE,
        MergeProjectRule.CALCITE_INSTANCE,
        RemoveEmptyScansRule.INSTANCE,
        FilterWindowTransposeRule.INSTANCE
      );

      if (ps.isRelPlanningEnabled()) {
        b.add(LOGICAL_FILTER_CORRELATE_RULE);
      }

      if (ps.isTransitiveFilterPushdownEnabled()) {
        // Add reduce expression rules to reduce any filters after applying transitive rule.
        if (ps.options.getOption(PlannerSettings.REDUCE_ALGEBRAIC_EXPRESSIONS)) {
          b.add(ReduceTrigFunctionsRule.INSTANCE);
        }

        b.add(CompositeFilterJoinRule.NO_TOP_FILTER,
          CompositeFilterJoinRule.TOP_FILTER);

        if (ps.isConstantFoldingEnabled()) {
          if (ps.isTransitiveReduceProjectExpressionsEnabled()) {
            b.add(PROJECT_REDUCE_EXPRESSIONS_CALCITE_RULE);
          }
          if (ps.isTransitiveReduceFilterExpressionsEnabled()) {
            b.add(FILTER_REDUCE_EXPRESSIONS_CALCITE_RULE);
          }
          if (ps.isTransitiveReduceCalcExpressionsEnabled()) {
            b.add(CALC_REDUCE_EXPRESSIONS_CALCITE_RULE);
          }
        }
      } else {
        b.add(FILTER_INTO_JOIN_CALCITE_RULE,
          JOIN_CONDITION_PUSH_CALCITE_RULE,
          JOIN_PUSH_EXPRESSIONS_RULE);
      }

      return RuleSets.ofList(b.build());
    }
  },

  PRE_LOGICAL_TRANSITIVE("Pre-Logical Transitive Filter Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(PRE_LOGICAL.getRules(context));
    }
  },

  POST_SUBSTITUTION("Post-substitution normalization") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return PRE_LOGICAL.getRules(context);
    }
  },

  /**
   * Initial phase of join planning
   */
  JOIN_PLANNING_MULTI_JOIN("Multi-Join analysis") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      // Check if multi-join optimization has been disabled

      return RuleSets.ofList(
          MULTIJOIN_BOTH_PROJECTS_TRANSPOSE_RULE,
          MULTIJOIN_LEFT_PROJECT_TRANSPOSE_RULE,
          MULTIJOIN_RIGHT_PROJECT_TRANSPOSE_RULE,
          JOIN_TO_MULTIJOIN_RULE,
          PROJECT_MULTIJOIN_MERGE_RULE,
          FILTER_MULTIJOIN_MERGE_RULE,
          MergeProjectRule.LOGICAL_INSTANCE,
          PROJECT_REMOVE_DRULE,
          FILTER_MERGE_DRULE
      );
    }
  },

  /**
   * Finalizing phase of join planning
   */
  JOIN_PLANNING_OPTIMIZATION("LOPT Join Planning") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      // add these rules because the MultiJoin produced in Multi-join analysis phase may have expressions like cast,
      // but HashJoinPrule requires simple references
      ImmutableList.Builder<RelOptRule> builder = ImmutableList.<RelOptRule>builder();

      builder
        .add(JOIN_PUSH_EXPRESSIONS_LOGICAL_RULE)
        .add(MergeProjectRule.LOGICAL_INSTANCE);


      // Check if multi-join optimization has been enabled
      if (context.getPlannerSettings().isJoinOptimizationEnabled()) {
        if (context.getPlannerSettings().isExperimentalBushyJoinOptimizerEnabled()) {
          builder.add(MULTI_JOIN_OPTIMIZE_BUSHY_RULE);
        } else {
          builder.add(LOPT_OPTIMIZE_JOIN_RULE);
        }
      } else {
        builder.add(LOPT_UNOPTIMIZE_JOIN_RULE);
      }

      return RuleSets.ofList(builder.add(JoinNormalizationRule.INSTANCE).build());
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

      moreRules.add(ExternalQueryScanRule.INSTANCE);

      return PlannerPhase.mergedRuleSets(LOGICAL_RULE_SET, RuleSets.ofList(moreRules));
    }

    @Override
    public boolean forceVerbose() {
      return true;
    }
  },

  RELATIONAL_PLANNING("Relational Planning") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      final ImmutableList.Builder<RelOptRule> rules = ImmutableList.builder();
      rules.add(DremioAggregateReduceFunctionsRule.NO_REDUCE_SUM);

      if (context.getPlannerSettings()
        .getOptions()
        .getOption(PlannerSettings.JDBC_PUSH_DOWN_PLUS)) {
        rules.add(
          SimpleFilterJoinRule.LOGICAL_INSTANCE,
          JOIN_CONDITION_PUSH_LOGICAL_RULE,
          PushFilterPastProjectRule.INSTANCE
        );
      }

      return RuleSets.ofList(rules.build());
    }
  },

  PHYSICAL("Physical Planning") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return PlannerPhase.getPhysicalRules(context);
    }
  },

  PHYSICAL_HEP("Physical Heuristic Planning") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      ImmutableList.Builder<RelOptRule> builder = ImmutableList.builder();
      builder.add(FilterProjectNLJRule.INSTANCE);
      builder.add(FilterNLJMergeRule.INSTANCE);
      if (context.getPlannerSettings().options.getOption(PlannerSettings.ENABlE_PROJCT_NLJ_MERGE)) {
        builder.add(ProjectNLJMergeRule.INSTANCE);
      }
      if (context.getPlannerSettings().options.getOption(PlannerSettings.NLJ_PUSHDOWN)) {
        builder.add(SimplifyNLJConditionRule.INSTANCE);
      }
      return RuleSets.ofList(builder.build());
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
    new FilterReduceExpressionsRule(LogicalFilter.class,
        ReduceExpressionsRule.DEFAULT_FILTER_OPTIONS.treatDynamicCallsAsNonConstant(false),
        DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Singleton rule that reduces constants inside a {@link LogicalProject}.
   */
  public static final ReduceExpressionsRule PROJECT_REDUCE_EXPRESSIONS_CALCITE_RULE =
    new ProjectReduceExpressionsRule(
        LogicalProject.class,
        ReduceExpressionsRule.DEFAULT_OPTIONS.treatDynamicCallsAsNonConstant(false),
        DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Singleton rule that reduces constants inside a {@link LogicalCalc}.
   */
  public static final ReduceExpressionsRule CALC_REDUCE_EXPRESSIONS_CALCITE_RULE =
    new CalcReduceExpressionsRule(LogicalCalc.class,
        ReduceExpressionsRule.DEFAULT_OPTIONS.treatDynamicCallsAsNonConstant(false),
        DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Planner rule that combines two {@link Filter}s.
   */
  public static final FilterMergeCrule FILTER_MERGE_CALCITE_RULE = new FilterMergeCrule(LogicalFilter.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  private static final FilterMergeCrule FILTER_MERGE_DRULE = new FilterMergeCrule(FilterRel.class, DremioRelFactories.LOGICAL_BUILDER);
  private static final ProjectRemoveRule PROJECT_REMOVE_DRULE = new ProjectRemoveRule(DremioRelFactories.LOGICAL_BUILDER);

  /**
   * Planner rule that pushes a {@link Filter} past a
   * {@link org.apache.calcite.rel.core.SetOp}.
   */
  public static final FilterSetOpTransposeRule FILTER_SET_OP_TRANSPOSE_CALCITE_RULE =
    new FilterSetOpTransposeRule(
        LogicalFilter.class,
        SetOp.class,
        DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Planner rule that pushes predicates from a Filter into the Join below.
   */
  public static final FilterJoinRule FILTER_INTO_JOIN_CALCITE_RULE = new LogicalFilterJoinRule();

  /**
   * Planner rules that push project expressions past join
   */
  public static final RelOptRule PUSH_PROJECT_PAST_JOIN_RULE = new ProjectJoinTransposeRule(
      ProjectRel.class,
      JoinRel.class,
      ExprCondition.TRUE,
      DremioRelFactories.LOGICAL_BUILDER);

  public static final RelOptRule PUSH_PROJECT_PAST_JOIN_CALCITE_RULE = DremioProjectJoinTransposeRule.INSTANCE;

  public static final RelOptRule LOGICAL_FILTER_CORRELATE_RULE = new FilterCorrelateRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER);

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
  public static final FilterJoinRule JOIN_CONDITION_PUSH_CALCITE_RULE = new JoinConditionPushRule(
      RelOptRule.operand(LogicalJoin.class, RelOptRule.any()),
      "FilterJoinRuleCrel:no-filter",
      DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  public static final FilterJoinRule JOIN_CONDITION_PUSH_LOGICAL_RULE = new JoinConditionPushRule(
      RelOptRule.operand(LogicalJoin.class, RelOptRule.any()),
      "FilterJoinRuleDrel:no-filter",
      DremioRelFactories.LOGICAL_BUILDER);

  private static class JoinConditionPushRule extends FilterJoinRule {
    public JoinConditionPushRule() {
      super(RelOptRule.operand(LogicalJoin.class, RelOptRule.any()),
          "FilterJoinRule:no-filter", true, DremioRelFactories.CALCITE_LOGICAL_BUILDER,
          FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM);
    }
    public JoinConditionPushRule(RelOptRuleOperand operand, String id, RelBuilderFactory relBuilderFactory) {
      super(operand, id, true, relBuilderFactory, FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Join join = call.rel(0);
      perform(call, null, join);
    }
  }

  public static final JoinPushExpressionsRule JOIN_PUSH_EXPRESSIONS_RULE = new JoinPushExpressionsRule(LogicalJoin.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);
  private static final JoinPushExpressionsRule JOIN_PUSH_EXPRESSIONS_LOGICAL_RULE = new JoinPushExpressionsRule(JoinRel.class, DremioRelFactories.LOGICAL_BUILDER);


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
  public static final FilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE_CALCITE_RULE = new FilterAggregateTransposeRule(LogicalFilter.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER, LogicalAggregate.class);

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

  private static final RelOptRule MULTIJOIN_BOTH_PROJECTS_TRANSPOSE_RULE =
      new MultiJoinProjectTransposeRule(
          operand(JoinRel.class,
              operand(ProjectRel.class,
                  operand(MultiJoin.class, any())),
              operand(ProjectRel.class,
                  operand(MultiJoin.class, any()))),
          DremioRelFactories.LOGICAL_BUILDER,
          "MultiJoinProjectTransposeRule:TwoProjects");
  private static final RelOptRule MULTIJOIN_LEFT_PROJECT_TRANSPOSE_RULE =
      new MultiJoinProjectTransposeRule(
          operand(JoinRel.class,
              some(
                  operand(ProjectRel.class,
                      operand(MultiJoin.class, any())))),
          DremioRelFactories.LOGICAL_BUILDER,
      "MultiJoinProjectTransposeRule:LeftProject");
  private static final RelOptRule MULTIJOIN_RIGHT_PROJECT_TRANSPOSE_RULE =
      new MultiJoinProjectTransposeRule(
      operand(JoinRel.class,
          operand(RelNode.class, any()),
          operand(ProjectRel.class,
              operand(MultiJoin.class, any()))),
      DremioRelFactories.LOGICAL_BUILDER,
      "MultiJoinProjectTransposeRule:RightProject");

  private static final RelOptRule JOIN_TO_MULTIJOIN_RULE = new JoinToMultiJoinRule(JoinRel.class, DremioRelFactories.LOGICAL_BUILDER);
  private static final RelOptRule PROJECT_MULTIJOIN_MERGE_RULE = new ProjectMultiJoinMergeRule(ProjectRel.class, DremioRelFactories.LOGICAL_BUILDER);
  private static final RelOptRule FILTER_MULTIJOIN_MERGE_RULE = new FilterMultiJoinMergeRule(FilterRel.class, DremioRelFactories.LOGICAL_BUILDER);
  private static final RelOptRule LOPT_OPTIMIZE_JOIN_RULE = new LoptOptimizeJoinRule(DremioRelFactories.LOGICAL_BUILDER, false);
  private static final RelOptRule LOPT_UNOPTIMIZE_JOIN_RULE = new LoptOptimizeJoinRule(DremioRelFactories.LOGICAL_BUILDER, true);
  private static final MultiJoinOptimizeBushyRule MULTI_JOIN_OPTIMIZE_BUSHY_RULE = new MultiJoinOptimizeBushyRule(DremioRelFactories.LOGICAL_BUILDER);

  private static final RelOptRule PUSH_PROJECT_PAST_FILTER_INSTANCE = new ProjectFilterTransposeRule(
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
    final ImmutableList.Builder<RelOptRule> userConfigurableRules = ImmutableList.builder();

    userConfigurableRules.add(ConvertCountDistinctToHll.INSTANCE);
    if (ps.options.getOption(PlannerSettings.REDUCE_ALGEBRAIC_EXPRESSIONS)) {
      userConfigurableRules.add(ReduceTrigFunctionsRule.INSTANCE);
    }

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

      // remove expansion nodes when converting to logical.
      ExpansionDrule.INSTANCE,

      /*
       * Aggregate optimization rules
       */
      UnionToDistinctRule.INSTANCE,
      AggregateRemoveRule.INSTANCE,
      DremioAggregateReduceFunctionsRule.INSTANCE,
      AggregateExpandDistinctAggregatesRule.JOIN,

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

      // Disabled as it causes infinite loops with MergeProjectRule, ProjectFilterTranspose (with Expression preservation) and FilterProjectTranspose
      // PlannerPhase.PUSH_PROJECT_PAST_JOIN_CALCITE_RULE,

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
      FlattenRule.INSTANCE,
      EmptyRule.INSTANCE,
      CorrelateRule.INSTANCE
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
    ruleList.add(ExternalQueryScanPrule.INSTANCE);

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

  /**
   * Phase names during planning
   */
  public static final String PLAN_CONVERTED_SCAN = "Convert Scan";
  public static final String PLAN_VALIDATED = "Validation";
  public static final String PLAN_CONVERTED_TO_REL = "Convert To Rel";
  public static final String PLAN_FIND_MATERIALIZATIONS = "Find Materializations";
  public static final String PLAN_NORMALIZED = "Normalization";
  public static final String PLAN_REL_TRANSFORM = "Substitution";
  public static final String PLAN_FINAL_PHYSICAL = "Final Physical Transformation";
}
