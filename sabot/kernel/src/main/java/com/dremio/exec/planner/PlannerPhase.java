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

import static com.dremio.exec.planner.logical.DremioAggregateReduceFunctionsRule.DEFAULT_FUNCTIONS_TO_REDUCE_NO_SUM;
import static org.apache.calcite.plan.RelOptRule.any;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.some;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule.Config;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DremioLoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.MultiJoinOptimizeBushyRule;
import org.apache.calcite.rel.rules.MultiJoinProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.PushProjector.ExprCondition;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.expr.fn.hll.ConvertCountDistinctToHll;
import com.dremio.exec.expr.fn.hll.RewriteNdvAsHll;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.logical.AggregateFilterToCaseRule;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.AggregateRule;
import com.dremio.exec.planner.logical.CompositeFilterJoinRule;
import com.dremio.exec.planner.logical.Conditions;
import com.dremio.exec.planner.logical.CorrelateRule;
import com.dremio.exec.planner.logical.DremioAggregateProjectPullUpConstantsRule;
import com.dremio.exec.planner.logical.DremioAggregateReduceFunctionsRule;
import com.dremio.exec.planner.logical.DremioAggregateReduceFunctionsRule.ForGroupingSets;
import com.dremio.exec.planner.logical.DremioProjectJoinTransposeRule;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.DremioSortMergeRule;
import com.dremio.exec.planner.logical.EmptyRule;
import com.dremio.exec.planner.logical.EnhancedFilterJoinRule;
import com.dremio.exec.planner.logical.ExpansionDrule;
import com.dremio.exec.planner.logical.FilterFlattenTransposeRule;
import com.dremio.exec.planner.logical.FilterJoinRulesUtil;
import com.dremio.exec.planner.logical.FilterMergeCrule;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.FilterRule;
import com.dremio.exec.planner.logical.FilterWindowTransposeRule;
import com.dremio.exec.planner.logical.FlattenRule;
import com.dremio.exec.planner.logical.InClauseCommonSubexpressionEliminationRule;
import com.dremio.exec.planner.logical.JoinFilterCanonicalizationRule;
import com.dremio.exec.planner.logical.JoinNormalizationRule;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.JoinRule;
import com.dremio.exec.planner.logical.LimitRule;
import com.dremio.exec.planner.logical.MedianRewriteRule;
import com.dremio.exec.planner.logical.MergeProjectForFlattenRule;
import com.dremio.exec.planner.logical.MergeProjectRule;
import com.dremio.exec.planner.logical.PercentileFunctionsRewriteRule;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.ProjectRule;
import com.dremio.exec.planner.logical.PushFilterPastProjectRule;
import com.dremio.exec.planner.logical.PushJoinFilterIntoProjectRule;
import com.dremio.exec.planner.logical.PushProjectForFlattenIntoScanRule;
import com.dremio.exec.planner.logical.PushProjectForFlattenPastProjectRule;
import com.dremio.exec.planner.logical.PushProjectIntoFilesystemScanRule;
import com.dremio.exec.planner.logical.PushProjectIntoScanRule;
import com.dremio.exec.planner.logical.PushProjectPastFlattenRule;
import com.dremio.exec.planner.logical.RemoveEmptyScansRule;
import com.dremio.exec.planner.logical.RewriteProjectToFlattenRule;
import com.dremio.exec.planner.logical.SampleRule;
import com.dremio.exec.planner.logical.SimpleFilterJoinRule;
import com.dremio.exec.planner.logical.SortRule;
import com.dremio.exec.planner.logical.TableModifyRule;
import com.dremio.exec.planner.logical.UnionAllRule;
import com.dremio.exec.planner.logical.UnionRel;
import com.dremio.exec.planner.logical.ValuesRule;
import com.dremio.exec.planner.logical.WindowRule;
import com.dremio.exec.planner.logical.rule.GroupSetToCrossJoinCaseStatement;
import com.dremio.exec.planner.logical.rule.LogicalAggregateGroupKeyFixRule;
import com.dremio.exec.planner.logical.rule.MinusToJoin;
import com.dremio.exec.planner.physical.EmptyPrule;
import com.dremio.exec.planner.physical.FileSystemTableModifyPrule;
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
import com.dremio.exec.planner.physical.rule.AddFilterWindowBelowExchangeRule;
import com.dremio.exec.planner.physical.rule.FilterNestedLoopJoinPRule;
import com.dremio.exec.planner.physical.rule.FilterProjectTransposePRule;
import com.dremio.exec.planner.physical.rule.MergeProjectsPRule;
import com.dremio.exec.planner.physical.rule.computation.HashJoinComputationExtractionRule;
import com.dremio.exec.planner.physical.rule.computation.NestedLoopJoinComputationExtractionRule;
import com.dremio.exec.planner.tablefunctions.ExternalQueryScanPrule;
import com.dremio.exec.planner.tablefunctions.ExternalQueryScanRule;
import com.dremio.exec.store.mfunctions.MFunctionQueryScanPrule;
import com.dremio.exec.store.mfunctions.MFunctionQueryScanRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public enum PlannerPhase {

  WINDOW_REWRITE("Window Function Rewrites") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(
          CALC_REDUCE_EXPRESSIONS_CALCITE_RULE,
          CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW
          );
    }
  },

  GROUP_SET_REWRITE("GroupSet Rewrites") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(
        REDUCE_FUNCTIONS_FOR_GROUP_SETS,
        GroupSetToCrossJoinCaseStatement.RULE
      );
    }
  },

  JDBC_PUSHDOWN("JDBC Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      final ImmutableList.Builder<RelOptRule> rules = ImmutableList.builder();
      rules.add(CALCITE_AGG_REDUCE_FUNCTIONS_NO_REDUCE_SUM);

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

  NESTED_SCHEMA_PROJECT_PUSHDOWN("Nested-Schema Project Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(
        PUSH_PROJECT_PAST_FILTER_LOGICAL_INSTANCE,
        PUSH_PROJECT_PAST_JOIN_RULE_WITH_EXPR_JOIN,
        MergeProjectRule.LOGICAL_INSTANCE
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

  FILESYSTEM_PROJECT_PUSHDOWN("FileSystem Project Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      return RuleSets.ofList(
        PushFilterPastProjectRule.LOGICAL_INSTANCE,
        PushProjectIntoFilesystemScanRule.INSTANCE
      );
    }
  },

  PRE_LOGICAL("Pre-Logical Filter Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      ImmutableList.Builder<RelOptRule> b = ImmutableList.builder();
      PlannerSettings ps = context.getPlannerSettings();
      ImmutableList<RelOptRule> commonRules = getPreLogicalCommonRules(context);
      b.addAll(commonRules);
      if (ps.isEnhancedFilterJoinPushdownEnabled()) {
        b.add(EnhancedFilterJoinRule.WITH_FILTER);
        b.add(EnhancedFilterJoinRule.NO_FILTER);
      }
      return RuleSets.ofList(b.build());
    }
  },

  PRE_LOGICAL_TRANSITIVE("Pre-Logical Transitive Filter Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      ImmutableList.Builder<RelOptRule> b = ImmutableList.builder();
      ImmutableList<RelOptRule> commonRules = getPreLogicalCommonRules(context);
      b.addAll(commonRules);
      b.add(PlannerPhase.PUSH_PROJECT_PAST_JOIN_CALCITE_RULE);
      return RuleSets.ofList(b.build());
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
        .add(MergeProjectRule.LOGICAL_INSTANCE)
        .add(PushJoinFilterIntoProjectRule.INSTANCE);


      // Check if multi-join optimization has been enabled
      if (context.getPlannerSettings().isJoinOptimizationEnabled()) {
        if (context.getPlannerSettings().isExperimentalBushyJoinOptimizerEnabled()) {
          builder.add(MULTI_JOIN_OPTIMIZE_BUSHY_RULE);
        } else {
          boolean useKey = context.getPlannerSettings().joinUseKeyForNextFactor();
          boolean rotateFactors = context.getPlannerSettings().joinRotateFactors();
          builder.add(DremioLoptOptimizeJoinRule.Config.DEFAULT
            .withFindOnlyOneOrdering(false)
            .withUseCardinalityForNextFactor(useKey)
            .withRotateFactors(rotateFactors)
            .withRelBuilderFactory(DremioRelFactories.LOGICAL_BUILDER)
            .toRule());
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
        moreRules.add(CoreRules.PROJECT_REMOVE);
      }

      moreRules.add(ExternalQueryScanRule.INSTANCE);
      moreRules.add(MFunctionQueryScanRule.INSTANCE);

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

  POST_JOIN_OPTIMIZATION("Post Join Optimization") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context) {
      final ImmutableList.Builder<RelOptRule> rules = ImmutableList.builder();
      rules.add(InClauseCommonSubexpressionEliminationRule.INSTANCE);
      if (context.getPlannerSettings().isUnionAllDistributeEnabled()) {
        rules.add(UNION_MERGE_RULE);
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
      builder.add(FilterProjectTransposePRule.FILTER_PROJECT_NLJ);
      builder.add(FilterNestedLoopJoinPRule.INSTANCE);
      if (context.getPlannerSettings().options.getOption(PlannerSettings.ENABlE_PROJCT_NLJ_MERGE)) {
        builder.add(MergeProjectsPRule.PROJECT_PROJECT_JOIN);
      }
      if (context.getPlannerSettings().options.getOption(PlannerSettings.NLJ_PUSHDOWN)) {
        builder.add(NestedLoopJoinComputationExtractionRule.INSTANCE);
      }
      if (context.getPlannerSettings().options.getOption(PlannerSettings.HASH_JOIN_PUSHDOWN)) {
        builder.add(HashJoinComputationExtractionRule.INSTANCE);
      }

      if (context.getPlannerSettings().options.getOption(PlannerSettings.ENABLE_FILTER_WINDOW_OPTIMIZER)) {
        builder.add(AddFilterWindowBelowExchangeRule.INSTANCE);
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
   ReduceExpressionsRule.FilterReduceExpressionsRule.Config.DEFAULT
      .withMatchNullability(true)
      .withUnknownAsFalse(true)
      .withTreatDynamicCallsAsConstant(true)
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .as(ReduceExpressionsRule.FilterReduceExpressionsRule.Config.class)
      .withOperandFor(LogicalFilter.class)
      .as(ReduceExpressionsRule.FilterReduceExpressionsRule.Config.class)
      .toRule();

  /**
   * Singleton rule that reduces constants inside a {@link LogicalProject}.
   */
  public static final ReduceExpressionsRule PROJECT_REDUCE_EXPRESSIONS_CALCITE_RULE =
    ReduceExpressionsRule.ProjectReduceExpressionsRule.Config.DEFAULT
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .as(ReduceExpressionsRule.ProjectReduceExpressionsRule.Config.class)
      .withOperandFor(LogicalProject.class)
      .withTreatDynamicCallsAsConstant(true)
      .as(ReduceExpressionsRule.ProjectReduceExpressionsRule.Config.class)
      .toRule();

  /**
   * Singleton rule that reduces constants inside a {@link LogicalCalc}.
   */
  public static final ReduceExpressionsRule CALC_REDUCE_EXPRESSIONS_CALCITE_RULE =
    ReduceExpressionsRule.CalcReduceExpressionsRule.Config.DEFAULT
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .as(ReduceExpressionsRule.CalcReduceExpressionsRule.Config.class)
      .withOperandFor(LogicalCalc.class)
      .withTreatDynamicCallsAsConstant(true)
      .as(ReduceExpressionsRule.CalcReduceExpressionsRule.Config.class)
      .toRule();

  public static final ReduceExpressionsRule JOIN_REDUCE_EXPRESSIONS_CALCITE_RULE =
    ReduceExpressionsRule.JoinReduceExpressionsRule.Config.DEFAULT
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .as(ReduceExpressionsRule.JoinReduceExpressionsRule.Config.class)
      .withOperandFor(LogicalJoin.class)
      .withTreatDynamicCallsAsConstant(true)
      .as(ReduceExpressionsRule.JoinReduceExpressionsRule.Config.class)
      .toRule();
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
    FilterSetOpTransposeRule.Config.DEFAULT.withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .withOperandSupplier(b0 ->
              b0.operand(LogicalFilter.class).oneInput(b1 ->
              b1.operand(SetOp.class).anyInputs()))
      .as(FilterSetOpTransposeRule.Config.class)
      .toRule();

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

  public static final RelOptRule PUSH_PROJECT_PAST_JOIN_RULE_WITH_EXPR_JOIN = new ProjectJoinTransposeRule(
    ProjectRel.class,
    JoinRel.class,
    new DremioProjectJoinTransposeRule.ProjectJoinExprCondition(),
    DremioRelFactories.LOGICAL_BUILDER);

  public static final RelOptRule PUSH_PROJECT_PAST_JOIN_CALCITE_RULE = DremioProjectJoinTransposeRule.INSTANCE;

  public static final RelOptRule LOGICAL_FILTER_CORRELATE_RULE = new FilterCorrelateRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  public static final DremioAggregateReduceFunctionsRule REDUCE_FUNCTIONS_FOR_GROUP_SETS =
    new ForGroupingSets(LogicalAggregate.class,
      DremioRelFactories.CALCITE_LOGICAL_BUILDER, EnumSet.copyOf(Config.DEFAULT_FUNCTIONS_TO_REDUCE));

  public static final AggregateReduceFunctionsRule CALCITE_AGG_REDUCE_FUNCTIONS_NO_REDUCE_SUM =
    new AggregateReduceFunctionsRule(LogicalAggregate.class,
      RelFactories.LOGICAL_BUILDER,
      EnumSet.copyOf(DEFAULT_FUNCTIONS_TO_REDUCE_NO_SUM));

  private static final class LogicalFilterJoinRule extends FilterJoinRule {
    private LogicalFilterJoinRule() {
      super(Config.EMPTY
        .withRelBuilderFactory(
          DremioRelFactories.CALCITE_LOGICAL_BUILDER)
        .withOperandSupplier(b0 ->
          b0.operand(LogicalFilter.class).oneInput(b1 ->
            b1.operand(LogicalJoin.class).anyInputs()))
        .withDescription("FilterJoinRule:filter")
        .as(FilterJoinRule.Config.class)
        .withSmart(true)
        .withPredicate(FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM)
        .as(FilterJoinRule.Config.class));
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
    JoinConditionPushRule.Config.EMPTY
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .withOperandSupplier(b ->
        b.operand(LogicalJoin.class).anyInputs())
      .withDescription("FilterJoinRuleCrel:no-filter")
      .as(JoinConditionPushRule.Config.class)
      .withSmart(true)
      .withPredicate(FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM)
      .as(JoinConditionPushRule.Config.class));

  public static final FilterJoinRule JOIN_CONDITION_PUSH_LOGICAL_RULE = new JoinConditionPushRule(
    JoinConditionPushRule.Config.EMPTY
      .withRelBuilderFactory(DremioRelFactories.LOGICAL_BUILDER)
      .withOperandSupplier(b ->
        b.operand(LogicalJoin.class).anyInputs())
      .withDescription("FilterJoinRuleDrel:no-filter")
      .as(JoinConditionPushRule.Config.class)
      .withSmart(true)
      .withPredicate(FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM)
      .as(JoinConditionPushRule.Config.class));

  private static class JoinConditionPushRule extends FilterJoinRule {
    public JoinConditionPushRule(Config config) {
      super(config);


    }
    public JoinConditionPushRule() {
      super(JoinConditionPushRule.Config.EMPTY
        .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
        .withOperandSupplier(b ->
                b.operand(LogicalJoin.class).anyInputs())
        .withDescription("FilterJoinRule:no-filter")
        .as(JoinConditionPushRule.Config.class)
        .withSmart(true)
        .withPredicate(FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM)
        .as(JoinConditionPushRule.Config.class));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Join join = call.rel(0);
      perform(call, null, join);
    }
  }

  public static final JoinPushExpressionsRule JOIN_PUSH_EXPRESSIONS_RULE = new JoinPushExpressionsRule(LogicalJoin.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);
  private static final JoinPushExpressionsRule JOIN_PUSH_EXPRESSIONS_LOGICAL_RULE = new JoinPushExpressionsRule(JoinRel.class, DremioRelFactories.LOGICAL_BUILDER);
  public static final UnionMergeRule UNION_MERGE_RULE = new UnionMergeRule(UnionRel.class, "UnionMergeDrule", DremioRelFactories.LOGICAL_BUILDER);


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
  private static final RelOptRule LOPT_UNOPTIMIZE_JOIN_RULE =
    DremioLoptOptimizeJoinRule.Config.DEFAULT
      .withFindOnlyOneOrdering(true)
      .withRelBuilderFactory(DremioRelFactories.LOGICAL_BUILDER)
      .toRule();;
  private static final MultiJoinOptimizeBushyRule MULTI_JOIN_OPTIMIZE_BUSHY_RULE = new MultiJoinOptimizeBushyRule(DremioRelFactories.LOGICAL_BUILDER);

  private static final RelOptRule PUSH_PROJECT_PAST_FILTER_INSTANCE = new ProjectFilterTransposeRule(
    ProjectRel.class,
    FilterRel.class,
    DremioRelFactories.LOGICAL_PROPAGATE_BUILDER, Conditions.PRESERVE_ITEM_CASE);

  private static final RelOptRule PUSH_PROJECT_PAST_FILTER_LOGICAL_INSTANCE = new ProjectFilterTransposeRule(
    ProjectRel.class,
    FilterRel.class,
    DremioRelFactories.LOGICAL_BUILDER, Conditions.PRESERVE_CASE_NESTED_FIELDS);


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

    if (ps.options.getOption(PlannerSettings.FILTER_EXTRACT_CONJUNCTIONS)) {
      userConfigurableRules.add(ExtractCommonConjunctionInFilterRule.INSTANCE);
      userConfigurableRules.add(ExtractCommonConjunctionInJoinRule.INSTANCE);
    }

    if (ps.isConstantFoldingEnabled()) {
      // TODO - DRILL-2218, DX-2319
      if (ps.isReduceJoinExpressionsEnabled()) {
        userConfigurableRules.add(JOIN_REDUCE_EXPRESSIONS_CALCITE_RULE);
      }
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

    userConfigurableRules.add(AggregateFilterToCaseRule.INSTANCE);

    userConfigurableRules.add(MedianRewriteRule.INSTANCE);

    userConfigurableRules.add(PercentileFunctionsRewriteRule.INSTANCE);

    return RuleSets.ofList(userConfigurableRules.build());
  }

  static ImmutableList<RelOptRule> getPreLogicalCommonRules(OptimizerRulesContext context) {
    ImmutableList.Builder<RelOptRule> b = ImmutableList.builder();
    PlannerSettings ps = context.getPlannerSettings();
    b.add(
      DremioAggregateProjectPullUpConstantsRule.INSTANCE2_REMOVE_ALL,
      LogicalAggregateGroupKeyFixRule.RULE,
      ConvertCountDistinctToHll.INSTANCE,
      RewriteNdvAsHll.INSTANCE,

      PushFilterPastProjectRule.CALCITE_NO_CHILD_CHECK,

      JoinFilterCanonicalizationRule.INSTANCE,

      FILTER_SET_OP_TRANSPOSE_CALCITE_RULE,
      FILTER_AGGREGATE_TRANSPOSE_CALCITE_RULE,
      FILTER_MERGE_CALCITE_RULE,
      CoreRules.INTERSECT_TO_DISTINCT,
      MinusToJoin.RULE,

      DremioSortMergeRule.INSTANCE,

      CoreRules.PROJECT_WINDOW_TRANSPOSE,
      CoreRules.PROJECT_SET_OP_TRANSPOSE,
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

    return b.build();
  }

  // These logical rules don't require any context, so singleton instances can be used.
  static final RuleSet LOGICAL_RULE_SET = RuleSets.ofList(ImmutableSet.<RelOptRule>builder()
    .add(

      // remove expansion nodes when converting to logical.
      ExpansionDrule.INSTANCE,

      /*
       * Aggregate optimization rules
       */
      CoreRules.UNION_TO_DISTINCT,
      CoreRules.AGGREGATE_REMOVE,
      DremioAggregateReduceFunctionsRule.INSTANCE,
      CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,

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
      CorrelateRule.INSTANCE,
      TableModifyRule.INSTANCE
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
    ruleList.add(MFunctionQueryScanPrule.INSTANCE);

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

    ruleList.add(new FileSystemTableModifyPrule(optimizerRulesContext));

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
  public static final String PLAN_CACHE_USED = "Plan Cache Used";
  public static final String PLAN_CONVERTED_TO_REL = "Convert To Rel";
  public static final String PLAN_FIND_MATERIALIZATIONS = "Find Materializations";
  public static final String PLAN_NORMALIZED = "Normalization";
  public static final String PLAN_REL_TRANSFORM = "Substitution";
  public static final String PLAN_FINAL_PHYSICAL = "Final Physical Transformation";
}
