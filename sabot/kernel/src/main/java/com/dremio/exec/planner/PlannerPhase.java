/*
 * Copyright (C) 2017 Dremio Corporation
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterJoinRule.FilterIntoJoinRule;
import org.apache.calcite.rel.rules.FilterJoinRule.JoinConditionPushRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
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
import com.dremio.exec.planner.logical.AggregateRule;
import com.dremio.exec.planner.logical.Conditions;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.FilterFlattenTransposeRule;
import com.dremio.exec.planner.logical.FilterJoinRulesUtil;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.FilterRule;
import com.dremio.exec.planner.logical.FlattenRule;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.JoinRule;
import com.dremio.exec.planner.logical.LimitRule;
import com.dremio.exec.planner.logical.MergeProjectForFlattenRule;
import com.dremio.exec.planner.logical.MergeProjectRule;
import com.dremio.exec.planner.logical.OldPushProjectForFlattenIntoScanRule;
import com.dremio.exec.planner.logical.OldPushProjectIntoScanRule;
import com.dremio.exec.planner.logical.OldScanRelRule;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.ProjectRule;
import com.dremio.exec.planner.logical.PushFiltersProjectPastFlattenRule;
import com.dremio.exec.planner.logical.PushFilterPastProjectRule;
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
import com.dremio.exec.planner.physical.ScanPrule;
import com.dremio.exec.planner.physical.ScreenPrule;
import com.dremio.exec.planner.physical.SortConvertPrule;
import com.dremio.exec.planner.physical.SortPrule;
import com.dremio.exec.planner.physical.StreamAggPrule;
import com.dremio.exec.planner.physical.UnionAllPrule;
import com.dremio.exec.planner.physical.ValuesPrule;
import com.dremio.exec.planner.physical.WindowPrule;
import com.dremio.exec.planner.physical.WriterPrule;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePlugin2;
import com.dremio.exec.store.StoragePluginInstanceRulesFactory;
import com.dremio.exec.store.StoragePluginTypeRulesFactory;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.StoragePluginType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

public enum PlannerPhase {

  WINDOW_REWRITE("Window Function Rewrites") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return RuleSets.ofList(
          CALC_REDUCE_EXPRESSIONS_CALCITE_RULE,
          ProjectToWindowRule.PROJECT
          );
    }
  },

  JDBC_PUSHDOWN("JDBC Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(
          RuleSets.ofList(AggregateReduceFunctionsRule.INSTANCE),
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
          getStorageRules(context, plugins, this));
    }
  },

  FLATTEN_PUSHDOWN("Flatten Function Pushdown") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return RuleSets.ofList(
          PushFiltersProjectPastFlattenRule.INSTANCE,
          PushProjectPastFlattenRule.INSTANCE,
          OldPushProjectForFlattenIntoScanRule.INSTANCE,
          PushProjectForFlattenIntoScanRule.INSTANCE,
          PushProjectForFlattenPastProjectRule.INSTANCE,
          MergeProjectForFlattenRule.INSTANCE,
          PUSH_PROJECT_PAST_FILTER_INSTANCE,
          PushFilterPastProjectRule.INSTANCE,
          PushProjectPastJoinRule.INSTANCE
      );
    }
  },

  JOIN_PLANNING("LOPT Join Planning") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(
          RuleSets.ofList(
              JOIN_TO_MULTIJOIN_RULE,
              LOPT_OPTIMIZE_JOIN_RULE),
              //ProjectRemoveRule.INSTANCE),
          getStorageRules(context, plugins, this)
          );
    }
  },

  REDUCE_EXPRESSIONS("Reduce Expressions") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.getEnabledReduceExpressionsRules(context);
    }
  },

  LOGICAL("Logical Planning", true) {
    @Override
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      RuleSet ruleSet = PlannerPhase.mergedRuleSets(
          getStorageRules(context, plugins, this),
          LOGICAL_RULE_SET);


      if(!context.getPlannerSettings().isFilterFlattenTransposeEnabled()){
        return ruleSet;
      }

      return PlannerPhase.mergedRuleSets(ruleSet, RuleSets.ofList(FilterFlattenTransposeRule.INSTANCE));
    }
  },

  PHYSICAL("Physical Planning") {
    @Override
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(
          PlannerPhase.getPhysicalRules(context),
          getStorageRules(context, plugins, this));
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

  public abstract RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins);

  /**
   * Collect all rules for StoragePlugins.
   *
   * Collects the following:
   *   - One set of rules for each storage plugin type.
   *   - One set of rules for each storage plugin instance.
   *   - One set of rules for old storage plugins (per instance) (deprecated)
   *   - One set of rules for old abstract storage plugins (per instance) (deprecated)
   *
   * Note: This includes handling of both the new and old StoragePlugin interfaces. Once all
   * plugins are migrated, this will only do the first two retrievals.
   *
   * @param context
   * @param plugins
   * @param phase
   * @return
   */
  private static RuleSet getStorageRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins,
      PlannerPhase phase) {
    final ImmutableSet.Builder<RelOptRule> rules = ImmutableSet.builder();
    Map<StoragePluginType, Class<?>> typeRulesFactories = new HashMap<>();


    // add instance level rules.
    for(StoragePlugin<?> plugin : plugins){
      if(plugin.getStoragePlugin2() != null){
        StoragePlugin2 registry = plugin.getStoragePlugin2();
        StoragePluginId pluginId = registry.getId();
        Class<?> typeRules = pluginId.getType().getRulesFactoryClass();
        if(typeRules != null){
          typeRulesFactories.put(pluginId.getType(), typeRules);
        }
        try {
          if(registry.getRulesFactoryClass() != null){
            StoragePluginInstanceRulesFactory factory = registry.getRulesFactoryClass().newInstance();
            rules.addAll(factory.getRules(context, phase, pluginId));
          }
        } catch (InstantiationException | IllegalAccessException e) {
          throw Throwables.propagate(e);
        }
      }else if(plugin instanceof AbstractStoragePlugin){
        rules.addAll(((AbstractStoragePlugin<?>) plugin).getOptimizerRules(context, phase));
      }else{
        rules.addAll(plugin.getOptimizerRules(context));
      }
    }

    // add type level rules
    for(Entry<StoragePluginType, Class<?>> entry : typeRulesFactories.entrySet()){
      try {
        StoragePluginTypeRulesFactory factory = (StoragePluginTypeRulesFactory) entry.getValue().newInstance();
        rules.addAll(factory.getRules(context, phase, entry.getKey()));
      } catch (InstantiationException | IllegalAccessException e) {
        throw Throwables.propagate(e);
      }
    }

    ImmutableSet<RelOptRule> rulesSet = rules.build();
    return RuleSets.ofList(rulesSet);
  }

  // START ---------------------------------------------
  // Calcite's default RelBuilder (RelFactories.LOGICAL_BUILDER) uses Values for an empty expression; see
  // tools.RelBuilder#empty. But Dremio does not support Values for the purpose of empty expressions. Instead,
  // Dremio uses LIMIT(0, 0); see logical.RelBuilder#empty. These rules use this alternative builder.

  /**
   * Singleton rule that reduces constants inside a {@link LogicalFilter}.
   */
  static final ReduceExpressionsRule FILTER_REDUCE_EXPRESSIONS_CALCITE_RULE =
    new FilterReduceExpressionsRule(LogicalFilter.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Singleton rule that reduces constants inside a {@link LogicalProject}.
   */
  static final ReduceExpressionsRule PROJECT_REDUCE_EXPRESSIONS_CALCITE_RULE =
    new ProjectReduceExpressionsRule(LogicalProject.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Singleton rule that reduces constants inside a {@link LogicalCalc}.
   */
  static final ReduceExpressionsRule CALC_REDUCE_EXPRESSIONS_CALCITE_RULE =
    new CalcReduceExpressionsRule(LogicalCalc.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Planner rule that combines two {@link Filter}s.
   */
  static final FilterMergeRule FILTER_MERGE_CALCITE_RULE =
    new FilterMergeRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Planner rule that pushes a {@link Filter} past a
   * {@link org.apache.calcite.rel.core.SetOp}.
   */
  static final FilterSetOpTransposeRule FILTER_SET_OP_TRANSPOSE_CALCITE_RULE =
    new FilterSetOpTransposeRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  /**
   * Planner rule that pushes predicates from a Filter into the Join below.
   */
  static final FilterJoinRule FILTER_INTO_JOIN_CALCITE_RULE =
    new FilterIntoJoinRule(true, DremioRelFactories.CALCITE_LOGICAL_BUILDER,
      FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM);

  /**
   * Planner rule that pushes predicates in a Join into the inputs to the Join.
   */
  static final FilterJoinRule JOIN_CONDITION_PUSH_CALCITE_RULE =
    new JoinConditionPushRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER,
      FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM);

  /**
   * Planner rule that pushes a {@link LogicalProject} past a {@link LogicalFilter}.
   */
  static final ProjectFilterTransposeRule PUSH_PROJECT_PAST_FILTER_CALCITE_RULE =
    new ProjectFilterTransposeRule(LogicalProject.class, LogicalFilter.class,
      DremioRelFactories.CALCITE_LOGICAL_BUILDER, Conditions.PRESERVE_ITEM_CASE);

  /**
   * Planner rule that pushes a {@link org.apache.calcite.rel.core.Filter}
   * past a {@link org.apache.calcite.rel.core.Aggregate}.
   */
  static final FilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE_CALCITE_RULE =
    new FilterAggregateTransposeRule(Filter.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER,
      Aggregate.class);

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
  static final RelOptRule LOPT_OPTIMIZE_JOIN_RULE = new LoptOptimizeJoinRule(DremioRelFactories.LOGICAL_BUILDER);

  final static RelOptRule PUSH_PROJECT_PAST_FILTER_INSTANCE = new ProjectFilterTransposeRule(
    ProjectRel.class,
    FilterRel.class,
    DremioRelFactories.LOGICAL_PROPAGATE_BUILDER,
    Conditions.PRESERVE_ITEM_CASE);

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
      // Add support for Distinct Union (by using Union-All followed by Distinct)
      UnionToDistinctRule.INSTANCE,

      AggregateReduceFunctionsRule.INSTANCE,
      AggregateExpandDistinctAggregatesRule.JOIN,

      // Add support for WHERE style joins.
      FILTER_INTO_JOIN_CALCITE_RULE,
      JOIN_CONDITION_PUSH_CALCITE_RULE,
      JoinPushExpressionsRule.INSTANCE,
      // End support for WHERE style joins.

      /*
       Filter push-down related rules
       */
      PushFilterPastProjectRule.CALCITE_INSTANCE,
      // Due to infinite loop in planning (DRILL-3257), temporarily disable this rule
      // FILTER_SET_OP_TRANSPOSE_CALCITE_RULE,
      FILTER_AGGREGATE_TRANSPOSE_CALCITE_RULE,
      FILTER_MERGE_CALCITE_RULE,
      AggregateRemoveRule.INSTANCE,
      //ProjectRemoveRule.INSTANCE,
      //SortRemoveRule.INSTANCE,

      /*
       Projection push-down related rules
       */
      PUSH_PROJECT_PAST_FILTER_CALCITE_RULE,
      PushProjectPastJoinRule.CALCITE_INSTANCE,
      // Due to infinite loop in planning (DRILL-3257), temporarily disable this rule
      //ProjectSetOpTransposeRule.INSTANCE,
      ProjectWindowTransposeRule.INSTANCE,
      OldPushProjectIntoScanRule.INSTANCE,
      PushProjectIntoScanRule.INSTANCE,
      ProjectRule.INSTANCE,
      MergeProjectRule.INSTANCE,

      /*
       Rewrite flatten rules
       */
      RewriteProjectToFlattenRule.INSTANCE,

      /*
       Convert to Drel
       */
      ExpandConversionRule.INSTANCE,
      FilterRule.INSTANCE,
      WindowRule.INSTANCE,
      AggregateRule.INSTANCE,
      LimitRule.INSTANCE,
      SampleRule.INSTANCE,
      SortRule.INSTANCE,
      JoinRule.INSTANCE,
      UnionAllRule.INSTANCE,
      ValuesRule.INSTANCE,
      OldScanRelRule.INSTANCE,

      FlattenRule.INSTANCE
      ).build());

  static final RuleSet getPhysicalRules(OptimizerRulesContext optimizerRulesContext) {
    final List<RelOptRule> ruleList = new ArrayList<>();
    final PlannerSettings ps = optimizerRulesContext.getPlannerSettings();

    ruleList.add(SortConvertPrule.INSTANCE);
    ruleList.add(SortPrule.INSTANCE);
    ruleList.add(ProjectPrule.INSTANCE);
    ruleList.add(FlattenPrule.INSTANCE);
    ruleList.add(ScanPrule.INSTANCE);
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

  static RuleSet mergedRuleSets(RuleSet... ruleSets) {
    final ImmutableSet.Builder<RelOptRule> relOptRuleSetBuilder = ImmutableSet.builder();
    for (final RuleSet ruleSet : ruleSets) {
      for (final RelOptRule relOptRule : ruleSet) {
        relOptRuleSetBuilder.add(relOptRule);
      }
    }
    return RuleSets.ofList(relOptRuleSetBuilder.build());
  }
}
