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
package com.dremio.exec.planner.rules;

import static com.dremio.exec.planner.logical.DremioAggregateReduceFunctionsRule.DEFAULT_FUNCTIONS_TO_REDUCE_NO_SUM;
import static org.apache.calcite.plan.RelOptRule.any;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.some;

import java.util.EnumSet;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.DremioLoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.MultiJoinOptimizeBushyRule;
import org.apache.calcite.rel.rules.MultiJoinProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.UnionMergeRule;

import com.dremio.exec.planner.DremioJoinToMuliJoinRule;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.Conditions;
import com.dremio.exec.planner.logical.DremioAggregateReduceFunctionsRule;
import com.dremio.exec.planner.logical.DremioProjectJoinTransposeRule;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.FilterJoinRulesUtil;
import com.dremio.exec.planner.logical.FilterMergeCrule;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.ProjectInputRefPastFilterRule;
import com.dremio.exec.planner.logical.ProjectPullUpConstTypeCastRule;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.RewriteProjectToFlattenRule;
import com.dremio.exec.planner.logical.rule.GroupSetToCrossJoinCaseStatement;

public interface DremioCoreRules {



  AggregateReduceFunctionsRule CALCITE_AGG_REDUCE_FUNCTIONS_NO_REDUCE_SUM =
    AggregateReduceFunctionsRule.Config.DEFAULT
      .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
      .as(AggregateReduceFunctionsRule.Config.class)
      .withOperandFor(LogicalAggregate.class)
      // reduce specific functions provided by the client
      .withFunctionsToReduce(EnumSet.copyOf(DEFAULT_FUNCTIONS_TO_REDUCE_NO_SUM))
      .toRule();

  RelOptRule CONVERT_JOIN_SUB_QUERY_TO_CORRELATE =
    DremioJoinSubQueryRemoveRule.Config.DEFAULT
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .toRule();

  RelOptRule CONVERT_FILTER_SUB_QUERY_TO_CORRELATE =
    SubQueryRemoveRule.Config.FILTER
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .toRule();

  RelOptRule CONVERT_PROJECT_SUB_QUERY_TO_CORRELATE =
    SubQueryRemoveRule.Config.PROJECT
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .toRule();

  GroupSetToCrossJoinCaseStatement.Rule GROUP_SET_TO_CROSS_JOIN_CASE_STATEMENT_RULE =
    GroupSetToCrossJoinCaseStatement.Rule.Config.DEFUALT.toRule();

  /**
   * Planner rule that pushes a {@link org.apache.calcite.rel.core.Filter}
   * past a {@link org.apache.calcite.rel.core.Aggregate}.
   */
  FilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE_CALCITE_RULE =
    new FilterAggregateTransposeRule(LogicalFilter.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER, LogicalAggregate.class);

  FilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE_DRULE =
    new FilterAggregateTransposeRule(FilterRel.class, DremioRelFactories.LOGICAL_BUILDER, AggregateRel.class);

  DremioAggregateReduceFunctionsRule REDUCE_FUNCTIONS_FOR_GROUP_SETS =
    new DremioAggregateReduceFunctionsRule.ForGroupingSets(LogicalAggregate.class,
      DremioRelFactories.CALCITE_LOGICAL_BUILDER, EnumSet.copyOf(AggregateReduceFunctionsRule.Config.DEFAULT_FUNCTIONS_TO_REDUCE));


  /**
   * Planner rule that pushes predicates in a Join into the inputs to the Join.
   */
  FilterJoinRule JOIN_CONDITION_PUSH_CALCITE_RULE = new JoinConditionPushRule(
    JoinConditionPushRule.Config.EMPTY
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .withOperandSupplier(b ->
        b.operand(LogicalJoin.class).anyInputs())
      .withDescription("FilterJoinRuleCrel:no-filter")
      .as(JoinConditionPushRule.Config.class)
      .withSmart(true)
      .withPredicate(FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM)
      .as(JoinConditionPushRule.Config.class));

  FilterJoinRule JOIN_CONDITION_PUSH_LOGICAL_RULE = new JoinConditionPushRule(
    JoinConditionPushRule.Config.EMPTY
      .withRelBuilderFactory(DremioRelFactories.LOGICAL_BUILDER)
      .withOperandSupplier(b ->
        b.operand(LogicalJoin.class).anyInputs())
      .withDescription("FilterJoinRuleDrel:no-filter")
      .as(JoinConditionPushRule.Config.class)
      .withSmart(true)
      .withPredicate(FilterJoinRulesUtil.EQUAL_IS_NOT_DISTINCT_FROM)
      .as(JoinConditionPushRule.Config.class));

  //just a workaround for the empty field list in projects. Need to find a formal way to deal with dummy projects.
  RelOptRule JOIN_PROJECT_TRANSPOSE_LEFT =
    JoinProjectTransposeRule.Config.LEFT
      .withOperandSupplier(b0 ->
        b0.operand(Join.class).inputs(
          b1 -> b1.operand(Project.class).predicate(p->!p.getInput().getRowType().getFieldList().isEmpty()).anyInputs()))
      .toRule();

  RelOptRule JOIN_PROJECT_TRANSPOSE_RIGHT =
    JoinProjectTransposeRule.Config.RIGHT
      .withOperandSupplier(b0 ->
        b0.operand(Join.class).inputs(
          b1 -> b1.operand(RelNode.class).predicate(p->!(p instanceof Project)).anyInputs(),
          b2 -> b2.operand(Project.class).predicate(p->!p.getInput().getRowType().getFieldList().isEmpty()).anyInputs()))
      .toRule();

  JoinPushExpressionsRule JOIN_PUSH_EXPRESSIONS_RULE = new JoinPushExpressionsRule(LogicalJoin.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);
  JoinPushExpressionsRule JOIN_PUSH_EXPRESSIONS_LOGICAL_RULE = new JoinPushExpressionsRule(JoinRel.class, DremioRelFactories.LOGICAL_BUILDER);

  /**
   * Planner rule that pushes a {@link LogicalProject} past a {@link LogicalFilter}.
   */
  ProjectFilterTransposeRule PUSH_PROJECT_PAST_FILTER_CALCITE_RULE =
    new ProjectFilterTransposeRule(LogicalProject.class, LogicalFilter.class,
      DremioRelFactories.CALCITE_LOGICAL_BUILDER, Conditions.PRESERVE_ITEM_CASE);

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

  RelOptRule MULTIJOIN_BOTH_PROJECTS_TRANSPOSE_RULE =
    new MultiJoinProjectTransposeRule(
      operand(JoinRel.class,
        operand(ProjectRel.class,
          operand(MultiJoin.class, any())),
        operand(ProjectRel.class,
          operand(MultiJoin.class, any()))),
      DremioRelFactories.LOGICAL_BUILDER,
      "MultiJoinProjectTransposeRule:TwoProjects");
  RelOptRule MULTIJOIN_LEFT_PROJECT_TRANSPOSE_RULE =
    new MultiJoinProjectTransposeRule(
      operand(JoinRel.class,
        some(
          operand(ProjectRel.class,
            operand(MultiJoin.class, any())))),
      DremioRelFactories.LOGICAL_BUILDER,
      "MultiJoinProjectTransposeRule:LeftProject");
  RelOptRule MULTIJOIN_RIGHT_PROJECT_TRANSPOSE_RULE =
    new MultiJoinProjectTransposeRule(
      operand(JoinRel.class,
        operand(RelNode.class, any()),
        operand(ProjectRel.class,
          operand(MultiJoin.class, any()))),
      DremioRelFactories.LOGICAL_BUILDER,
      "MultiJoinProjectTransposeRule:RightProject");

  RelOptRule JOIN_TO_MULTIJOIN_RULE =
    DremioJoinToMuliJoinRule.INSTANCE;

  RelOptRule JOIN_TO_MULTIJOIN_RULE_NO_OUTER =
    DremioJoinToMuliJoinRule.NO_OUTER;
  RelOptRule PROJECT_MULTIJOIN_MERGE_RULE =
    new ProjectMultiJoinMergeRule(ProjectRel.class, DremioRelFactories.LOGICAL_BUILDER);
  RelOptRule FILTER_MULTIJOIN_MERGE_RULE =
    new FilterMultiJoinMergeRule(FilterRel.class, DremioRelFactories.LOGICAL_BUILDER);
  RelOptRule LOPT_UNOPTIMIZE_JOIN_RULE =
    DremioLoptOptimizeJoinRule.Config.DEFAULT
      .withFindOnlyOneOrdering(true)
      .withRelBuilderFactory(DremioRelFactories.LOGICAL_BUILDER)
      .toRule();;
  MultiJoinOptimizeBushyRule MULTI_JOIN_OPTIMIZE_BUSHY_RULE =
    new MultiJoinOptimizeBushyRule(DremioRelFactories.LOGICAL_BUILDER);

  RelOptRule PUSH_PROJECT_PAST_FILTER_INSTANCE =
    new ProjectFilterTransposeRule(
      ProjectRel.class,
      FilterRel.class,
      DremioRelFactories.LOGICAL_PROPAGATE_BUILDER,
      Conditions.PRESERVE_ITEM_CASE);

  RelOptRule PUSH_PROJECT_PAST_FILTER_LOGICAL_INSTANCE =
    new ProjectFilterTransposeRule(
      ProjectRel.class,
      FilterRel.class,
      DremioRelFactories.LOGICAL_BUILDER,
      Conditions.PRESERVE_CASE_NESTED_FIELDS);

  RelOptRule PUSH_PROJECT_INPUT_REF_PAST_FILTER_LOGICAL_INSTANCE = new ProjectInputRefPastFilterRule();

  RelOptRule UNION_MERGE_RULE = UnionMergeRule.Config.DEFAULT.withOperandFor(Union.class).toRule();

  /**
   * Singleton rule that reduces constants inside a {@link LogicalFilter}.
   */
  ReduceExpressionsRule FILTER_REDUCE_EXPRESSIONS_CALCITE_RULE =
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
  ReduceExpressionsRule PROJECT_REDUCE_EXPRESSIONS_CALCITE_RULE =
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
  ReduceExpressionsRule CALC_REDUCE_EXPRESSIONS_CALCITE_RULE =
    ReduceExpressionsRule.CalcReduceExpressionsRule.Config.DEFAULT
      .withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .as(ReduceExpressionsRule.CalcReduceExpressionsRule.Config.class)
      .withOperandFor(LogicalCalc.class)
      .withTreatDynamicCallsAsConstant(true)
      .as(ReduceExpressionsRule.CalcReduceExpressionsRule.Config.class)
      .toRule();

  ReduceExpressionsRule JOIN_REDUCE_EXPRESSIONS_CALCITE_RULE =
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
  FilterMergeCrule FILTER_MERGE_CALCITE_RULE = new FilterMergeCrule(LogicalFilter.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  FilterMergeCrule FILTER_MERGE_DRULE = new FilterMergeCrule(FilterRel.class, DremioRelFactories.LOGICAL_BUILDER);
  ProjectRemoveRule PROJECT_REMOVE_DRULE = new ProjectRemoveRule(DremioRelFactories.LOGICAL_BUILDER);

  /**
   * Planner rule that pushes a {@link Filter} past a
   * {@link org.apache.calcite.rel.core.SetOp}.
   */
  FilterSetOpTransposeRule FILTER_SET_OP_TRANSPOSE_CALCITE_RULE =
    FilterSetOpTransposeRule.Config.DEFAULT.withRelBuilderFactory(DremioRelFactories.CALCITE_LOGICAL_BUILDER)
      .withOperandSupplier(b0 ->
        b0.operand(LogicalFilter.class).oneInput(b1 ->
          b1.operand(SetOp.class).anyInputs()))
      .as(FilterSetOpTransposeRule.Config.class)
      .toRule();

  /**
   * Planner rule that pushes predicates from a Filter into the Join below.
   */
  FilterJoinRule FILTER_INTO_JOIN_CALCITE_RULE = new LogicalFilterJoinRule();

  /**
   * Planner rules that push project expressions past join
   */
  RelOptRule PUSH_PROJECT_PAST_JOIN_RULE =
    new ProjectJoinTransposeRule(
      ProjectRel.class,
      JoinRel.class,
      PushProjector.ExprCondition.TRUE,
      DremioRelFactories.LOGICAL_BUILDER);

  RelOptRule PUSH_PROJECT_INPUT_REF_PAST_JOIN_RULE =
    new ProjectJoinTransposeRule(
      ProjectRel.class,
      JoinRel.class,
      Conditions.PUSH_REX_INPUT_REF,
      DremioRelFactories.LOGICAL_BUILDER);

  RelOptRule PUSH_PROJECT_PAST_JOIN_RULE_WITH_EXPR_JOIN =
    new ProjectJoinTransposeRule(
      ProjectRel.class,
      JoinRel.class,
      new DremioProjectJoinTransposeRule.ProjectJoinExprCondition(),
      DremioRelFactories.LOGICAL_BUILDER);

  RelOptRule PUSH_PROJECT_PAST_JOIN_CALCITE_RULE = DremioProjectJoinTransposeRule.INSTANCE;

  RelOptRule LOGICAL_FILTER_CORRELATE_RULE = new FilterCorrelateRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  RelOptRule REWRITE_PROJECT_TO_FLATTEN_RULE = new RewriteProjectToFlattenRule(LogicalProject.class);


  ProjectPullUpConstTypeCastRule PROJECT_PULLUP_CONST_TYPE_CAST_RULE =
    ProjectPullUpConstTypeCastRule.INSTANCE;
}

final class LogicalFilterJoinRule extends FilterJoinRule {
  public LogicalFilterJoinRule() {
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

final class JoinConditionPushRule extends FilterJoinRule {
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
