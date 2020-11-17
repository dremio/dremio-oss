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
package com.dremio.exec.planner.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.dremio.exec.planner.sql.SqlFlattenOperator;
import com.google.common.collect.Lists;

public class PushFilterPastProjectRule extends RelOptRule {

  public final static RelOptRule INSTANCE = new PushFilterPastProjectRule(
      FilterRel.class, ProjectRel.class, RelNode.class, DremioRelFactories.LOGICAL_PROPAGATE_BUILDER);

  public final static RelOptRule CALCITE_INSTANCE = new PushFilterPastProjectRule(
      LogicalFilter.class, LogicalProject.class, RelNode.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  public final static RelOptRule CALCITE_NO_CHILD_CHECK = new PushFilterPastProjectRule(
      LogicalFilter.class, LogicalProject.class, null, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  private final boolean checkChild;

  /**
   * Determine if the expression qualifies for push down.
   *
   * @param filterExpr filter expression
   * @param projExprs project expressions
   * @return true iff the expression qualifies
   */
  private boolean qualifies(final RexNode filterExpr, final List<RexNode> projExprs) {

    // block uses "throw Util.FoundOne" to terminate expression tree search
    try {
      // 1. filter-window transpose is not valid since input row count changes
      for (RexNode projExpr : projExprs) {
        projExpr.accept(new WindowFunctionFinder());
      }

      // 2. filter-project transpose is not valid if:
      // 2.a. filter has a ITEM or FLATTEN operator
      // 2.b. filter references a field in project that has ITEM, FLATTEN or sub-query
      final RexVisitor<Void> filterVisitor =
          new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitCall(RexCall call) {
              if (SqlStdOperatorTable.ITEM.equals(call.getOperator()) ||
                  SqlFlattenOperator.INSTANCE.getName().toLowerCase().equals(call.getOperator().getName().toLowerCase())) {
                throw new Util.FoundOne(call);
              }
              return super.visitCall(call);
            }

            @Override
            public Void visitInputRef(RexInputRef inputRef) {
              final int index = inputRef.getIndex();
              final RexNode projExpr = projExprs.get(index);

              projExpr.accept(new UnsupportedProjectExprFinder());

              return super.visitInputRef(inputRef);
            }

            @Override
            public Void visitFieldAccess(RexFieldAccess fieldAccess) {
              fieldAccess.getReferenceExpr().accept(this);
              return super.visitFieldAccess(fieldAccess);
            }
          };

      filterExpr.accept(filterVisitor);
      return true;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return false;
    }
  }

  /**
   * Push filter past project.  Only push filter past project if the projects underneath are collapsed into
   * a single project.  Don't create a long chain of projects!
   *
   * @param filterClass filter class
   * @param projectClass project class
   * @param relBuilderFactory rel builder factory
   */
  private PushFilterPastProjectRule(
      Class<? extends Filter> filterClass,
      Class<? extends Project> projectClass,
      Class<? extends RelNode> childClass,
      RelBuilderFactory relBuilderFactory) {
    super(childClass == null ?
        operand(filterClass, operand(projectClass, any())) :
        operand(filterClass, operand(projectClass, operand(childClass, any()))),
        relBuilderFactory, "PushFilterPastProjectRule");
    this.checkChild = childClass != null;
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    if(!checkChild) {
      return true;
    }
    // Make sure that we do not push filter past project onto things that it can't go past (filter/aggregate).
    // Also, do not push filter past project if there is yet another project.  Just make the projects merge first.
    return !(call.rel(2) instanceof Project || call.rel(2) instanceof Aggregate || call.rel(2) instanceof Values);
  }


  // implement RelOptRule
  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filterRel = call.rel(0);
    Project projRel = call.rel(1);

    // get a conjunctions of the filter condition. For each conjunction, if it refers to ITEM or FLATTEN expression
    // then we could not pushed down. Otherwise, it's qualified to be pushed down.
    final List<RexNode> predList = RelOptUtil.conjunctions(filterRel.getCondition());

    final List<RexNode> qualifiedPredList = Lists.newArrayList();
    final List<RexNode> unqualifiedPredList = Lists.newArrayList();


    for (final RexNode pred : predList) {
      if (qualifies(pred, projRel.getProjects())) {
        qualifiedPredList.add(pred);
      } else {
        unqualifiedPredList.add(pred);
      }
    }

    final RexNode qualifedPred = RexUtil.composeConjunction(filterRel.getCluster().getRexBuilder(), qualifiedPredList, true);

    if (qualifedPred == null) {
      return;
    }

    // convert the filter to one that references the child of the project
    RexNode newCondition =
        RelOptUtil.pushPastProject(qualifedPred, projRel);

    RelBuilder relBuilder = relBuilderFactory.create(filterRel.getCluster(), null);
    relBuilder.push(projRel.getInput());
    relBuilder.filter(newCondition);
    relBuilder.project(Pair.left(projRel.getNamedProjects()), Pair.right(projRel.getNamedProjects()));

    final RexNode unqualifiedPred = RexUtil.composeConjunction(filterRel.getCluster().getRexBuilder(), unqualifiedPredList, true);

    if (unqualifiedPred == null) {
      call.transformTo(relBuilder.build());
    } else {
      // if there are filters not qualified to be pushed down, then we have to put those filters on top of
      // the new Project operator.
      // Filter -- unqualified filters
      //   \
      //    Project
      //     \
      //      Filter  -- qualified filters
      relBuilder.filter(unqualifiedPred);
      call.transformTo(relBuilder.build());
    }
  }

  private static class WindowFunctionFinder extends RexVisitorImpl<Void> {
    WindowFunctionFinder() {
      super(true);
    }

    @Override
    public Void visitOver(RexOver over) {
      throw new Util.FoundOne(over);
    }
  }

  private static class UnsupportedProjectExprFinder extends RexVisitorImpl<Void> {
    UnsupportedProjectExprFinder() {
      super(true);
    }

    @Override
    public Void visitCall(RexCall call) {
      if (SqlStdOperatorTable.ITEM.equals(call.getOperator()) ||
          SqlFlattenOperator.INSTANCE.getName().toLowerCase().equals(call.getOperator().getName().toLowerCase())) {
        throw new Util.FoundOne(call);
      }
      return super.visitCall(call);
    }

    @Override
    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      throw new Util.FoundOne(fieldAccess);
    }

    @Override
    public Void visitSubQuery(RexSubQuery subQuery) {
      throw new Util.FoundOne(subQuery);
    }
  }
}
