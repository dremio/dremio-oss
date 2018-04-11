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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.Lists;

public class PushFilterPastProjectRule extends RelOptRule {

  public final static RelOptRule INSTANCE = new PushFilterPastProjectRule(
      FilterRel.class, ProjectRel.class, DremioRelFactories.LOGICAL_PROPAGATE_BUILDER);
  public final static RelOptRule CALCITE_INSTANCE = new PushFilterPastProjectRule(
      LogicalFilter.class, LogicalProject.class, DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  private RexCall findItemOrFlatten(
      final RexNode node,
      final List<RexNode> projExprs) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
        @Override
        public Void visitCall(RexCall call) {
          if ("item".equals(call.getOperator().getName().toLowerCase()) ||
            "flatten".equals(call.getOperator().getName().toLowerCase())) {
            throw new Util.FoundOne(call); /* throw exception to interrupt tree walk (this is similar to
                                              other utility methods in RexUtil.java */
          }
          return super.visitCall(call);
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
          final int index = inputRef.getIndex();
          RexNode n = projExprs.get(index);
          if (n instanceof RexCall) {
            RexCall r = (RexCall) n;
            if ("item".equals(r.getOperator().getName().toLowerCase()) ||
                "flatten".equals(r.getOperator().getName().toLowerCase())) {
              throw new Util.FoundOne(r);
            }
          }

          return super.visitInputRef(inputRef);
        }
      };
      node.accept(visitor);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (RexCall) e.getNode();
    }
  }

  /**
   * Push filter past project.  Only push filter past project if the projects underneath are collapsed into
   * a single project.  Don't create a long chain of projects!
   * @param filterClass
   * @param projectClass
   * @param relBuilderFactory
   */
  protected PushFilterPastProjectRule(Class<? extends Filter> filterClass,
                                           Class<? extends Project> projectClass,
                                           RelBuilderFactory relBuilderFactory) {
    super(
        operand(filterClass, operand(projectClass, operand(RelNode.class, any()))), relBuilderFactory, "PushFilterPastProjectRule");
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
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
      if (findItemOrFlatten(pred, projRel.getProjects()) == null) {
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

}
