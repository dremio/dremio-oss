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

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.service.Pointer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Litmus;

/** Dremio version of RelDecorrelator extended from Calcite */
public final class LegacyDremioRelDecorrelator extends LegacyCalciteRelDecorrelator {
  private static final String DECORRELATION_ERROR_MESSAGE = "This query cannot be decorrelated.";

  private boolean isRelPlanning;
  private final RelBuilder relBuilder;

  public LegacyDremioRelDecorrelator(
      CorelMap cm,
      Context context,
      RelBuilder relBuilder,
      boolean forceValueGenerator,
      boolean isRelPlanning) {
    super(cm, context, relBuilder, forceValueGenerator);
    this.isRelPlanning = isRelPlanning;
    this.relBuilder = relBuilder;
  }

  @Override
  protected RelBuilderFactory relBuilderFactory() {
    if (isRelPlanning) {
      return DremioRelFactories.LOGICAL_BUILDER;
    }
    return DremioRelFactories.CALCITE_LOGICAL_BUILDER;
  }

  /**
   * Decorrelates a query.
   *
   * <p>This is the main entry point to {@code DremioRelDecorrelator}.
   *
   * @param rootRel Root node of the query
   * @param forceValueGenerator force value generator to be created when decorrelating filters
   * @param relBuilder Builder for relational expressions
   * @return Equivalent query with all {@link Correlate} instances removed
   */
  public static RelNode decorrelateQuery(
      RelNode rootRel, RelBuilder relBuilder, boolean forceValueGenerator, boolean isRelPlanning) {
    rootRel = FlattenDecorrelator.decorrelate(rootRel, relBuilder);
    final CorelMap corelMap = new CorelMapBuilder().build(rootRel);
    if (!corelMap.hasCorrelation()) {
      return rootRel;
    }

    final RelOptCluster cluster = rootRel.getCluster();
    final LegacyDremioRelDecorrelator decorrelator =
        new LegacyDremioRelDecorrelator(
            corelMap,
            cluster.getPlanner().getContext(),
            relBuilder,
            forceValueGenerator,
            isRelPlanning);
    RelNode newRootRel = decorrelator.removeCorrelationViaRule(rootRel);
    if (!decorrelator.cm.getMapCorToCorRel().isEmpty()) {
      newRootRel = decorrelator.decorrelate(newRootRel);
    }

    return newRootRel;
  }

  public static RelNode decorrelateQuery(
      RelNode rootRel, RelBuilder relBuilder, boolean isRelPlanning) {

    // Check if the correlates can be rewritten into joins.
    rootRel = rewriteCorrelate(rootRel, relBuilder);

    // Try with both forceValueGenerator true and false
    RelNode decorrelateQuery = decorrelateQuery(rootRel, relBuilder, true, isRelPlanning);
    if (correlateCount(decorrelateQuery) != 0) {
      decorrelateQuery = decorrelateQuery(rootRel, relBuilder, false, isRelPlanning);
    }

    if (correlateCount(decorrelateQuery) != 0) {
      throw new DecorrelationException(DECORRELATION_ERROR_MESSAGE);
    }

    return decorrelateQuery;
  }

  /**
   * Rewrites CorrelateRel to a join if the correlation id is not getting used in the right
   * relational tree.
   */
  private static RelNode rewriteCorrelate(RelNode relNode, RelBuilder relBuilder) {
    return relNode.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            if (!(other instanceof Correlate)) {
              return super.visit(other);
            }
            final Correlate correlate = (Correlate) other;
            // if the correlation id is not getting used in the right subtree, rewrite correlate to
            // join.
            if (RelOptUtil.notContainsCorrelation(
                correlate.getRight(), correlate.getCorrelationId(), Litmus.IGNORE)) {
              return super.visit(
                  relBuilder
                      .push(correlate.getLeft())
                      .push(correlate.getRight())
                      .join(correlate.getJoinType(), relBuilder.getRexBuilder().makeLiteral(true))
                      .build());
            } else {
              return super.visit(other);
            }
          }
        });
  }

  @Override
  public Frame decorrelateRel(Values rel, boolean isCorVarDefined) {
    // There are no inputs, so rel does not need to be changed.
    return decorrelateRel((RelNode) rel, isCorVarDefined);
  }

  /**
   * Modified from upstream to add a join condition if the same correlated variable occurs on both
   * sides.
   *
   * @param rel
   * @param isCorVarDefined
   * @return
   */
  @Override
  public Frame decorrelateRel(Join rel, boolean isCorVarDefined) {
    // For SEMI/ANTI join decorrelate it's input directly,
    // because the correlate variables can only be propagated from
    // the left side, which is not supported yet.
    if (!rel.getJoinType().projectsRight()) {
      decorrelateRel((RelNode) rel, isCorVarDefined);
    }

    // Rewrite logic
    // 1. rewrite join condition.
    // 2. map output positions and produce corVars if any.
    final RelNode oldLeft = rel.getLeft();
    final RelNode oldRight = rel.getRight();

    final Frame leftFrame = getInvoke(oldLeft, isCorVarDefined, rel);
    final Frame rightFrame = getInvoke(oldRight, isCorVarDefined, rel);

    if (leftFrame == null || rightFrame == null) {
      // If any input has not been rewritten, do not rewrite this rel.
      return null;
    }

    Sets.SetView<CorDef> intersection =
        Sets.intersection(leftFrame.corDefOutputs.keySet(), rightFrame.corDefOutputs.keySet());

    RexNode condition = decorrelateExpr(currentRel, map, cm, rel.getCondition());

    final RelNode newJoin =
        relBuilder
            .push(leftFrame.r)
            .push(rightFrame.r)
            .join(
                rel.getJoinType(),
                relBuilder.and(
                    ImmutableList.<RexNode>builder()
                        .add(condition)
                        .addAll(
                            intersection.stream()
                                    .map(
                                        cor ->
                                            relBuilder.equals(
                                                relBuilder.field(
                                                    2, 0, leftFrame.corDefOutputs.get(cor)),
                                                relBuilder.field(
                                                    2, 1, rightFrame.corDefOutputs.get(cor))))
                                ::iterator)
                        .build()),
                ImmutableSet.of())
            .build();

    // Create the mapping between the output of the old correlation rel
    // and the new join rel
    Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();

    int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();
    int newLeftFieldCount = leftFrame.r.getRowType().getFieldCount();

    int oldRightFieldCount = oldRight.getRowType().getFieldCount();
    //noinspection AssertWithSideEffects
    assert rel.getRowType().getFieldCount() == oldLeftFieldCount + oldRightFieldCount;

    // Left input positions are not changed.
    mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (int i = 0; i < oldRightFieldCount; i++) {
      mapOldToNewOutputs.put(
          i + oldLeftFieldCount, rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount);
    }

    final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>(leftFrame.corDefOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (Map.Entry<CorDef, Integer> entry : rightFrame.corDefOutputs.entrySet()) {
      // For left joins, we want to pull up the left side fields if possible.
      // If we don't do this, it could possibly yield incorrect results.
      if (rel.getJoinType() == JoinRelType.LEFT && intersection.contains(entry.getKey())) {
        continue;
      }
      corDefOutputs.put(entry.getKey(), entry.getValue() + newLeftFieldCount);
    }
    return register(rel, newJoin, mapOldToNewOutputs, corDefOutputs);
  }

  @Override
  public Frame decorrelateRel(Sort rel, boolean isCorVarDefined) {
    if (isCorVarDefined && (rel.offset != null || rel.fetch != null)) {
      return null;
    }
    return super.decorrelateRel(rel, isCorVarDefined);
  }

  @Override
  public Frame getInvoke(RelNode r, boolean isCorVarDefined, RelNode parent) {
    final Frame frame = doInvoke(r, isCorVarDefined);
    currentRel = parent;
    if (frame != null) {
      map.put(r, frame);
    }
    currentRel = parent;
    return frame;
  }

  private Frame doInvoke(RelNode r, boolean isCorVarDefined) {
    if (r instanceof Aggregate) {
      return decorrelateRel((Aggregate) r, isCorVarDefined);
    } else if (r instanceof Correlate) {
      return decorrelateRel((Correlate) r, isCorVarDefined);
    } else if (r instanceof Filter) {
      return decorrelateRel((Filter) r, isCorVarDefined);
    } else if (r instanceof Join) {
      return decorrelateRel((Join) r, isCorVarDefined);
    } else if (r instanceof Project) {
      return decorrelateRel((Project) r, isCorVarDefined);
    } else if (r instanceof TableScan) {
      return decorrelateRel(r, isCorVarDefined);
    } else if (r instanceof Sort) {
      return decorrelateRel((Sort) r, isCorVarDefined);
    } else if (r instanceof Values) {
      return decorrelateRel((Values) r, isCorVarDefined);
    } else {
      return dispatcher.invoke(r, isCorVarDefined);
    }
  }

  public static int correlateCount(RelNode rel) {
    final Pointer<Integer> count = new Pointer<>(0);
    rel.accept(
        new StatelessRelShuttleImpl() {
          @Override
          public RelNode visit(RelNode other) {
            if (other instanceof Correlate) {
              count.value++;
            }
            return super.visit(other);
          }

          @Override
          public RelNode visit(LogicalCorrelate correlate) {
            count.value++;
            return super.visit(correlate);
          }
        });
    return count.value;
  }
}
