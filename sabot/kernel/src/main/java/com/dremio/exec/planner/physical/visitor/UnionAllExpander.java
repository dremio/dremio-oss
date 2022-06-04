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
package com.dremio.exec.planner.physical.visitor;

import static com.dremio.exec.planner.physical.DistributionTrait.ROUND_ROBIN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelConversionException;

import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.EmptyPrel;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.HasDistributionAffinity;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.RoundRobinExchangePrel;
import com.dremio.exec.planner.physical.UnionAllPrel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * A visitor that expands the merged union based on the following logic:
 *   1) Remove all empty inputs for a given union
 *   2) If all inputs are empty, replace the union with empty,
 *   3) If a single input is not empty, replace the union with project
 *   4) If there are more than two non-empty inputs,
 *      based on the width of each child, incrementally create binary
 *      UnionAlls and insert RoundRobinExchange between them
 *   5) If certain child's width is smaller than the defined
 *      threshold value, or the width difference between two inputs
 *      are bigger than the defined ratio,
 *      additionally add RoundRobinExchange to the smaller side
 */
public final class UnionAllExpander extends BasePrelVisitor<Prel, Void, IOException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionAllExpander.class);

  private final long targetSliceSize;
  private final SqlHandlerConfig config;
  private int count;

  private UnionAllExpander(SqlHandlerConfig config, long targetSliceSize) {
    this.targetSliceSize = targetSliceSize;
    this.config = config;
    this.count = 0;
  }

  public static Prel expandUnionAlls(Prel prel, SqlHandlerConfig config, long targetSliceSize) throws RelConversionException {
    UnionAllExpander exchange = new UnionAllExpander(config, targetSliceSize);
    try {
      Prel expanded = prel.accept(exchange, null);
      expanded.accept(new UnionAllInputValidator(), null);
      return expanded;
    } catch (IOException ex) {
      throw new RelConversionException("Failure while attempting to expanding UnionAlls.", ex);
    }
  }

  private long getInputRoundRobinThreshold() {
    return config.getContext().getOptions().getOption(PlannerSettings.UNION_ALL_INPUT_ROUND_ROBIN_THRESHOLD_VALUE);
  }

  private double getInputRoundRobinRatio() {
    return config.getContext().getOptions().getOption(PlannerSettings.UNION_ALL_INPUT_ROUND_ROBIN_THRESHOLD_RATIO);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws IOException {
    // convert inputs
    final List<RelNode> children = new ArrayList<>();
    for (Prel p : prel) {
      children.add(p.accept(this, null));
    }

    if (!(prel instanceof UnionAllPrel)) {
      return (Prel) prel.copy(prel.getTraitSet(), children);
    }

    BatchSchema batchSchema = null;
    // populate priority queue based on width of inputs
    PriorityQueue<UnionChild> pq = new PriorityQueue<>(children.size());
    for (RelNode child : children) {
      Prel childPrel = (Prel) child;
      final UnionChild unionChild = new UnionChild(
        WidthFinder.computeStat(childPrel, targetSliceSize),
        childPrel, count++);
      pq.add(unionChild);
    }

    if (pq.size() < 2) {
      Prel inputRel = pq.isEmpty() ?
        new EmptyPrel(prel.getCluster(), prel.getTraitSet(), prel.getRowType(), batchSchema) : addRoundRobinIfStrict(pq.poll().relNode);
      final RexBuilder rexBuilder = prel.getCluster().getRexBuilder();
      final List<RelDataTypeField> unionFields = prel.getRowType().getFieldList();
      final List<RelDataTypeField> inputFields = inputRel.getRowType().getFieldList();
      final List<RexNode> projects = new ArrayList<>();
      for (int i = 0 ; i < unionFields.size() ; i++) {
        projects.add(rexBuilder.makeCast(unionFields.get(i).getType(), new RexInputRef(i, inputFields.get(i).getType())));
      }
      return ProjectPrel.create(prel.getCluster(), prel.getTraitSet(), inputRel, projects, prel.getRowType());
    }

    // construct unions with RoundRobinExchanges
    UnionChild left = pq.poll();
    UnionChild right = pq.poll();
    Prel convertedLeft = ((double) left.stat.getMaxWidth()) <= right.stat.getMaxWidth() * getInputRoundRobinRatio() ?
      addRoundRobin(left.relNode) : addRoundRobinIfStrict(left);
    Prel convertedRight = addRoundRobinIfStrict(right);
    boolean isSingular = left.stat.isSingular() && right.stat.isSingular() && !left.stat.isDistributionStrict()&& !right.stat.isDistributionStrict();

    try {
      Prel union = new UnionAllPrel(
        prel.getCluster(), prel.getTraitSet(),
        ImmutableList.of(convertedLeft, convertedRight),
        false);

      while (!pq.isEmpty()) {
        UnionChild newInput = pq.poll();
        isSingular = isSingular && newInput.stat.isSingular() && !newInput.stat.isDistributionStrict();
        union = !isSingular ? addRoundRobin(union) : union;
        Prel convertedNewInput = shouldAddRoundRobinToNewInput(isSingular, newInput.stat) ? addRoundRobin(newInput.relNode) : addRoundRobinIfStrict(newInput);
        union = new UnionAllPrel(prel.getCluster(), prel.getTraitSet(), ImmutableList.of(union, convertedNewInput), false);
      }
      return union;
    } catch (InvalidRelException ex) {
      // This exception should not be thrown as we already checked compatibility
      logger.warn("Failed to expand unionAll as inputs are not compatible", ex);
      return prel;
    }
  }

  private boolean shouldAddRoundRobinToNewInput(boolean isSingular, FragmentStatVisitor.MajorFragmentStat inputStat) {
    return !isSingular && (inputStat.isSingular() || inputStat.getMaxWidth() <= getInputRoundRobinThreshold());
  }

  private Prel addRoundRobin(Prel prel) {
    return new RoundRobinExchangePrel(prel.getCluster(), createEmptyTraitSet().plus(Prel.PHYSICAL).plus(ROUND_ROBIN), prel);
  }

  private Prel addRoundRobinIfStrict(UnionChild child) {
    return child.stat.isDistributionStrict() ? addRoundRobin(child.relNode) : child.relNode;
  }

  private Prel addRoundRobinIfStrict(Prel prel) {
    return findHardAffinity(prel) ? addRoundRobin(prel) : prel;
  }

  public RelTraitSet createEmptyTraitSet() {
    return RelTraitSet.createEmpty().plus(Convention.NONE).plus(DistributionTrait.DEFAULT).plus(RelCollations.EMPTY);
  }

  private static class WidthFinder extends FragmentStatVisitor {
    public WidthFinder(long targetSliceSize) {
      super(targetSliceSize);
    }

    public static MajorFragmentStat computeStat(Prel prel, long targetSliceSize) {
      final WidthFinder widthFinder = new WidthFinder(targetSliceSize);
      final MajorFragmentStat stat = widthFinder.getNewStat();
      prel.accept(widthFinder, stat);
      return stat;
    }

    @Override
    public Prel visitExchange(ExchangePrel prel, MajorFragmentStat parent) throws RuntimeException {
      parent.add(prel);
      MajorFragmentStat newFrag = new MajorFragmentStat();
      Prel newChild = ((Prel) prel.getInput()).accept(this, newFrag);

      if (newFrag.isSingular() && parent.isSingular() &&
        (!newFrag.isDistributionStrict() && !parent.isDistributionStrict())) {
        parent.merge(newFrag);
      }
      return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList((RelNode) newChild));
    }
  }

  private static final class UnionChild implements Comparable<UnionChild> {
    private final FragmentStatVisitor.MajorFragmentStat stat;
    private final Prel relNode;
    private final int count;

    public UnionChild(FragmentStatVisitor.MajorFragmentStat stat, Prel relNode, int count) {
      this.stat = stat;
      this.relNode = relNode;
      this.count = count;
    }

    @Override
    public int compareTo(UnionChild another) {
      int cmp = Integer.compare(this.stat.getMaxWidth(), another.stat.getMaxWidth());
      return cmp != 0 ? cmp : Integer.compare(this.count, another.count);
    }
  }
  private static boolean findHardAffinity(Prel prel) {
    if (prel instanceof HasDistributionAffinity) {
      return ((HasDistributionAffinity) prel).getDistributionAffinity() == DistributionAffinity.HARD;
    } else if (prel instanceof GroupScan) {
      return ((GroupScan) prel).getDistributionAffinity() == DistributionAffinity.HARD;
    } else if(prel.getInputs().size() == 1) {
      return findHardAffinity(Iterables.getOnlyElement(prel));
    } else {
      return false;
    }
  }

  private static class UnionAllInputValidator extends BasePrelVisitor<Prel, Void, IOException> {
    @Override
    public Prel visitPrel(Prel prel, Void value) throws IOException {
      List<RelNode> children = new ArrayList<>();
      for (Prel child : prel) {
        children.add(child.accept(this, null));
      }
      if (prel instanceof UnionAllPrel && children.size() != 2) {
        throw new IOException("UnionAll operators must have only 2 inputs");
      }
      return (Prel) prel.copy(prel.getTraitSet(), children);
    }
  }
}
