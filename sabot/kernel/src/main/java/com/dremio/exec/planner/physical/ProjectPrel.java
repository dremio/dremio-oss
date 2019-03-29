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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ProjectRelBase;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionType;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class ProjectPrel extends ProjectRelBase implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectPrel.class);


  protected ProjectPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps,
      RelDataType rowType) {
    super(PHYSICAL, cluster, traits, child, exps, rowType);
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> exps, RelDataType rowType) {
    return ProjectPrel.create(getCluster(), traitSet, input, exps, rowType);
  }


  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    com.dremio.exec.physical.config.Project p = new com.dremio.exec.physical.config.Project(
        this.getProjectExpressions(new ParseContext(PrelUtil.getSettings(getCluster()))), childPOP);
    return creator.addMetadata(this, p);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitProject(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }

  /**
   * Creates an instance of ProjectPrel.
   *
   * @param cluster
   * @param traits
   * @param child
   * @param exps
   * @param rowType
   * @return new instance of ProjectPrel
   */
  public static ProjectPrel create(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps,
                                   RelDataType rowType) {
    final RelTraitSet trimmedTraits = trimTraits(cluster, child, exps, traits);
    return new ProjectPrel(cluster, trimmedTraits, child, exps, rowType);
  }

  protected static RelTraitSet trimTraits(RelOptCluster cluster, RelNode input, List<? extends RexNode> exps, RelTraitSet traits) {
    return ProjectRelBase.trimTraits(cluster, input, exps, traits)
        .replaceIf(DistributionTraitDef.INSTANCE, () -> {
          final PlannerSettings settings = PrelUtil.getPlannerSettings(cluster.getPlanner());
          if (!settings.shouldPullDistributionTrait()) {
            // Do not change distribution trait (even if not valid from a planner point of view)
            return traits.getTrait(DistributionTraitDef.INSTANCE);
          }

          final DistributionTrait inputDistribution = input.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
          final Map<Integer, Integer> index = new HashMap<>();

          for (Ord<? extends RexNode> exp : Ord.zip(exps)) {
            // For distribution, either $0 or cast($0 as ...) would keep the distribution after projection.
            if (exp.e instanceof RexInputRef) {
              index.put(((RexInputRef) exp.e).getIndex(), exp.i);
            } else if (exp.e.isA(SqlKind.CAST)) {
              RexNode operand = ((RexCall) exp.e).getOperands().get(0);
              if (operand instanceof RexInputRef) {
                index.put(((RexInputRef) operand).getIndex(), exp.i);
              }
            }
          }
          List<DistributionField> newFields = Lists.newArrayList();
          for (DistributionField field : inputDistribution.getFields()) {
            if (index.containsKey(field.getFieldId())) {
              newFields.add(new DistributionField(index.get(field.getFieldId())));
            }
          }

          // After the projection, if the new distribution fields is empty, or new distribution fields is a subset of
          // original distribution field, we should replace with either SINGLETON or RANDOM_DISTRIBUTED.
          if (newFields.isEmpty() || newFields.size() < inputDistribution.getFields().size()) {
            if (inputDistribution.getType() != DistributionType.SINGLETON) {
              return DistributionTrait.ANY;
            } else {
              return DistributionTrait.SINGLETON;
            }
          } else {
            return new DistributionTrait(inputDistribution.getType(), ImmutableList.copyOf(newFields));
          }
        });
  }


}
