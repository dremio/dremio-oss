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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class ProjectPrule extends Prule {
  public static final RelOptRule INSTANCE = new ProjectPrule();

  private ProjectPrule() {
    super(RelOptHelper.some(ProjectRel.class, RelOptHelper.any(RelNode.class)), "ProjectPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectRel project = (ProjectRel) call.rel(0);
    final RelNode input = project.getInput();

    RelTraitSet traits = input.getTraitSet().plus(Prel.PHYSICAL);
    RelNode convertedInput = convert(input, traits);

    // Maintain two different map for distribution trait and collation trait.
    // For now, the only difference comes from the way how cast function impacts propagating trait.
    final Map<Integer, Integer> distributionMap = getDistributionMap(project);
    final Map<Integer, Integer> collationMap = getCollationMap(project);

    boolean traitPull = new ProjectTraitPull(call, distributionMap, collationMap).go(project, convertedInput);

    if(!traitPull){
      call.transformTo(new ProjectPrel(project.getCluster(), convertedInput.getTraitSet(), convertedInput, project.getProjects(), project.getRowType()));
    }
  }

  private class ProjectTraitPull extends SubsetTransformer<ProjectRel, RuntimeException> {
    final Map<Integer, Integer> distributionMap;
    final Map<Integer, Integer> collationMap;

    public ProjectTraitPull(RelOptRuleCall call, Map<Integer, Integer> distributionMap, Map<Integer, Integer> collationMap) {
      super(call);
      this.distributionMap = distributionMap;
      this.collationMap = collationMap;
    }

    @Override
    public RelNode convertChild(ProjectRel project, RelNode rel) throws RuntimeException {
      DistributionTrait childDist = rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
      RelCollation childCollation = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);


      DistributionTrait newDist = convertDist(childDist, distributionMap);
      RelCollation newCollation = convertRelCollation(childCollation, collationMap);
      RelTraitSet newProjectTraits = newTraitSet(Prel.PHYSICAL, newDist, newCollation);
      return new ProjectPrel(project.getCluster(), newProjectTraits, rel, project.getProjects(), project.getRowType());
    }

  }

  private DistributionTrait convertDist(DistributionTrait srcDist, Map<Integer, Integer> inToOut) {
    List<DistributionField> newFields = Lists.newArrayList();

    for (DistributionField field : srcDist.getFields()) {
      if (inToOut.containsKey(field.getFieldId())) {
        newFields.add(new DistributionField(inToOut.get(field.getFieldId())));
      }
    }

    // After the projection, if the new distribution fields is empty, or new distribution fields is a subset of
    // original distribution field, we should replace with either SINGLETON or RANDOM_DISTRIBUTED.
    if (newFields.isEmpty() || newFields.size() < srcDist.getFields().size()) {
      if (srcDist.getType() != DistributionType.SINGLETON) {
        return DistributionTrait.ANY;
      } else {
        return DistributionTrait.SINGLETON;
      }
    } else {
      return new DistributionTrait(srcDist.getType(), ImmutableList.copyOf(newFields));
    }
  }

  private RelCollation convertRelCollation(RelCollation src, Map<Integer, Integer> inToOut) {
    List<RelFieldCollation> newFields = Lists.newArrayList();

    for ( RelFieldCollation field : src.getFieldCollations()) {
      if (inToOut.containsKey(field.getFieldIndex())) {
        newFields.add(new RelFieldCollation(inToOut.get(field.getFieldIndex()), field.getDirection(), field.nullDirection));
      }
    }

    if (newFields.isEmpty()) {
      return RelCollationImpl.of();
    } else {
      return RelCollationImpl.of(newFields);
    }
  }

  private Map<Integer, Integer> getDistributionMap(ProjectRel project) {
    Map<Integer, Integer> m = new HashMap<Integer, Integer>();

    for (Ord<RexNode> node : Ord.zip(project.getProjects())) {
      // For distribution, either $0 or cast($0 as ...) would keep the distribution after projection.
      if (node.e instanceof RexInputRef) {
        m.put( ((RexInputRef) node.e).getIndex(), node.i);
      } else if (node.e.isA(SqlKind.CAST)) {
        RexNode operand = ((RexCall) node.e).getOperands().get(0);
        if (operand instanceof RexInputRef) {
          m.put(((RexInputRef) operand).getIndex(), node.i);
        }
      }
    }
    return m;

  }

  private Map<Integer, Integer> getCollationMap(ProjectRel project) {
    Map<Integer, Integer> m = new HashMap<Integer, Integer>();

    for (Ord<RexNode> node : Ord.zip(project.getProjects())) {
      // For collation, only $0 will keep the sort-ness after projection.
      if (node.e instanceof RexInputRef) {
        m.put( ((RexInputRef) node.e).getIndex(), node.i);
      }
    }
    return m;

  }

}
