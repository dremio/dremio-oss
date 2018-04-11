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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.logical.data.Project;
import com.dremio.exec.planner.common.ProjectRelBase;
import com.dremio.exec.planner.torel.ConversionContext;
import com.google.common.collect.Lists;

/**
 * Project implemented in Dremio.
 */
public class ProjectRel extends ProjectRelBase implements Rel {

  private final boolean hasFlattenFields;
  private final boolean canPushPastFlatten;

  protected ProjectRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<? extends RexNode> exps, RelDataType rowType) {
    this(cluster, traits, child, exps, rowType, true);
  }

  protected ProjectRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<? extends RexNode> exps, RelDataType rowType, boolean canPushPastFlatten) {
    super(LOGICAL, cluster, traits, child, exps, rowType);
    this.canPushPastFlatten = canPushPastFlatten;
    this.hasFlattenFields = FlattenVisitors.hasFlatten(this);
  }

  public boolean canPushPastFlatten() {
    return canPushPastFlatten;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (hasFlattenFields) {
      return planner.getCostFactory().makeInfiniteCost();
    } else {
      return super.computeSelfCost(planner, mq);
    }
  }

  @Override
  public org.apache.calcite.rel.core.Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> exps, RelDataType rowType) {
    return new ProjectRel(this.getCluster(), traitSet, input, exps, rowType, this.canPushPastFlatten());
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    LogicalOperator inputOp = implementor.visitChild(this, 0, getInput());
    Project.Builder builder = Project.builder();
    builder.setInput(inputOp);
    for (NamedExpression e: this.getProjectExpressions(implementor.getContext())) {
      builder.addExpr(e);
    }
    return builder.build();
  }

  public static ProjectRel convert(Project project, ConversionContext context) throws InvalidRelException{
    RelNode input = context.toRel(project.getInput());
    List<RelDataTypeField> fields = Lists.newArrayList();
    List<RexNode> exps = Lists.newArrayList();
    for(NamedExpression expr : project.getSelections()){
      fields.add(new RelDataTypeFieldImpl(expr.getRef().getRootSegment().getPath(), fields.size(), context.getTypeFactory().createSqlType(SqlTypeName.ANY) ));
      exps.add(context.toRex(expr.getExpr()));
    }
    return new ProjectRel(context.getCluster(), context.getLogicalTraits(), input, exps, new RelRecordType(fields));
  }

  /** provide a public method to create an instance of ProjectRel.
   *
   * @param cluster
   * @param traits
   * @param child
   * @param exps
   * @param rowType
   * @return new instance of ProjectRel
   */
  public static ProjectRel create(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<? extends RexNode> exps,
                                       RelDataType rowType) {
    return new ProjectRel(cluster, traits, child, exps, rowType);
  }

}
