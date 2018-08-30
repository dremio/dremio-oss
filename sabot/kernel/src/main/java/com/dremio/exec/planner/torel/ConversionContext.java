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
package com.dremio.exec.planner.torel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.LogicalPlan;
import com.dremio.common.logical.data.Filter;
import com.dremio.common.logical.data.GroupingAggregate;
import com.dremio.common.logical.data.Join;
import com.dremio.common.logical.data.Limit;
import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.common.logical.data.Order;
import com.dremio.common.logical.data.Project;
import com.dremio.common.logical.data.Scan;
import com.dremio.common.logical.data.Union;
import com.dremio.common.logical.data.visitors.AbstractLogicalVisitor;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.LimitRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.SortRel;
import com.dremio.exec.planner.logical.UnionRel;

public class ConversionContext implements ToRelContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConversionContext.class);

  private static final ConverterVisitor VISITOR = new ConverterVisitor();

  private final RelOptCluster cluster;

  public ConversionContext(RelOptCluster cluster, LogicalPlan plan) {
    super();
    this.cluster = cluster;
  }

  @Override
  public RelOptCluster getCluster() {
    return cluster;
  }

  public RexBuilder getRexBuilder(){
    return cluster.getRexBuilder();
  }

  public RelTraitSet getLogicalTraits(){
    RelTraitSet set = RelTraitSet.createEmpty();
    set.add(Rel.LOGICAL);
    return set;
  }

  public RelNode toRel(LogicalOperator operator) throws InvalidRelException{
    return operator.accept(VISITOR, this);
  }

  public RexNode toRex(LogicalExpression e){
    return null;
  }

  public RelDataTypeFactory getTypeFactory(){
    return cluster.getTypeFactory();
  }

  public RelOptTable getTable(Scan scan){
    return null;
  }

  @Override
  public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
    throw new UnsupportedOperationException();
  }

  private static class ConverterVisitor extends AbstractLogicalVisitor<RelNode, ConversionContext, InvalidRelException>{

    @Override
    public RelNode visitScan(Scan scan, ConversionContext context){
      //return BaseScanRel.convert(scan, context);
      return null;
    }

    @Override
    public RelNode visitFilter(Filter filter, ConversionContext context) throws InvalidRelException{
      //return BaseFilterRel.convert(filter, context);
      return null;
    }

    @Override
    public RelNode visitProject(Project project, ConversionContext context) throws InvalidRelException{
      //return BaseProjectRel.convert(project, context);
      return null;
    }

    @Override
    public RelNode visitOrder(Order order, ConversionContext context) throws InvalidRelException{
      return SortRel.convert(order, context);
    }

    @Override
    public RelNode visitJoin(Join join, ConversionContext context) throws InvalidRelException{
      return JoinRel.convert(join, context);
    }

    @Override
    public RelNode visitLimit(Limit limit, ConversionContext context) throws InvalidRelException{
      return LimitRel.convert(limit, context);
    }

    @Override
    public RelNode visitUnion(Union union, ConversionContext context) throws InvalidRelException{
      return UnionRel.convert(union, context);
    }

    @Override
    public RelNode visitGroupingAggregate(GroupingAggregate groupBy, ConversionContext context)
        throws InvalidRelException {
      return AggregateRel.convert(groupBy, context);
    }

  }



}
