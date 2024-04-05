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
package com.dremio.exec.planner.physical.relbuilder;

import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.NestedLoopJoinPrel;
import com.dremio.exec.planner.physical.Prel;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

public class PrelBuilder extends RelBuilder {

  public PrelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  public PrelBuilder nestedLoopJoin(JoinRelType joinType, RexNode condition) {
    RelNode right = build();
    RelNode left = build();
    RelNode join =
        NestedLoopJoinPrel.create(
            cluster,
            left.getTraitSet().plus(RelCollations.EMPTY),
            left,
            right,
            joinType,
            condition);
    push(join);
    return this;
  }

  public PrelBuilder hashJoin(JoinRelType joinRelType, RexNode condition, RexNode extraConditions) {
    RelNode right = build();
    RelNode left = build();
    RelNode join =
        HashJoinPrel.create(
            cluster,
            left.getTraitSet().plus(RelCollations.EMPTY),
            left,
            right,
            condition,
            extraConditions,
            joinRelType,
            false);
    push(join);
    return this;
  }

  // Overriding Return types

  @Override
  public PrelBuilder filter(RexNode... predicates) {
    super.filter(predicates);
    return this;
  }

  @Override
  public PrelBuilder filter(Iterable<? extends RexNode> predicates) {
    super.filter(predicates);
    return this;
  }

  @Override
  public PrelBuilder project(Iterable<? extends RexNode> nodes) {
    super.project(nodes);
    return this;
  }

  @Override
  public PrelBuilder project(Iterable<? extends RexNode> nodes, Iterable<String> fieldNames) {
    super.project(nodes, fieldNames);
    return this;
  }

  @Override
  public PrelBuilder project(
      Iterable<? extends RexNode> nodes, Iterable<String> fieldNames, boolean force) {
    super.project(nodes, fieldNames, force);
    return this;
  }

  @Override
  public PrelBuilder project(RexNode... nodes) {
    super.project(nodes);
    return this;
  }

  @Override
  public PrelBuilder scan(Iterable<String> tableNames) {
    super.scan(tableNames);
    return this;
  }

  @Override
  public PrelBuilder scan(String... tableNames) {
    super.scan(tableNames);
    return this;
  }

  @Override
  public Prel build() {
    return (Prel) super.build();
  }
}
