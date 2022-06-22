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

package com.dremio.exec.physical.config;

import java.util.Iterator;
import java.util.List;

import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

@JsonTypeName("nested-loop-join")
public class NestedLoopJoinPOP extends AbstractBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NestedLoopJoinPOP.class);

  private final PhysicalOperator build;
  private final PhysicalOperator probe;
  private final JoinRelType joinType;
  private final LogicalExpression condition;
  private final boolean vectorized;
  private final LogicalExpression vectorOp;

  @JsonCreator
  public NestedLoopJoinPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("probe") PhysicalOperator probe,
      @JsonProperty("build") PhysicalOperator build,
      @JsonProperty("joinType") JoinRelType joinType,
      @JsonProperty("condition") LogicalExpression condition,
      @JsonProperty("vectorized") boolean vectorized,
      @JsonProperty("vectorOp") LogicalExpression vectorOp) {
    super(props);
    this.probe = probe;
    this.build = build;
    this.joinType = joinType;
    this.condition = condition;
    this.vectorized = vectorized;
    this.vectorOp = vectorOp;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitNestedLoopJoin(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.size() == 2);
    return new NestedLoopJoinPOP(props, children.get(0), children.get(1), joinType, condition, vectorized, vectorOp);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.forArray(probe, build);
  }

  public PhysicalOperator getProbe() {
    return probe;
  }

  public PhysicalOperator getBuild() {
    return build;
  }

  public JoinRelType getJoinType() {
    return joinType;
  }

  public LogicalExpression getCondition() {
    return condition;
  }

  public boolean isVectorized() {
    return vectorized;
  }

  public LogicalExpression getVectorOp() {
    return vectorOp;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.NESTED_LOOP_JOIN_VALUE;
  }
}
