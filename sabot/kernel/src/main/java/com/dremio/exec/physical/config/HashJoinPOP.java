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

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.JoinRelType;

@JsonTypeName("hash-join")
public class HashJoinPOP extends AbstractBase {

  private final PhysicalOperator left;
  private final PhysicalOperator right;
  private final List<JoinCondition> conditions;
  private final LogicalExpression extraCondition;
  private final JoinRelType joinType;
  private final boolean vectorize;

  private final boolean spill;
  private RuntimeFilterInfo runtimeFilterInfo;

  @JsonCreator
  public HashJoinPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("left") PhysicalOperator left,
      @JsonProperty("right") PhysicalOperator right,
      @JsonProperty("conditions") List<JoinCondition> conditions,
      @JsonProperty("extraCondition") LogicalExpression extraCondition,
      @JsonProperty("joinType") JoinRelType joinType,
      @JsonProperty("vectorize") boolean vectorize,
      @JsonProperty("spill") boolean spill,
      @JsonProperty("runtimeFilterInfo") RuntimeFilterInfo runtimeFilterInfo) {
    super(props);
    this.left = left;
    this.right = right;
    this.conditions = conditions;
    this.extraCondition = extraCondition;
    this.joinType = joinType;
    this.vectorize = vectorize;
    this.spill = spill;
    this.runtimeFilterInfo = runtimeFilterInfo;
  }

  public HashJoinPOP(
      OpProps props,
      PhysicalOperator left,
      PhysicalOperator right,
      List<JoinCondition> conditions,
      LogicalExpression extraCondition,
      JoinRelType joinType,
      boolean vectorize,
      RuntimeFilterInfo runtimeFilterInfo) {
    super(props);
    this.left = left;
    this.right = right;
    this.conditions = conditions;
    this.extraCondition = extraCondition;
    this.joinType = joinType;
    this.vectorize = vectorize;
    this.spill = false;
    this.runtimeFilterInfo = runtimeFilterInfo;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitHashJoin(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.size() == 2);
    return new HashJoinPOP(
        props,
        children.get(0),
        children.get(1),
        conditions,
        extraCondition,
        joinType,
        vectorize,
        spill,
        runtimeFilterInfo);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.forArray(left, right);
  }

  public PhysicalOperator getLeft() {
    return left;
  }

  public PhysicalOperator getRight() {
    return right;
  }

  public JoinRelType getJoinType() {
    return joinType;
  }

  public List<JoinCondition> getConditions() {
    return conditions;
  }

  public LogicalExpression getExtraCondition() {
    return extraCondition;
  }

  public boolean isVectorize() {
    return vectorize;
  }

  public boolean isSpill() {
    return spill;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HASH_JOIN_VALUE;
  }

  public RuntimeFilterInfo getRuntimeFilterInfo() {
    return runtimeFilterInfo;
  }

  public void setRuntimeFilterInfo(RuntimeFilterInfo runtimeFilterInfo) {
    this.runtimeFilterInfo = runtimeFilterInfo;
  }

  @Override
  public Set<Integer> getExtCommunicableMajorFragments() {
    if (getRuntimeFilterInfo() == null) {
      return Collections.emptySet();
    }
    return runtimeFilterInfo.getRuntimeFilterProbeTargets().stream()
        .map(RuntimeFilterProbeTarget::getProbeScanMajorFragmentId)
        .collect(Collectors.toSet());
  }
}
