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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.planner.physical.filter.RuntimeFilterEntry;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

@JsonTypeName("hash-join")
public class HashJoinPOP extends AbstractBase {

  private final PhysicalOperator left;
  private final PhysicalOperator right;
  private final List<JoinCondition> conditions;
  private final JoinRelType joinType;
  private final boolean vectorize;
  private RuntimeFilterInfo runtimeFilterInfo;

  @JsonCreator
  public HashJoinPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("left") PhysicalOperator left,
      @JsonProperty("right") PhysicalOperator right,
      @JsonProperty("conditions") List<JoinCondition> conditions,
      @JsonProperty("joinType") JoinRelType joinType,
      @JsonProperty("vectorize") boolean vectorize,
      @JsonProperty("runtimeFilterInfo") RuntimeFilterInfo runtimeFilterInfo
      ) {
    super(props);
    this.left = left;
    this.right = right;
    this.conditions = conditions;
    this.joinType = joinType;
    this.vectorize = vectorize;
    this.runtimeFilterInfo = runtimeFilterInfo;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
      return physicalVisitor.visitHashJoin(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
      Preconditions.checkArgument(children.size() == 2);
      return new HashJoinPOP(props, children.get(0), children.get(1), conditions, joinType, vectorize, runtimeFilterInfo);
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

  public boolean isVectorize() {
    return vectorize;
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
    // Collect all probe scan side major fragments on runtime filter join columns.
    return Stream.concat(getRuntimeFilterInfo().getPartitionJoinColumns().stream(), getRuntimeFilterInfo().getNonPartitionJoinColumns().stream())
            .map(RuntimeFilterEntry::getProbeScanMajorFragmentId).collect(Collectors.toSet());
  }
}
