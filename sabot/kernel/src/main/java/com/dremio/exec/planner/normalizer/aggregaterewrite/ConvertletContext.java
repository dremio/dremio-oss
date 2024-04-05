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
package com.dremio.exec.planner.normalizer.aggregaterewrite;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

/** Context passed to an AggregateConvertlet to convert AggregateCalls */
public final class ConvertletContext {
  private final Aggregate oldAggRel;
  private final AggregateCall oldCall;
  private final List<AggregateCall> newCalls;
  private final Map<AggregateCall, RexNode> aggCallMapping;
  private final List<RexNode> inputExprs;

  public ConvertletContext(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    this.oldAggRel = oldAggRel;
    this.oldCall = oldCall;
    this.newCalls = newCalls;
    this.aggCallMapping = aggCallMapping;
    this.inputExprs = inputExprs;
  }

  public Aggregate getOldAggRel() {
    return oldAggRel;
  }

  public AggregateCall getOldCall() {
    return oldCall;
  }

  public List<AggregateCall> getNewCalls() {
    return newCalls;
  }

  public Map<AggregateCall, RexNode> getAggCallMapping() {
    return aggCallMapping;
  }

  public List<RexNode> getInputExprs() {
    return inputExprs;
  }

  public int getArg(int index) {
    return oldCall.getArgList().get(index);
  }

  public RexBuilder getRexBuilder() {
    return oldAggRel.getCluster().getRexBuilder();
  }

  public int addExpression(RexNode rexNode) {
    int index = inputExprs.size();
    inputExprs.add(rexNode);
    return index;
  }

  public RexNode addAggregate(AggregateCall newCall) {
    List<RelDataType> inputTypes =
        newCall.getArgList().stream()
            .map(index -> inputExprs.get(index).getType())
            .collect(Collectors.toList());

    return getRexBuilder()
        .addAggCall(newCall, oldAggRel.getGroupCount(), newCalls, aggCallMapping, inputTypes);
  }
}
