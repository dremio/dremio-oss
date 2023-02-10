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
package com.dremio.exec.planner.serializer.logical;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.planner.serializer.RelCollationSerde;
import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.exec.planner.serializer.SqlOperatorConverter;
import com.dremio.plan.serialization.PAggregateCall;
import com.dremio.plan.serialization.PGroupSet;
import com.dremio.plan.serialization.PLogicalAggregate;
import com.google.protobuf.StringValue;

/**
 * Serde for LogicalAggregate
 */
public final class LogicalAggregateSerde implements RelNodeSerde<LogicalAggregate, PLogicalAggregate> {
  @Override
  public PLogicalAggregate serialize(LogicalAggregate aggregate, RelToProto s) {
    return PLogicalAggregate.newBuilder()
        .setInput(s.toProto(aggregate.getInput()))
        .addAllAggregateCall(
            aggregate.getAggCallList()
            .stream()
            .map(c -> toProto(c, s.getSqlOperatorConverter()))
            .collect(Collectors.toList()))
        .setGroupSet(toProto(aggregate.getGroupSet()))
        .addAllGroupSets(aggregate
          .getGroupSets()
          .stream()
          .map(LogicalAggregateSerde::toProto)
          .collect(Collectors.toList()))
        .setIndicator(false)
        .build();
  }

  @Override
  public LogicalAggregate deserialize(PLogicalAggregate node, RelFromProto s) {
    RelNode input = s.toRel(node.getInput());
    List<AggregateCall> calls = node.getAggregateCallList().stream().map(call -> fromProto(s, call, input, node.getGroupSet().getGroupList().size())).collect(Collectors.toList());
    ImmutableBitSet groupSet = ImmutableBitSet.of(node.getGroupSet().getGroupList());
    List<ImmutableBitSet> groupSets = node
      .getGroupSetsList()
      .stream()
      .map(gs -> ImmutableBitSet.of(gs.getGroupList()))
      .collect(Collectors.toList());
    return LogicalAggregate.create(input, groupSet, groupSets, calls);
  }

  public static PGroupSet toProto(ImmutableBitSet set) {
    return PGroupSet.newBuilder()
        .addAllGroup(set.asList())
        .build();
  }

  public static PAggregateCall toProto(AggregateCall c, SqlOperatorConverter sqlOperatorConverter) {
    PAggregateCall.Builder builder = PAggregateCall.newBuilder()
        .setApproximate(c.isApproximate())
        .addAllArg(c.getArgList())
        .setDistinct(c.isDistinct())
        .setFilterArg(c.filterArg)
        .setRelCollation(RelCollationSerde.toProto(c.collation))
        .setOperator(sqlOperatorConverter.toProto(c.getAggregation()));
    if (c.getName() != null) {
      builder.setName(StringValue.of(c.getName()));
    }

    return builder.build();
  }

  public static AggregateCall fromProto(
    RelFromProto s,
    PAggregateCall pAggregateCall,
    RelNode input,
    int cardinality) {
    new SqlOperatorConverter(s.funcs()).fromProto(pAggregateCall.getOperator());
    String name = null;
    if (pAggregateCall.hasName()) {
      name = pAggregateCall.getName().getValue();
    }

    RelCollation relCollation;
    if (pAggregateCall.hasRelCollationLegacy()) {
      assert !pAggregateCall.hasRelCollation();
      relCollation = RelCollationSerde.fromProto(pAggregateCall.getRelCollationLegacy());
    } else if (pAggregateCall.hasRelCollation()) {
      assert !pAggregateCall.hasRelCollationLegacy();
      relCollation = RelCollationSerde.fromProto(pAggregateCall.getRelCollation());
    } else {
      relCollation = RelCollations.EMPTY;
    }

    return AggregateCall.create(
      (SqlAggFunction) s.toOp(pAggregateCall.getOperator()),
      pAggregateCall.getDistinct(),
      pAggregateCall.getApproximate(),
      pAggregateCall.getArgList(),
      -1,
      relCollation,
      cardinality,
      input,
      null,
      name);
  }
}
