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

import com.dremio.exec.planner.serializer.RelCollationSerde;
import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.plan.serialization.PLogicalMatch;
import com.dremio.plan.serialization.PRexNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rex.RexNode;

/** Serde for Logical Match. */
public final class LogicalMatchSerde implements RelNodeSerde<LogicalMatch, PLogicalMatch> {
  /**
   * How to serialize this RelNode
   *
   * @param node The node to serialize
   * @param s Contextual utility to help with serialization.
   * @return The serialized Protobuf message
   */
  @Override
  public PLogicalMatch serialize(LogicalMatch node, RelToProto s) {
    PLogicalMatch.Builder builder =
        PLogicalMatch.newBuilder()
            .setInput(s.toProto(node.getInput()))
            .setRowType(s.toProto(node.getRowType()))
            .setPattern(s.toProto(node.getPattern()))
            .setStrictStart(node.isStrictStart())
            .setStrictEnd(node.isStrictEnd())
            .putAllPatternDefinition(LogicalMatchSerde.toProto(node.getPatternDefinitions(), s))
            .putAllMeasures(LogicalMatchSerde.toProto(node.getMeasures(), s))
            .setAfter(s.toProto(node.getAfter()))
            .putAllSubsets(LogicalMatchSerde.toProto(node.getSubsets()))
            .setAllRows(node.isAllRows())
            .addAllPartitionKeys(
                node.getPartitionKeys().stream().map(s::toProto).collect(Collectors.toList()))
            .setOrderKeys(RelCollationSerde.toProto(node.getOrderKeys()));

    if (node.getInterval() != null) {
      builder.setInterval(s.toProto(node.getInterval()));
    }

    return builder.build();
  }

  /**
   * How to deserialize a RelNode from the corresponding Protobuf Message.
   *
   * @param pLogicalMatch The Protobuf Message to deserialize
   * @param s Contextual utility used to help with deserialization.
   * @return
   */
  @Override
  public LogicalMatch deserialize(PLogicalMatch pLogicalMatch, RelFromProto s) {
    return LogicalMatch.create(
        s.toRel(pLogicalMatch.getInput()),
        s.toRelDataType(pLogicalMatch.getRowType()),
        s.toRex(pLogicalMatch.getPattern()),
        pLogicalMatch.getStrictStart(),
        pLogicalMatch.getStrictEnd(),
        LogicalMatchSerde.fromProto(pLogicalMatch.getPatternDefinitionMap(), s),
        LogicalMatchSerde.fromProto(pLogicalMatch.getMeasuresMap(), s),
        s.toRex(pLogicalMatch.getAfter()),
        LogicalMatchSerde.fromProto(pLogicalMatch.getSubsetsMap()),
        pLogicalMatch.getAllRows(),
        pLogicalMatch.getPartitionKeysList().stream().map(s::toRex).collect(Collectors.toList()),
        RelCollationSerde.fromProto(pLogicalMatch.getOrderKeys()),
        pLogicalMatch.getInterval() != null ? s.toRex(pLogicalMatch.getInterval()) : null);
  }

  private static Map<String, PRexNode> toProto(
      ImmutableMap<String, RexNode> map, RelToProto relToProto) {
    Preconditions.checkNotNull(map);
    Preconditions.checkNotNull(relToProto);

    Map<String, PRexNode> protoMap = new HashMap<>();
    for (String key : map.keySet()) {
      protoMap.put(key, relToProto.toProto(map.get(key)));
    }

    return protoMap;
  }

  private static Map<String, PLogicalMatch.Strings> toProto(
      ImmutableMap<String, SortedSet<String>> subsets) {
    Preconditions.checkNotNull(subsets);

    Map<String, PLogicalMatch.Strings> protoSubsets = new HashMap<>();
    for (String key : subsets.keySet()) {
      protoSubsets.put(
          key,
          PLogicalMatch.Strings.newBuilder()
              .addAllValues(subsets.get(key).stream().collect(Collectors.toList()))
              .build());
    }

    return protoSubsets;
  }

  private static Map<String, RexNode> fromProto(
      Map<String, PRexNode> protoMap, RelFromProto relFromProto) {
    Preconditions.checkNotNull(protoMap);
    Preconditions.checkNotNull(relFromProto);

    Map<String, RexNode> map = new HashMap<>();
    for (String key : protoMap.keySet()) {
      map.put(key, relFromProto.toRex(protoMap.get(key)));
    }

    return map;
  }

  private static Map<String, SortedSet<String>> fromProto(
      Map<String, PLogicalMatch.Strings> protoSubsets) {
    Preconditions.checkNotNull(protoSubsets);

    Map<String, SortedSet<String>> subsets = new HashMap<>();
    for (String key : protoSubsets.keySet()) {
      subsets.put(key, Sets.newTreeSet(protoSubsets.get(key).getValuesList()));
    }

    return subsets;
  }
}
