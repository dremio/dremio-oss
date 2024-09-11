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
import com.dremio.plan.serialization.PGroup;
import com.dremio.plan.serialization.PLogicalWindow;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.ImmutableBitSet;

public class LogicalWindowSerde implements RelNodeSerde<LogicalWindow, PLogicalWindow> {

  @Override
  public PLogicalWindow serialize(LogicalWindow window, RelToProto s) {
    return PLogicalWindow.newBuilder()
        .setInput(s.toProto(window.getInput()))
        .setType(s.toProto(window.getRowType()))
        .addAllConstants(
            window.getConstants().stream().map(s::toProto).collect(Collectors.toList()))
        .addAllGroups(window.groups.stream().map(g -> toProto(g, s)).collect(Collectors.toList()))
        .build();
  }

  @Override
  public LogicalWindow deserialize(PLogicalWindow node, RelFromProto s) {
    RelNode input = s.toRel(node.getInput());
    return LogicalWindow.create(
        input.getTraitSet(),
        input,
        node.getConstantsList().stream()
            .map(c -> (RexLiteral) s.toRex(c))
            .collect(Collectors.toList()),
        s.toRelDataType(node.getType()),
        node.getGroupsList().stream().map(g -> fromProto(g, s)).collect(Collectors.toList()));
  }

  private PGroup toProto(Group group, RelToProto s) {
    return PGroup.newBuilder()
        .addAllKeys(group.keys)
        .setIsRows(group.isRows)
        .setLowerBound(s.toProto(group.lowerBound))
        .setUpperBound(s.toProto(group.upperBound))
        .setOrderKeys(RelCollationSerde.toProto(group.orderKeys))
        .addAllAggCalls(group.aggCalls.stream().map(s::toProto).collect(Collectors.toList()))
        .build();
  }

  private Group fromProto(PGroup pGroup, RelFromProto s) {
    return new Group(
        ImmutableBitSet.of(pGroup.getKeysList()),
        pGroup.getIsRows(),
        s.toRex(pGroup.getLowerBound()),
        s.toRex(pGroup.getUpperBound()),
        RelCollationSerde.fromProto(pGroup.getOrderKeys()),
        pGroup.getAggCallsList().stream()
            .map(pRexWinAggCall -> (RexWinAggCall) s.toRex(pRexWinAggCall))
            .collect(Collectors.toList()));
  }
}
