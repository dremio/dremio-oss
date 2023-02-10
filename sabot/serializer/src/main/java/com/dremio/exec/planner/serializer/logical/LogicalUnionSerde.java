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

import java.util.stream.Collectors;

import org.apache.calcite.rel.logical.LogicalUnion;

import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.plan.serialization.PLogicalUnion;

/**
 * Serde for LogicalUnion
 */
public final class LogicalUnionSerde implements RelNodeSerde<LogicalUnion, PLogicalUnion> {
  @Override
  public PLogicalUnion serialize(LogicalUnion union, RelToProto s) {
    return PLogicalUnion.newBuilder()
        .addAllInput(union.getInputs().stream().map(s::toProto).collect(Collectors.toList()))
        .setAll(union.all)
        .build();
  }

  @Override
  public LogicalUnion deserialize(PLogicalUnion node, RelFromProto s) {
    return LogicalUnion.create(
      node.getInputList().stream().map(s::toRel).collect(Collectors.toList()),
      node.getAll());
  }
}
