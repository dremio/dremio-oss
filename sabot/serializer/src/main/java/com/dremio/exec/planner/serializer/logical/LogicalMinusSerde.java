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

import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.plan.serialization.PLogicalMinus;
import java.util.stream.Collectors;
import org.apache.calcite.rel.logical.LogicalMinus;

/** Serde for LogicalMinus */
public final class LogicalMinusSerde implements RelNodeSerde<LogicalMinus, PLogicalMinus> {

  @Override
  public PLogicalMinus serialize(LogicalMinus minus, RelToProto s) {
    return PLogicalMinus.newBuilder()
        .addAllInputs(minus.getInputs().stream().map(s::toProto).collect(Collectors.toList()))
        .setAll(minus.all)
        .build();
  }

  @Override
  public LogicalMinus deserialize(PLogicalMinus node, RelFromProto s) {
    return LogicalMinus.create(
        node.getInputsList().stream().map(s::toRel).collect(Collectors.toList()), node.getAll());
  }
}
