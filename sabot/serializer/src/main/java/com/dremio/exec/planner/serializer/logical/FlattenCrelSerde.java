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

import com.dremio.exec.calcite.logical.FlattenCrel;
import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.plan.serialization.PFlattenCrel;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexInputRef;

public class FlattenCrelSerde implements RelNodeSerde<FlattenCrel, PFlattenCrel> {
  @Override
  public PFlattenCrel serialize(FlattenCrel node, RelToProto s) {
    return PFlattenCrel.newBuilder()
        .setInput(s.toProto(node.getInput()))
        .addAllToFlatten(node.getToFlatten().stream().map(s::toProto).collect(Collectors.toList()))
        .addAllAlias(node.getAliases())
        .setNumProjects(node.getNumProjectsPushed())
        .build();
  }

  @Override
  public FlattenCrel deserialize(PFlattenCrel pFlattenCrel, RelFromProto s) {
    return FlattenCrel.create(
        s.toRel(pFlattenCrel.getInput()),
        pFlattenCrel.getToFlattenList().stream()
            .map(s::toRex)
            .map(rexNode -> (RexInputRef) rexNode)
            .collect(Collectors.toList()),
        pFlattenCrel.getAliasList(),
        pFlattenCrel.getNumProjects());
  }
}
