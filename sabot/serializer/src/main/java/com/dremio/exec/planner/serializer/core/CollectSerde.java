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
package com.dremio.exec.planner.serializer.core;

import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.exec.planner.serializer.RelTraitSetSerde;
import com.dremio.plan.serialization.PAbstractRelNode;
import com.dremio.plan.serialization.PCollect;
import com.dremio.plan.serialization.PSingleRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Collect;

/** Serde for Collect. */
public final class CollectSerde implements RelNodeSerde<Collect, PCollect> {
  /**
   * How to serialize this RelNode
   *
   * @param node The node to serialize
   * @param s Contextual utility to help with serialization.
   * @return The serialized Protobuf message
   */
  @Override
  public PCollect serialize(Collect node, RelToProto s) {
    return PCollect.newBuilder()
        .setSingleRel(
            PSingleRel.newBuilder()
                .setAbstractRelNode(
                    PAbstractRelNode.newBuilder()
                        .setTraitSet(RelTraitSetSerde.toProto(node.getTraitSet()))
                        .build())
                .setInput(s.toProto(node.getInput()))
                .build())
        .setFieldName(node.getFieldName())
        .build();
  }

  /**
   * How to deserialize a RelNode from the corresponding Protobuf Message.
   *
   * @param pCollect The Protobuf Message to deserialize
   * @param s Contextual utility used to help with deserialization.
   * @return
   */
  @Override
  public Collect deserialize(PCollect pCollect, RelFromProto s) {
    RelNode input = s.toRel(pCollect.getSingleRel().getInput());
    return new Collect(
        input.getCluster(),
        RelTraitSetSerde.fromProto(pCollect.getSingleRel().getAbstractRelNode().getTraitSet()),
        input,
        pCollect.getFieldName());
  }
}
