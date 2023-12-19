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

import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Uncollect;

import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.exec.planner.serializer.RelTraitSetSerde;
import com.dremio.plan.serialization.PAbstractRelNode;
import com.dremio.plan.serialization.PSingleRel;
import com.dremio.plan.serialization.PUncollect;

/**
 * Serde for Uncollect.
 */
public final class UncollectSerde implements RelNodeSerde<Uncollect, PUncollect> {
  /**
   * How to serialize this RelNode
   *
   * @param node The node to serialize
   * @param s    Contextual utility to help with serialization.
   * @return The serialized Protobuf message
   */
  @Override
  public PUncollect serialize(Uncollect node, RelToProto s) {
    return PUncollect.newBuilder()
      .setSingleRel(
        PSingleRel.newBuilder()
          .setAbstractRelNode(
            PAbstractRelNode.newBuilder()
              // DX-68622: There is a bug in the trait set serializer so for now we are just using an empty trait set.
              .setTraitSet(RelTraitSetSerde.toProto(RelTraitSet.createEmpty()))
              .build())
          .setInput(s.toProto(node.getInput()))
          .build())
      .setWithOrdinality(node.withOrdinality)
      // This is empty for now, since unnest does not expose the aliases to us.
      .addAllItemAliases(Collections.emptyList())
      .build();
  }

  /**
   * How to deserialize a RelNode from the corresponding Protobuf Message.
   *
   * @param pUncollect The Protobuf Message to deserialize
   * @param s          Contextual utility used to help with deserialization.
   * @return
   */
  @Override
  public Uncollect deserialize(PUncollect pUncollect, RelFromProto s) {
    return Uncollect.create(
      // DX-68622: There is a bug in the trait set serializer so for now we are just using an empty trait set.
      RelTraitSet.createEmpty(),
      s.toRel(pUncollect.getSingleRel().getInput()),
      pUncollect.getWithOrdinality(),
      pUncollect.getItemAliasesList().stream().collect(Collectors.toList()));
  }
}
