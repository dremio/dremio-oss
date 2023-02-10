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

import org.apache.calcite.plan.RelOptSamplingParameters;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sample;

import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.exec.planner.serializer.RelTraitSetSerde;
import com.dremio.plan.serialization.PAbstractRelNode;
import com.dremio.plan.serialization.PSample;
import com.dremio.plan.serialization.PSingleRel;

/**
 * Serde for Sample.
 */
public final class SampleSerde implements RelNodeSerde<Sample, PSample> {
  /**
   * How to serialize this RelNode
   *
   * @param node The node to serialize
   * @param s    Contextual utility to help with serialization.
   * @return The serialized Protobuf message
   */
  @Override
  public PSample serialize(Sample node, RelToProto s) {
    return PSample.newBuilder()
      .setSingleRel(
        PSingleRel.newBuilder()
          .setAbstractRelNode(
            PAbstractRelNode.newBuilder()
              .setTraitSet(RelTraitSetSerde.toProto(node.getTraitSet()))
              .build())
          .setInput(s.toProto(node.getInput()))
          .build())
      .setParams(RelOptSamplingParametersSerde.toProto(node.getSamplingParameters()))
      .build();
  }

  /**
   * How to deserialize a RelNode from the corresponding Protobuf Message.
   *
   * @param pSample The Protobuf Message to deserialize
   * @param s       Contextual utility used to help with deserialization.
   * @return
   */
  @Override
  public Sample deserialize(PSample pSample, RelFromProto s) {
    RelNode input = s.toRel(pSample.getSingleRel().getInput());
    return new Sample(input.getCluster(), input, RelOptSamplingParametersSerde.fromProto(pSample.getParams()));
  }

  private static final class RelOptSamplingParametersSerde {
    public static RelOptSamplingParameters fromProto(PSample.PRelOptSampleParamters pRelOptSampleParamters) {
      return new RelOptSamplingParameters(
        pRelOptSampleParamters.getIsBernoulli(),
        pRelOptSampleParamters.getSamplingPercentage(),
        pRelOptSampleParamters.getIsRepeatable(),
        pRelOptSampleParamters.getRepeatableSeed());
    }

    public static PSample.PRelOptSampleParamters toProto(RelOptSamplingParameters relOptSamplingParameters) {
      return PSample.PRelOptSampleParamters.newBuilder()
        .setIsBernoulli(relOptSamplingParameters.isBernoulli())
        .setIsRepeatable(relOptSamplingParameters.isRepeatable())
        .setRepeatableSeed(relOptSamplingParameters.getRepeatableSeed())
        .setSamplingPercentage(relOptSamplingParameters.getSamplingPercentage())
        .build();
    }
  }
}
