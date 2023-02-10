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
package com.dremio.exec.planner.serializer;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;

import com.dremio.plan.serialization.PRelTraitSet;
import com.google.common.base.Preconditions;

/**
 * Serde for RelTraitSet
 */
public final class RelTraitSetSerde {
  private RelTraitSetSerde() { }

  public static RelTraitSet fromProto(PRelTraitSet pRelTraitSet) {
    Preconditions.checkNotNull(pRelTraitSet);
    RelTraitSet relTraitSet = RelTraitSet.createEmpty();

    boolean isNewFormat = pRelTraitSet.getTraitsCount() != 0;
    if (isNewFormat) {
      assert pRelTraitSet.getLegacyCollationCount() == 0;
      List<RelTrait> relTraits = pRelTraitSet
        .getTraitsList()
        .stream()
        .map(RelTraitSerde::fromProto)
        .collect(Collectors.toList());
      assert relTraitSet.addAll(relTraits);
    } else {
      assert pRelTraitSet.getTraitsCount() == 0;
      List<RelFieldCollation> relFieldCollations = pRelTraitSet
        .getLegacyCollationList()
        .stream()
        .map(RelFieldCollationSerde::fromProto)
        .collect(Collectors.toList());

      RelCollation relCollation = RelCollations.of(relFieldCollations);
      assert relTraitSet.add(relCollation);
    }

    return relTraitSet;
  }

  public static PRelTraitSet toProto(RelTraitSet relTraitSet) {
    Preconditions.checkNotNull(relTraitSet);
    PRelTraitSet.Builder builder = PRelTraitSet.newBuilder();
    if (relTraitSet.size() != 0) {
      builder.addAllTraits(relTraitSet
        .stream()
        .map(RelTraitSerde::toProto)
        .collect(Collectors.toList()));
    }

    return builder.build();
  }
}
