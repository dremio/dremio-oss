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

import com.dremio.plan.serialization.PRelTrait;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelCollation;
import org.apache.commons.lang3.NotImplementedException;

/** Serde for RelTrait. */
public final class RelTraitSerde {
  private RelTraitSerde() {}

  public static RelTrait fromProto(PRelTrait pRelTrait) {
    Preconditions.checkNotNull(pRelTrait);

    RelTrait relTrait;
    if (pRelTrait.hasCollation()) {
      relTrait = RelCollationSerde.fromProto(pRelTrait.getCollation());
    } else {
      throw new NotImplementedException("Don't know how to handle this PRelTrait.");
    }

    return relTrait;
  }

  public static PRelTrait toProto(RelTrait relTrait) {
    Preconditions.checkNotNull(relTrait);

    PRelTrait pRelTrait;
    if (relTrait instanceof RelCollation) {
      pRelTrait =
          PRelTrait.newBuilder()
              .setCollation(RelCollationSerde.toProto((RelCollation) relTrait))
              .build();
    } else {
      throw new NotImplementedException("Don't know how to handle this RelTrait.");
    }

    return pRelTrait;
  }
}
