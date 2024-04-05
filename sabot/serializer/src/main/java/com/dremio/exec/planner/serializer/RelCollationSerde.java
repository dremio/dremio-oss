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

import com.dremio.plan.serialization.PRelCollation;
import com.dremio.plan.serialization.PRelCollationImpl;
import com.dremio.plan.serialization.PRelFieldCollation;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.lang3.NotImplementedException;

/** Serde for RelCollation. */
public final class RelCollationSerde {
  private RelCollationSerde() {}

  public static RelCollation fromProto(PRelCollation pRelCollation) {
    Preconditions.checkNotNull(pRelCollation);

    if (pRelCollation.hasImpl()) {
      return fromProto(pRelCollation.getImpl());
    } else {
      throw new NotImplementedException("Don't know how to deserialize this type of PRelCollation");
    }
  }

  public static RelCollation fromProto(PRelCollationImpl pRelCollationImpl) {
    Preconditions.checkNotNull(pRelCollationImpl);
    List<RelFieldCollation> fieldCollations =
        pRelCollationImpl.getFieldCollationsList().stream()
            .map(pRelFieldCollation -> RelFieldCollationSerde.fromProto(pRelFieldCollation))
            .collect(Collectors.toList());
    return RelCollations.of(fieldCollations);
  }

  public static PRelCollation toProto(RelCollation relCollation) {
    Preconditions.checkNotNull(relCollation);

    List<PRelFieldCollation> pRelFieldCollations =
        relCollation.getFieldCollations().stream()
            .map(collation -> RelFieldCollationSerde.toProto(collation))
            .collect(Collectors.toList());
    return PRelCollation.newBuilder()
        .setImpl(PRelCollationImpl.newBuilder().addAllFieldCollations(pRelFieldCollations))
        .build();
  }
}
