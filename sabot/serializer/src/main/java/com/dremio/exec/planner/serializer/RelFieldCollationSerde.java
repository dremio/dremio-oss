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

import org.apache.calcite.rel.RelFieldCollation;

import com.dremio.plan.serialization.PRelFieldCollation;
import com.google.common.base.Preconditions;

/**
 * Serde for RelFieldCollation.
 */
public final class RelFieldCollationSerde {
  private RelFieldCollationSerde() {}

  public static RelFieldCollation fromProto(PRelFieldCollation pRelFieldCollation) {
    Preconditions.checkNotNull(pRelFieldCollation);

    return new RelFieldCollation(
      pRelFieldCollation.getFieldIndex(),
      fromProto(pRelFieldCollation.getDirection()),
      fromProto(pRelFieldCollation.getNullDirection()));
  }

  public static PRelFieldCollation toProto(RelFieldCollation relFieldCollation) {
    Preconditions.checkNotNull(relFieldCollation);

    return PRelFieldCollation.newBuilder()
      .setDirection(toProto(relFieldCollation.getDirection()))
      .setNullDirection(toProto(relFieldCollation.nullDirection))
      .setFieldIndex(relFieldCollation.getFieldIndex())
      .build();
  }

  private static RelFieldCollation.Direction fromProto(PRelFieldCollation.PDirection pDirection) {
    switch(pDirection) {
    case ASCENDING: return RelFieldCollation.Direction.ASCENDING;
    case CLUSTERED: return RelFieldCollation.Direction.CLUSTERED;
    case DESCENDING: return RelFieldCollation.Direction.DESCENDING;
    case STRICTLY_ASCENDING: return RelFieldCollation.Direction.STRICTLY_ASCENDING;
    case STRICTLY_DESCENDING: return RelFieldCollation.Direction.STRICTLY_DESCENDING;
    default:
      throw new UnsupportedOperationException(String.format("Unknown direction %s.", pDirection));
    }
  }

  private static RelFieldCollation.NullDirection fromProto(PRelFieldCollation.PNullDirection pNullDirection) {
    switch(pNullDirection) {
    case FIRST: return RelFieldCollation.NullDirection.FIRST;
    case LAST: return RelFieldCollation.NullDirection.LAST;
    case UNSPECIFIED_NULL_DIRECTION: return RelFieldCollation.NullDirection.UNSPECIFIED;
    default:
      throw new UnsupportedOperationException(String.format("Unknown null direction %s.", pNullDirection));
    }
  }

  public static PRelFieldCollation.PDirection toProto(RelFieldCollation.Direction direction) {
    switch(direction) {
    case ASCENDING: return PRelFieldCollation.PDirection.ASCENDING;
    case CLUSTERED: return PRelFieldCollation.PDirection.CLUSTERED;
    case DESCENDING: return PRelFieldCollation.PDirection.DESCENDING;
    case STRICTLY_ASCENDING: return PRelFieldCollation.PDirection.STRICTLY_ASCENDING;
    case STRICTLY_DESCENDING: return PRelFieldCollation.PDirection.STRICTLY_DESCENDING;
    default:
      throw new UnsupportedOperationException(String.format("Unknown direction %s.", direction));
    }
  }

  public static PRelFieldCollation.PNullDirection toProto(RelFieldCollation.NullDirection nullDirection) {
    switch(nullDirection) {
    case FIRST: return PRelFieldCollation.PNullDirection.FIRST;
    case LAST: return PRelFieldCollation.PNullDirection.LAST;
    case UNSPECIFIED: return PRelFieldCollation.PNullDirection.UNSPECIFIED_NULL_DIRECTION;
    default:
      throw new UnsupportedOperationException(String.format("Unknown null direction %s.", nullDirection));
    }
  }
}
