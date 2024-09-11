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
package com.dremio.catalog.model.dataset;

import java.util.stream.Stream;

public enum TableVersionType {
  NOT_SPECIFIED("NOT_SPECIFIED"),
  BRANCH("BRANCH"),
  TAG("TAG"),
  REFERENCE("REFERENCE"),
  SNAPSHOT_ID("SNAPSHOT"),
  TIMESTAMP("TIMESTAMP"),
  COMMIT("COMMIT"),
  ;

  private final String sqlRepresentation;

  TableVersionType(String sqlRepresentation) {
    this.sqlRepresentation = sqlRepresentation;
  }

  public String toSqlRepresentation() {
    return sqlRepresentation;
  }

  public boolean isTimeTravel() {
    switch (this) {
      case SNAPSHOT_ID:
      case TIMESTAMP:
        return true;
      default:
        return false;
    }
  }

  public static TableVersionType getType(String type) {
    return Stream.of(values())
        .filter(tableVersionType -> tableVersionType.sqlRepresentation.equals(type))
        .findFirst()
        .orElse(null);
  }
}
