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
package com.dremio.dac.explore.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

/**
 * A row in the data grid. This class is for testing purposes only to deserialize the result output
 * from server.
 */
public class RowPOJO {

  private final List<CellPOJO> row;

  @JsonCreator
  public RowPOJO(@JsonProperty("row") List<CellPOJO> row) {
    super();
    this.row = row;
  }

  public List<CellPOJO> getRow() {
    return row;
  }

  public CellPOJO getCell(Column col) {
    return getRow().get(col.getIndex());
  }

  @Override
  public int hashCode() {
    return Objects.hash(row);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    RowPOJO other = (RowPOJO) obj;

    return Objects.equals(row, other.row);
  }
}
