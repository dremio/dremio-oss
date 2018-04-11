/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * A column descriptor for the data grid
 */
public class Column {
  private final String name;
  private final DataType type;
  private final int index;

  @JsonCreator
  public Column(
      @JsonProperty("name") String name,
      @JsonProperty("type") DataType type,
      @JsonProperty("index") int index) {
    super();
    this.name = name;
    this.type = type;
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public DataType getType() {
    return type;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Column column = (Column) o;

    return index == column.index &&
        Objects.equal(name, column.name) &&
        type == column.type;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, type, index);
  }
}
