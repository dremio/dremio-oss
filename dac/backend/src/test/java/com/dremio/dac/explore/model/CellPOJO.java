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

import java.util.Objects;

import com.dremio.dac.proto.model.dataset.DataType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A cell containing a value in the data grid. For testing purposes only to deserialize the result output from server.
 */
public class CellPOJO {
  private final Object value;
  private final DataType type;
  private final String url;

  @JsonCreator
  public CellPOJO(@JsonProperty("v") Object value, @JsonProperty("t") DataType type, @JsonProperty("u") String url) {
    this.value = value;
    this.type = type;
    this.url = url;
  }

  @JsonProperty("v")
  public Object getValue() {
    return value;
  }

  @JsonProperty("t")
  public DataType getType() {
    return type;
  }

  @JsonProperty("u")
  public String getUrl() {
    return url;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, type, url);
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
    CellPOJO other = (CellPOJO) obj;

    return Objects.equals(type, other.type) &&
        Objects.equals(value, other.value) &&
        Objects.equals(url, other.url);
  }
}
