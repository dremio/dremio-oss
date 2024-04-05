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

import com.dremio.dac.explore.DataTypeUtil;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.sql.type.SqlTypeName;

/** column info */
public class Field {
  private final String name;
  private final String type;
  private final boolean isPartitioned;
  private final boolean isSorted;

  public Field(ViewFieldType field, boolean isPartitioned, boolean isSorted) {
    this.name = field.getName();
    this.type = DataTypeUtil.getDataType(SqlTypeName.get(field.getType())).name();
    this.isPartitioned = isPartitioned;
    this.isSorted = isSorted;
  }

  @JsonCreator
  public Field(
      @JsonProperty("name") String name,
      @JsonProperty("type") String type,
      @JsonProperty("isPartitioned") boolean isPartitioned,
      @JsonProperty("isSorted") boolean isSorted) {
    this.name = name;
    this.type = type;
    this.isPartitioned = isPartitioned;
    this.isSorted = isSorted;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  @JsonProperty("isPartitioned")
  public boolean isPartitioned() {
    return isPartitioned;
  }

  @JsonProperty("isSorted")
  public boolean isSorted() {
    return isSorted;
  }
}
