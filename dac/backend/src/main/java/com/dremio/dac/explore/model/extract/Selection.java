/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.explore.model.extract;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The selection in a given cell
 */
public class Selection {
  @NotNull
  private final String colName;
  // Can be null
  private final String cellText;
  @Min(0)
  private final int offset;
  @Min(0)
  private final int length;

  @JsonCreator
  public Selection(
      @JsonProperty("colName") String colName,
      @JsonProperty("cellText") String cellText,
      @JsonProperty("offset") int offset,
      @JsonProperty("length") int length) {
    super();
    this.colName = colName;
    this.cellText = cellText;
    this.offset = offset;
    this.length = length;
  }

  public String getColName() {
    return colName;
  }
  public String getCellText() {
    return cellText;
  }
  public int getOffset() {
    return offset;
  }
  public int getLength() {
    return length;
  }
  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }
}
