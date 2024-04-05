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
package com.dremio.dac.model.job.acceleration;

import com.dremio.sabot.kernel.proto.ReflectionExplanation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/** UI wrapper for {@link ReflectionExplanation} */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "explanationType")
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = ReflectionExplanationUI.DisjointFilter.class,
      name = "DISJOINT_FILTER"),
  @JsonSubTypes.Type(value = ReflectionExplanationUI.FieldMissing.class, name = "FIELD_MISSING"),
  @JsonSubTypes.Type(
      value = ReflectionExplanationUI.FilterOverSpecified.class,
      name = "FILTER_OVER_SPECIFIED"),
})
public abstract class ReflectionExplanationUI {

  public abstract String getExplanationType();

  public static final class DisjointFilter extends ReflectionExplanationUI {

    private final String filter;

    @JsonCreator
    public DisjointFilter(@JsonProperty("filter") String filter) {
      this.filter = filter;
    }

    public String getFilter() {
      return filter;
    }

    @Override
    public String getExplanationType() {
      return "DISJOINT_FILTER";
    }
  }

  public static final class FieldMissing extends ReflectionExplanationUI {
    private final String columnName;
    private final int columnIndex;

    @JsonCreator
    public FieldMissing(
        @JsonProperty("columnName") String columnName,
        @JsonProperty("columnIndex") int columnIndex) {
      this.columnName = columnName;
      this.columnIndex = columnIndex;
    }

    public String getColumnName() {
      return columnName;
    }

    public int getColumnIndex() {
      return columnIndex;
    }

    @Override
    public String getExplanationType() {
      return "FIELD_MISSING";
    }
  }

  public static final class FilterOverSpecified extends ReflectionExplanationUI {
    private final String filter;

    @JsonCreator
    public FilterOverSpecified(@JsonProperty("filter") String filter) {
      this.filter = filter;
    }

    public String getFilter() {
      return filter;
    }

    @Override
    public String getExplanationType() {
      return "FILTER_OVER_SPECIFIED";
    }
  }
}
