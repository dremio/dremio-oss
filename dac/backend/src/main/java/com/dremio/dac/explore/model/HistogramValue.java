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

import com.dremio.dac.proto.model.dataset.DataType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * to display an histogram
 */
public class HistogramValue {
  private final DataType type;
  // value is used as a label, so it can be range label as well
  private final String value;
  private final ValueRange valueRange;
  private final long count;

  private double percent;

  @JsonCreator
  public HistogramValue(
      @JsonProperty("type") DataType type,
      @JsonProperty("value") String value,
      @JsonProperty("percent") double percent,
      @JsonProperty("long") long count,
      @JsonProperty(value = "valueRange", defaultValue = "null") ValueRange valueRange) {
    this.type = type;
    this.value = value;
    this.percent = percent;
    this.count = count;
    this.valueRange = valueRange;
  }

  public DataType getType() {
    return type;
  }
  public String getValue() {
    return value;
  }
  public ValueRange getValueRange() { return valueRange; }
  public double getPercent() {
    return percent;
  }
  public long getCount() {
    return count;
  }

  public void setPercent(double percent) {
    this.percent = percent;
  }

  /**
   * Helper class to keep lower and Upper boundaries, so we could reuse it to dig deeper
   * into a particular histogram bin if needed
   */
  public static final class ValueRange {
    private final Object lowerLimit;
    private final Object upperLimit;

    @JsonCreator
    public ValueRange(@JsonProperty("start") Object lowerLimit,
                      @JsonProperty("end") Object upperLimit) {
      this.lowerLimit = lowerLimit;
      this.upperLimit = upperLimit;
    }

    public Object getLowerLimit() {
      return lowerLimit;
    }

    public Object getUpperLimit() {
      return upperLimit;
    }
  }
}
