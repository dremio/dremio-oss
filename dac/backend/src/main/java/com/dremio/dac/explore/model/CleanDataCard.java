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

import java.util.List;

import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Supports the data cleanup screen
 */
public class CleanDataCard {

  private final String newFieldName;
  private final String newFieldNamePrefix;
  private final List<ConvertToSingleType> convertToSingles;
  private final List<SplitByDataType> split;
  private final long availableValuesCount;
  private final List<HistogramValue> availableValues;

  @JsonCreator
  public CleanDataCard(
      @JsonProperty("newFieldName") String newFieldName,
      @JsonProperty("newFieldNamePrefix") String newFieldNamePrefix,
      @JsonProperty("convertToSingles") List<ConvertToSingleType> convertToSingles,
      @JsonProperty("split") List<SplitByDataType> split,
      @JsonProperty("availableValuesCount") long availableValuesCount,
      @JsonProperty("availableValues") List<HistogramValue> availableValues) {
    super();
    this.newFieldName = newFieldName;
    this.newFieldNamePrefix = newFieldNamePrefix;
    this.convertToSingles = convertToSingles;
    this.split = split;
    this.availableValuesCount = availableValuesCount;
    this.availableValues = availableValues;
  }

  public String getNewFieldName() {
    return newFieldName;
  }

  public String getNewFieldNamePrefix() {
    return newFieldNamePrefix;
  }

  public List<ConvertToSingleType> getConvertToSingles() {
    return convertToSingles;
  }

  public List<SplitByDataType> getSplit() {
    return split;
  }

  public long getAvailableValuesCount() {
    return availableValuesCount;
  }

  public List<HistogramValue> getAvailableValues() {
    return availableValues;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }

  /**
   * to display based on the typed picked in the "desiredType" dropdown
   */
  public static class ConvertToSingleType {
    private final DataType desiredType;
    private final boolean castWhenPossible;
    private final long nonMatchingCount;
    private final List<HistogramValue> availableNonMatching;

    @JsonCreator
    public ConvertToSingleType(
        @JsonProperty("desiredType") DataType desiredType,
        @JsonProperty("castWhenPossible") boolean castWhenPossible,
        @JsonProperty("nonMatchingCount") long nonMatchingCount,
        @JsonProperty("availableNonMatching") List<HistogramValue> availableNonMatching) {
      super();
      this.desiredType = desiredType;
      this.castWhenPossible = castWhenPossible;
      this.nonMatchingCount = nonMatchingCount;
      this.availableNonMatching = availableNonMatching;
    }
    public DataType getDesiredType() {
      return desiredType;
    }
    public boolean isCastWhenPossible() {
      return castWhenPossible;
    }
    public long getNonMatchingCount() {
      return nonMatchingCount;
    }
    public List<HistogramValue> getAvailableNonMatching() {
      return availableNonMatching;
    }
    @Override
    public String toString() {
      return JSONUtil.toString(this);
    }
  }

  /**
   * stats per type
   */
  public static class SplitByDataType {
    private final DataType type;
    private final double matchingPercent;

    @JsonCreator
    public SplitByDataType(
        @JsonProperty("type") DataType type,
        @JsonProperty("matchingPercent") double matchingPercent) {
      super();
      this.type = type;
      this.matchingPercent = matchingPercent;
    }
    public DataType getType() {
      return type;
    }
    public double getMatchingPercent() {
      return matchingPercent;
    }
    @Override
    public String toString() {
      return JSONUtil.toString(this);
    }
  }
}
