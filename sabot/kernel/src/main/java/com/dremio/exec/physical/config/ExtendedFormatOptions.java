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
package com.dremio.exec.physical.config;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("ExtendedFormatOptions")
public class ExtendedFormatOptions {
  /*
  areStringTransformationsNeeded is a special flag variable that we use to keep track of whether certain string transformations
  like NULL_IF are set in the query. This is useful to us in scenarios where we can follow optimised code paths with fewer steps
  and save time.

  One example of this in action is SchemaImposedOutput::writeValueInCurrentVector where we follow an optimised path
  provided the following conditions are met:
  1. The target column data type is VARCHAR
  2. Transformations like NULL_IF are not used.

  In future, if we add more string transformations or modify any existing ones, it is recommended that we also explore whether
  this flag can be used in that case for any optimisations.
   */
  private Boolean areStringTransformationsNeeded = false;
  private Boolean trimSpace;
  private Boolean emptyAsNull = true;
  private String dateFormat;
  private String timeFormat;
  private String timeStampFormat;

  private List<String> nullIfExpressions = new ArrayList<>();

  public ExtendedFormatOptions() {
  }

  public ExtendedFormatOptions(final Boolean trimSpace, final Boolean emptyAsNull, final String dateFormat, final String timeFormat, final String timeStampFormat, final List<String> nullIfExpressions) {
    this.trimSpace = trimSpace;
    this.emptyAsNull = emptyAsNull;
    this.dateFormat = dateFormat;
    this.timeFormat = timeFormat;
    this.timeStampFormat = timeStampFormat;
    this.nullIfExpressions = nullIfExpressions;
  }

  public Boolean getTrimSpace() {
    return trimSpace;
  }

  public void setTrimSpace(final Boolean trimSpace) {
    this.trimSpace = trimSpace;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  public void setDateFormat(final String dateFormat) {
    this.dateFormat = dateFormat;
  }

  public String getTimeFormat() {
    return timeFormat;
  }

  public void setTimeFormat(final String timeFormat) {
    this.timeFormat = timeFormat;
  }

  public Boolean getEmptyAsNull() {
    return emptyAsNull;
  }

  public void setEmptyAsNull(final Boolean emptyAsNull) {
    this.emptyAsNull = emptyAsNull;
  }

  public String getTimeStampFormat() {
    return timeStampFormat;
  }

  public void setTimeStampFormat(final String timeStampFormat) {
    this.timeStampFormat = timeStampFormat;
  }

  public List<String> getNullIfExpressions() {
    return nullIfExpressions;
  }

  public void setNullIfExpressions(final List<String> nullIfExpressions) {
    this.nullIfExpressions = nullIfExpressions;
    // Check if nullIfExpressions is non-null and non-empty.
    final boolean nullIfExpressionsContainsData = (nullIfExpressions != null && !nullIfExpressions.isEmpty());
    // In case 'nullIfExpressionsContainsData' is false, we want to preserve areStringTransformationsNeeded's original value.
    // Hence, use logical OR here while setting areStringTransformationsNeeded.
    setAreStringTransformationsNeeded(areStringTransformationsNeeded || nullIfExpressionsContainsData);
  }

  public boolean getAreStringTransformationsNeeded() {
    return areStringTransformationsNeeded;
  }

  private void setAreStringTransformationsNeeded(final Boolean areStringTransformationsNeeded) {
    this.areStringTransformationsNeeded = areStringTransformationsNeeded;
  }

  @Override
  public String toString() {
    return "ExtendedFormatOptions{" +
            "trimSpace=" + trimSpace +
            ", emptyAsNull=" + emptyAsNull +
            ", dateFormat='" + dateFormat + '\'' +
            ", timeFormat='" + timeFormat + '\'' +
            ", timeStampFormat='" + timeStampFormat + '\'' +
            ", nullIfExpressions=" + nullIfExpressions +
            '}';
  }
}
