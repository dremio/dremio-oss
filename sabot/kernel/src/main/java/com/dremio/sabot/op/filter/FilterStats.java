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
package com.dremio.sabot.op.filter;

import com.dremio.exec.proto.UserBitShared.MetricDef.AggregationType;
import com.dremio.exec.proto.UserBitShared.MetricDef.DisplayType;
import com.dremio.sabot.exec.context.MetricDef;

public class FilterStats {
  public enum Metric implements MetricDef {
    JAVA_BUILD_TIME,
    JAVA_EXECUTE_TIME,
    GANDIVA_BUILD_TIME,
    GANDIVA_EXECUTE_TIME,
    JAVA_EXPRESSIONS(DisplayType.DISPLAY_BY_DEFAULT, AggregationType.MAX, "Maximum number of expressions evaluated completely in Java"),
    GANDIVA_EXPRESSIONS(DisplayType.DISPLAY_BY_DEFAULT, AggregationType.MAX, "Maximum number of expressions evaluated completely in Gandiva"),
    MIXED_SPLITS;

    private final DisplayType displayType;
    private final AggregationType aggregationType;
    private final String displayCode;

    Metric() {
      this(DisplayType.DISPLAY_NEVER, AggregationType.SUM, "");
    }

    Metric(DisplayType displayType, AggregationType aggregationType, String displayCode) {
      this.displayType = displayType;
      this.aggregationType = aggregationType;
      this.displayCode = displayCode;
    }

    @Override
    public int metricId() {
      return ordinal();
    }

    @Override
    public DisplayType getDisplayType() {
      return this.displayType;
    }

    @Override
    public AggregationType getAggregationType() {
      return this.aggregationType;
    }

    @Override
    public String getDisplayCode() {
      return this.displayCode;
    }
  }
}
