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
package com.dremio.sabot.exec.context;

import com.dremio.exec.proto.UserBitShared.MetricDef.AggregationType;
import com.dremio.exec.proto.UserBitShared.MetricDef.DisplayType;
/**
 * Interface that defines a metric. For example, {@link com.dremio.exec.physical.impl.join.HashJoinOperator.Metric}.
 */
public interface MetricDef {
  public String name();
  public int metricId();

  /**
   * Default display type for all the stats is Never, change it if metric is important and should be
   * displayed in operator details window by default under profile page.
   */
  public default DisplayType getDisplayType() {
    return DisplayType.DISPLAY_NEVER;
  }

  /**
   * Display code is to reflect the meaning of the metric. default value wil be empty.
   * This will be displayed as a tool tip over metric name in operator details window under profile page.
   * Ex: Name :JAVA_EXPRESSIONS
   * suggested Display Code: Number of expressions evaluated completely in Java
   */
  public default String getDisplayCode() {
    return "";
  }

  /**
   * Aggregation logic of metric value across threads (Sum or Max).
   * @return Aggregation type of the metric across threads.
   * Default is SUM
   */
  public default AggregationType getAggregationType() {
    return AggregationType.SUM;
  }

}
