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
package com.dremio.exec.planner.normalizer;

import com.dremio.exec.planner.sql.NonCacheableFunctionDetector;
import org.apache.calcite.rel.RelNode;

public final class PreSerializedQuery {
  private final RelNode plan;
  private final NonCacheableFunctionDetector.Result nonCacheableFunctionDetectorResults;

  public PreSerializedQuery(
      RelNode plan, NonCacheableFunctionDetector.Result nonCacheableFunctionDetectorResults) {
    this.plan = plan;
    this.nonCacheableFunctionDetectorResults = nonCacheableFunctionDetectorResults;
  }

  public RelNode getPlan() {
    return plan;
  }

  public NonCacheableFunctionDetector.Result getNonCacheableFunctionDetectorResults() {
    return nonCacheableFunctionDetectorResults;
  }

  public PreSerializedQuery withNewPlan(RelNode newPlan) {
    return new PreSerializedQuery(newPlan, nonCacheableFunctionDetectorResults);
  }
}
