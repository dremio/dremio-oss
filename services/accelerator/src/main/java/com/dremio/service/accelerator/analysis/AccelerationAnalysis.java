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
package com.dremio.service.accelerator.analysis;

import org.apache.calcite.rel.RelNode;

import com.dremio.service.accelerator.proto.DatasetAnalysis;
import com.google.common.base.Preconditions;

/**
 * A wrapper around analysis that is used to generate layouts.
 */
public class AccelerationAnalysis {
  private final DatasetAnalysis datasetAnalysis;
  private final RelNode plan;

  public AccelerationAnalysis(final DatasetAnalysis datasetAnalysis, final RelNode plan) {
    this.datasetAnalysis = Preconditions.checkNotNull(datasetAnalysis, "dataset analysis is required");
    this.plan = Preconditions.checkNotNull(plan, "plan is required");
  }

  public DatasetAnalysis getDatasetAnalysis() {
    return datasetAnalysis;
  }

  public RelNode getPlan() {
    return plan;
  }
}
