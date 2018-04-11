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
package com.dremio.exec.planner.cost;

import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMdExplainVisibility;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMdMemory;
import org.apache.calcite.rel.metadata.RelMdParallelism;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMdPopulationSize;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMdUniqueKeys;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.collect.ImmutableList;

public class DefaultRelMetadataProvider extends ChainedRelMetadataProvider {
  public static final RelMetadataProvider INSTANCE = new DefaultRelMetadataProvider();
  private DefaultRelMetadataProvider() {
    super(ImmutableList.of(
        // MetadataProvider for Projectable aggregates. Should be before default ones
        RelMdProjectableAggregate.SOURCE,
        // Mostly relies on Calcite default with some adjustments...
        RelMdRowCount.SOURCE,
        RelMdDistinctRowCount.SOURCE,
        RelMdPercentageOriginalRows.SOURCE,
        RelMdColumnOrigins.SOURCE,
        RelMdMaxRowCount.SOURCE,
        RelMdUniqueKeys.SOURCE,
        RelMdColumnUniqueness.SOURCE,
        RelMdPopulationSize.SOURCE,
        RelMdSize.SOURCE,
        RelMdParallelism.SOURCE,
        RelMdDistribution.SOURCE,
        RelMdMemory.SOURCE,
        RelMdSelectivity.SOURCE,
        RelMdExplainVisibility.SOURCE,
        RelMdPredicates.SOURCE,
        RelMdCollation.SOURCE));
  }

  @Override
  public RelMetadataQuery getRelMetadataQuery() {
    return new RelMetadataQuery(this) {};
  }
}
