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
package com.dremio.exec.physical.base;

import java.util.List;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionType;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.WriterUpdater;
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Writer options.
 */
public class WriterOptions {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WriterOptions.class);

  public static final WriterOptions DEFAULT = new WriterOptions(null, ImmutableList.<String>of(),
      ImmutableList.<String>of(), ImmutableList.<String>of(), PartitionDistributionStrategy.UNSPECIFIED, false,
      Long.MAX_VALUE);

  private final Integer ringCount;
  private final List<String> partitionColumns;
  private final List<String> sortColumns;
  private final List<String> distributionColumns;
  private final PartitionDistributionStrategy partitionDistributionStrategy;
  private final boolean singleWriter;
  private final long recordLimit;

  @JsonCreator
  public WriterOptions(
    @JsonProperty("ringCount") Integer ringCount,
    @JsonProperty("partitionColumns") List<String> partitionColumns,
    @JsonProperty("sortColumns") List<String> sortColumns,
    @JsonProperty("distributionColumns") List<String> distributionColumns,
    @JsonProperty("partitionDistributionStrategy") PartitionDistributionStrategy partitionDistributionStrategy,
    @JsonProperty("singleWriter") boolean singleWriter,
    @JsonProperty("recordLimit") long recordLimit) {
    this.ringCount = ringCount;
    this.partitionColumns = partitionColumns;
    this.sortColumns = sortColumns;
    this.distributionColumns = distributionColumns;
    this.partitionDistributionStrategy = partitionDistributionStrategy;
    this.singleWriter = singleWriter;
    this.recordLimit = recordLimit;
  }

  public Integer getRingCount() {
    return ringCount;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public List<String> getSortColumns() {
    return sortColumns;
  }

  public List<String> getDistributionColumns() {
    return distributionColumns;
  }

  public boolean isSingleWriter() {
    return singleWriter;
  }

  public long getRecordLimit() { return recordLimit; }

  public boolean hasDistributions() {
    return distributionColumns != null && !distributionColumns.isEmpty();
  }

  public boolean hasPartitions() {
    return partitionColumns != null && !partitionColumns.isEmpty();
  }

  public boolean hasSort() {
    return sortColumns != null && !sortColumns.isEmpty();
  }

  public WriterOptions withRecordLimit(long recordLimit) {
    return new WriterOptions(this.ringCount, this.partitionColumns, this.sortColumns, this.distributionColumns,
      this.partitionDistributionStrategy, this.singleWriter, recordLimit);
  }

  public RelTraitSet inferTraits(final RelTraitSet inputTraitSet, final RelDataType inputRowType) {
    final RelTraitSet relTraits = inputTraitSet.plus(Prel.PHYSICAL);

    if (hasDistributions()) {
      return relTraits.plus(hashDistributedOn(distributionColumns, inputRowType));
    }

    if (hasPartitions()) {
      switch (partitionDistributionStrategy) {

      case HASH:
        return relTraits.plus(hashDistributedOn(partitionColumns, inputRowType));

      case ROUND_ROBIN:
        return relTraits.plus(DistributionTrait.ROUND_ROBIN);

      case UNSPECIFIED:
      case STRIPED:
        // fall through ..
      }
    }

    return relTraits;
  }

  private static DistributionTrait hashDistributedOn(final List<String> columns, final RelDataType inputRowType) {
    return new DistributionTrait(DistributionType.HASH_DISTRIBUTED,
        FluentIterable.from(WriterUpdater.getFieldIndices(columns, inputRowType))
            .transform(new Function<Integer, DistributionField>() {
              @Override
              public DistributionField apply(Integer input) {
                return new DistributionField(input);
              }
            }).toList());
  }
}
