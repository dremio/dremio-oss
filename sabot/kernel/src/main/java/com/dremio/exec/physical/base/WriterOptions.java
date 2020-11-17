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

import io.protostuff.ByteString;

/**
 * Writer options.
 */
public class WriterOptions {
  public enum IcebergWriterOperation {
    NONE,
    CREATE,
    INSERT
  }
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WriterOptions.class);

  public static final WriterOptions DEFAULT = new WriterOptions(null, ImmutableList.<String>of(),
      ImmutableList.<String>of(), ImmutableList.<String>of(), PartitionDistributionStrategy.UNSPECIFIED, false,
      Long.MAX_VALUE, IcebergWriterOperation.NONE, null);

  private final Integer ringCount;
  private final List<String> partitionColumns;
  private final List<String> sortColumns;
  private final List<String> distributionColumns;
  private final PartitionDistributionStrategy partitionDistributionStrategy;
  private final boolean singleWriter;
  private long recordLimit;
  // output limit per query from the PlannerSettings.OUTPUT_LIMIT_SIZE
  private final long outputLimitSize;
  private final IcebergWriterOperation icebergWriterOperation;
  private final ByteString extendedProperty;
  private final boolean outputLimitEnabled;

  public WriterOptions(
    Integer ringCount,
    List<String> partitionColumns,
    List<String> sortColumns,
    List<String> distributionColumns,
    PartitionDistributionStrategy partitionDistributionStrategy,
    boolean singleWriter,
    long recordLimit) {
    this(ringCount, partitionColumns, sortColumns, distributionColumns,
      partitionDistributionStrategy, singleWriter, recordLimit, IcebergWriterOperation.NONE, null);
  }

  public WriterOptions(
    Integer ringCount,
    List<String> partitionColumns,
    List<String> sortColumns,
    List<String> distributionColumns,
    PartitionDistributionStrategy partitionDistributionStrategy,
    boolean singleWriter,
    long recordLimit,
    IcebergWriterOperation icebergWriterOperation,
    ByteString extendedProperty
  ) {
    this(ringCount, partitionColumns, sortColumns, distributionColumns, partitionDistributionStrategy,
         singleWriter, recordLimit, icebergWriterOperation, extendedProperty, false, Long.MAX_VALUE);
  }

  @JsonCreator
  public WriterOptions(
    @JsonProperty("ringCount") Integer ringCount,
    @JsonProperty("partitionColumns") List<String> partitionColumns,
    @JsonProperty("sortColumns") List<String> sortColumns,
    @JsonProperty("distributionColumns") List<String> distributionColumns,
    @JsonProperty("partitionDistributionStrategy") PartitionDistributionStrategy partitionDistributionStrategy,
    @JsonProperty("singleWriter") boolean singleWriter,
    @JsonProperty("recordLimit") long recordLimit,
    @JsonProperty("icebergWriterOperation") IcebergWriterOperation icebergWriterOperation,
    @JsonProperty("extendedProperty") ByteString extendedProperty,
    @JsonProperty("outputLimitEnabled") boolean outputLimitEnabled,
    @JsonProperty("outputLimitSize") long outputLimitSize
    ) {
    this.ringCount = ringCount;
    this.partitionColumns = partitionColumns;
    this.sortColumns = sortColumns;
    this.distributionColumns = distributionColumns;
    this.partitionDistributionStrategy = partitionDistributionStrategy;
    this.singleWriter = singleWriter;
    this.recordLimit = recordLimit;
    this.icebergWriterOperation = icebergWriterOperation;
    this.extendedProperty = extendedProperty;
    this.outputLimitEnabled = outputLimitEnabled;
    this.outputLimitSize = outputLimitSize;
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

  public void setRecordLimit(long recordLimit) {
    this.recordLimit = recordLimit;
  }

  public long getRecordLimit() { return recordLimit; }

  public boolean isOutputLimitEnabled() {
    return outputLimitEnabled;
  }

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
      this.partitionDistributionStrategy, this.singleWriter, recordLimit, this.icebergWriterOperation, this.extendedProperty);
  }

  public long getOutputLimitSize() {
    return outputLimitSize;
  }

  public WriterOptions withOutputLimitEnabled(boolean outputLimitEnabled) {
    return new WriterOptions(this.ringCount, this.partitionColumns, this.sortColumns, this.distributionColumns,
                             this.partitionDistributionStrategy, this.singleWriter, this.recordLimit,
                             this.icebergWriterOperation, this.extendedProperty, outputLimitEnabled, this.outputLimitSize);
  }

  public WriterOptions withOutputLimitSize(long outputLimitSize) {
    return new WriterOptions(this.ringCount, this.partitionColumns, this.sortColumns, this.distributionColumns,
                             this.partitionDistributionStrategy, this.singleWriter, this.recordLimit,
                             this.icebergWriterOperation, this.extendedProperty, this.outputLimitEnabled, outputLimitSize);
  }

  public WriterOptions withPartitionColumns(List<String> partitionColumns) {
    return new WriterOptions(this.ringCount, partitionColumns, this.sortColumns, this.distributionColumns,
      this.partitionDistributionStrategy, this.singleWriter, this.recordLimit, this.icebergWriterOperation, this.extendedProperty);
  }

  public IcebergWriterOperation getIcebergWriterOperation() {
    return this.icebergWriterOperation;
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


  public ByteString getExtendedProperty() {
    return extendedProperty;
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
