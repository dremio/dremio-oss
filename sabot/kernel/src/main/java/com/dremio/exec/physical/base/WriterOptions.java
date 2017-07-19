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
package com.dremio.exec.physical.base;

import java.util.List;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Describes a set of traits to apply to a given writer.
 */
public class WriterOptions {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WriterOptions.class);

  public static WriterOptions DEFAULT = new WriterOptions(null, ImmutableList.<String>of(), false, false, ImmutableList.<String>of(), ImmutableList.<String>of());

  private final Integer ringCount;
  private final List<String> partitionColumns;
  private final List<String> sortColumns;
  private final List<String> distributionColumns;
  private final boolean hashPartition;
  private final boolean roundRobinPartition;

  @JsonCreator
  public WriterOptions(
      @JsonProperty("ringCount") Integer ringCount,
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("hashPartition") boolean hashPartition,
      @JsonProperty("roundRobinPartition") boolean roundRobinPartition,
      @JsonProperty("sortColumns") List<String> sortColumns,
      @JsonProperty("distributionColumns") List<String> distributionColumns) {
    super();
    this.ringCount = ringCount;
    this.partitionColumns = partitionColumns;
    this.sortColumns = sortColumns;
    this.distributionColumns = distributionColumns;
    this.hashPartition = hashPartition;
    this.roundRobinPartition = roundRobinPartition;
  }

  public boolean isHashPartition() {
    return hashPartition;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public RelTraitSet getTraitSetWithPartition(RelTraitSet inputTraitSet, RelNode input) {
    if (!hasPartitions()) {
      return inputTraitSet;
    }
    if (hashPartition) {
      return inputTraitSet.plus(getDistribution(getFieldIndices(partitionColumns, input)));
    }
    if (roundRobinPartition) {
      return inputTraitSet.plus(DistributionTrait.ROUND_ROBIN);
    }
    return inputTraitSet;
  }

  public List<String> getSortColumns() {
    return sortColumns;
  }

  public List<String> getDistributionColumns() {
    return distributionColumns;
  }

  public Integer getRingCount(){
    return ringCount;
  }

  public boolean hasDistributions() {
    return distributionColumns != null && !distributionColumns.isEmpty();
  }


  public boolean hasPartitions(){
    return partitionColumns != null && !partitionColumns.isEmpty();
  }

  public boolean hasSort(){
    return sortColumns != null && !sortColumns.isEmpty();
  }

  public static RelCollation getCollation(RelTraitSet set, List<Integer> keys){
    List<RelFieldCollation> fields = Lists.newArrayList();
    for (int key : keys) {
      fields.add(new RelFieldCollation(key));
    }
    return set.canonize(RelCollationImpl.of(fields));
  }

  public static DistributionTrait getDistribution(List<Integer> keys) {
    List<DistributionField> fields = Lists.newArrayList();
    for (int key : keys) {
      fields.add(new DistributionField(key));
    }
    return new DistributionTrait(DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(fields));
  }

  public static List<Integer> getFieldIndices(List<String> columns, RelNode input){
    final List<Integer> keys = Lists.newArrayList();
    final RelDataType inputRowType = input.getRowType();
    for (final String col : columns) {
      final RelDataTypeField field = inputRowType.getField(col, false, false);
      Preconditions.checkArgument(field != null, String.format("partition col %s could not be resolved in table's column lists!", col));
      keys.add(field.getIndex());
    }

    return keys;
  }



}
