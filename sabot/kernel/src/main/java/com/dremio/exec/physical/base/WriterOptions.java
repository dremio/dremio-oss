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

import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;
import static com.dremio.exec.store.iceberg.IcebergUtils.hasNonIdentityPartitionColumns;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionType;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.WriterUpdater;
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;

/** Writer options. */
// TODO: Convert to @Value.Immutable
@JsonIgnoreProperties(
    value = {"partitionSpec"},
    allowGetters = true)
public class WriterOptions {
  public static final WriterOptions DEFAULT =
      new WriterOptions(
          null,
          ImmutableList.<String>of(),
          ImmutableList.<String>of(),
          ImmutableList.<String>of(),
          PartitionDistributionStrategy.UNSPECIFIED,
          null,
          false,
          Long.MAX_VALUE,
          TableFormatWriterOptions.makeDefault(),
          null);

  private final Integer ringCount;
  private final List<String> partitionColumns;
  private List<String> sortColumns;
  private final List<String> distributionColumns;
  private final PartitionDistributionStrategy partitionDistributionStrategy;
  private final String tableLocation;
  private final boolean singleWriter;
  private long recordLimit;
  // output limit per query from the PlannerSettings.OUTPUT_LIMIT_SIZE
  private final long outputLimitSize;
  private final ByteString extendedProperty;
  private final boolean outputLimitEnabled;
  private final boolean readSignatureSupport;
  private ResolvedVersionContext version;
  private TableFormatWriterOptions tableFormatOptions;
  private final CombineSmallFileOptions combineSmallFileOptions;
  private final Map<String, String> tableProperties;

  public WriterOptions(
      Integer ringCount,
      List<String> partitionColumns,
      List<String> sortColumns,
      List<String> distributionColumns,
      PartitionDistributionStrategy partitionDistributionStrategy,
      boolean singleWriter,
      long recordLimit) {
    this(
        ringCount,
        partitionColumns,
        sortColumns,
        distributionColumns,
        partitionDistributionStrategy,
        null,
        singleWriter,
        recordLimit,
        TableFormatWriterOptions.makeDefault(),
        null);
  }

  public WriterOptions(
      Integer ringCount,
      List<String> partitionColumns,
      List<String> sortColumns,
      List<String> distributionColumns,
      PartitionDistributionStrategy partitionDistributionStrategy,
      boolean singleWriter,
      long recordLimit,
      TableFormatWriterOptions tableFormatOptions,
      ByteString extendedProperty,
      boolean readSignatureSupport) {
    this(
        ringCount,
        partitionColumns,
        sortColumns,
        distributionColumns,
        partitionDistributionStrategy,
        null,
        singleWriter,
        recordLimit,
        tableFormatOptions,
        extendedProperty,
        false,
        Long.MAX_VALUE,
        readSignatureSupport,
        null,
        null,
        Collections.emptyMap());
  }

  public WriterOptions(
      Integer ringCount,
      List<String> partitionColumns,
      List<String> sortColumns,
      List<String> distributionColumns,
      PartitionDistributionStrategy partitionDistributionStrategy,
      String tableLocation,
      boolean singleWriter,
      long recordLimit,
      TableFormatWriterOptions tableFormatOptions,
      ByteString extendedProperty) {
    this(
        ringCount,
        partitionColumns,
        sortColumns,
        distributionColumns,
        partitionDistributionStrategy,
        tableLocation,
        singleWriter,
        recordLimit,
        tableFormatOptions,
        extendedProperty,
        false,
        Long.MAX_VALUE,
        true,
        null,
        null,
        Collections.emptyMap());
  }

  public WriterOptions(
      Integer ringCount,
      List<String> partitionColumns,
      List<String> sortColumns,
      List<String> distributionColumns,
      PartitionDistributionStrategy partitionDistributionStrategy,
      String tableLocation,
      boolean singleWriter,
      long recordLimit,
      TableFormatWriterOptions tableFormatOptions,
      ByteString extendedProperty,
      Map<String, String> tableProperties) {
    this(
        ringCount,
        partitionColumns,
        sortColumns,
        distributionColumns,
        partitionDistributionStrategy,
        tableLocation,
        singleWriter,
        recordLimit,
        tableFormatOptions,
        extendedProperty,
        false,
        Long.MAX_VALUE,
        true,
        null,
        null,
        tableProperties);
  }

  public WriterOptions(
      Integer ringCount,
      List<String> partitionColumns,
      List<String> sortColumns,
      List<String> distributionColumns,
      PartitionDistributionStrategy partitionDistributionStrategy,
      String tableLocation,
      boolean singleWriter,
      long recordLimit,
      TableFormatWriterOptions tableFormatOptions,
      ByteString extendedProperty,
      ResolvedVersionContext version,
      Map<String, String> tableProperties) {
    this(
        ringCount,
        partitionColumns,
        sortColumns,
        distributionColumns,
        partitionDistributionStrategy,
        tableLocation,
        singleWriter,
        recordLimit,
        tableFormatOptions,
        extendedProperty,
        false,
        Long.MAX_VALUE,
        true,
        version,
        null,
        tableProperties);
  }

  public WriterOptions(
      Integer ringCount,
      List<String> partitionColumns,
      List<String> sortColumns,
      List<String> distributionColumns,
      PartitionDistributionStrategy partitionDistributionStrategy,
      String tableLocation,
      boolean singleWriter,
      long recordLimit,
      TableFormatWriterOptions tableFormatOptions,
      ByteString extendedProperty,
      ResolvedVersionContext version) {
    this(
        ringCount,
        partitionColumns,
        sortColumns,
        distributionColumns,
        partitionDistributionStrategy,
        tableLocation,
        singleWriter,
        recordLimit,
        tableFormatOptions,
        extendedProperty,
        false,
        Long.MAX_VALUE,
        true,
        version,
        null,
        Collections.emptyMap());
  }

  @JsonCreator
  public WriterOptions(
      @JsonProperty("ringCount") Integer ringCount,
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("sortColumns") List<String> sortColumns,
      @JsonProperty("distributionColumns") List<String> distributionColumns,
      @JsonProperty("partitionDistributionStrategy")
          PartitionDistributionStrategy partitionDistributionStrategy,
      @JsonProperty("tableLocation") String tableLocation,
      @JsonProperty("singleWriter") boolean singleWriter,
      @JsonProperty("recordLimit") long recordLimit,
      @JsonProperty("tableFormatOptions") TableFormatWriterOptions tableFormatOptions,
      @JsonProperty("extendedProperty") ByteString extendedProperty,
      @JsonProperty("outputLimitEnabled") boolean outputLimitEnabled,
      @JsonProperty("outputLimitSize") long outputLimitSize,
      @JsonProperty("readSignatureSupport") Boolean readSignatureSupport,
      @JsonProperty("versionContext") ResolvedVersionContext version,
      @JsonProperty("combineSmallFileOptions") CombineSmallFileOptions combineSmallFileOptions,
      @JsonProperty("tableProperties") Map<String, String> tableProperties) {
    this.ringCount = ringCount;
    this.partitionColumns = partitionColumns;
    this.sortColumns = sortColumns;
    this.distributionColumns = distributionColumns;
    this.partitionDistributionStrategy = partitionDistributionStrategy;
    this.tableLocation = tableLocation;
    this.singleWriter = singleWriter;
    this.recordLimit = recordLimit;
    this.extendedProperty = extendedProperty;
    this.outputLimitEnabled = outputLimitEnabled;
    this.outputLimitSize = outputLimitSize;
    this.readSignatureSupport = readSignatureSupport;
    this.version = version;
    this.tableFormatOptions = tableFormatOptions;
    this.combineSmallFileOptions = combineSmallFileOptions;
    this.tableProperties = tableProperties;
  }

  public CombineSmallFileOptions getCombineSmallFileOptions() {
    return combineSmallFileOptions;
  }

  public Integer getRingCount() {
    return ringCount;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public String getTableLocation() {
    return tableLocation;
  }

  public List<String> getSortColumns() {
    return sortColumns;
  }

  public void setSortColumns(List<String> sortColumns) {
    this.sortColumns = sortColumns;
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

  public long getRecordLimit() {
    return recordLimit;
  }

  public boolean isOutputLimitEnabled() {
    return outputLimitEnabled;
  }

  public ResolvedVersionContext getVersion() {
    return version;
  }

  public boolean hasDistributions() {
    return distributionColumns != null && !distributionColumns.isEmpty();
  }

  public boolean hasPartitions() {
    return partitionColumns != null && !partitionColumns.isEmpty();
  }

  public PartitionSpec getPartitionSpec() {
    return Optional.ofNullable(
            getTableFormatOptions().getIcebergSpecificOptions().getIcebergTableProps())
        .map(props -> props.getDeserializedPartitionSpec())
        .orElse(null);
  }

  public boolean hasSort() {
    return sortColumns != null && !sortColumns.isEmpty();
  }

  public void setTableFormatOptions(TableFormatWriterOptions icebergWriterOptions) {
    this.tableFormatOptions = icebergWriterOptions;
  }

  public void setResolvedVersionContext(ResolvedVersionContext resolvedVersionContext) {
    this.version = resolvedVersionContext;
  }

  public WriterOptions withRecordLimit(long recordLimit) {
    return new WriterOptions(
        this.ringCount,
        this.partitionColumns,
        this.sortColumns,
        this.distributionColumns,
        this.partitionDistributionStrategy,
        this.tableLocation,
        this.singleWriter,
        recordLimit,
        this.tableFormatOptions,
        this.extendedProperty);
  }

  public long getOutputLimitSize() {
    return outputLimitSize;
  }

  public WriterOptions withOutputLimitEnabled(boolean outputLimitEnabled) {
    return new WriterOptions(
        this.ringCount,
        this.partitionColumns,
        this.sortColumns,
        this.distributionColumns,
        this.partitionDistributionStrategy,
        this.tableLocation,
        this.singleWriter,
        this.recordLimit,
        this.tableFormatOptions,
        this.extendedProperty,
        outputLimitEnabled,
        this.outputLimitSize,
        this.readSignatureSupport,
        null,
        null,
        this.getTableProperties());
  }

  public WriterOptions withOutputLimitSize(long outputLimitSize) {
    return new WriterOptions(
        this.ringCount,
        this.partitionColumns,
        this.sortColumns,
        this.distributionColumns,
        this.partitionDistributionStrategy,
        this.tableLocation,
        this.singleWriter,
        this.recordLimit,
        this.tableFormatOptions,
        this.extendedProperty,
        this.outputLimitEnabled,
        outputLimitSize,
        this.readSignatureSupport,
        null,
        null,
        this.getTableProperties());
  }

  public WriterOptions withPartitionColumns(List<String> partitionColumns) {
    return new WriterOptions(
        this.ringCount,
        partitionColumns,
        this.sortColumns,
        this.distributionColumns,
        this.partitionDistributionStrategy,
        this.tableLocation,
        this.singleWriter,
        this.recordLimit,
        this.tableFormatOptions,
        this.extendedProperty,
        this.getTableProperties());
  }

  public WriterOptions withVersion(ResolvedVersionContext version) {
    return new WriterOptions(
        this.ringCount,
        this.partitionColumns,
        this.sortColumns,
        this.distributionColumns,
        this.partitionDistributionStrategy,
        this.tableLocation,
        this.singleWriter,
        recordLimit,
        this.tableFormatOptions,
        this.extendedProperty,
        false,
        Long.MAX_VALUE,
        readSignatureSupport,
        Preconditions.checkNotNull(version),
        null,
        this.getTableProperties());
  }

  public WriterOptions withCombineSmallFileOptions(
      CombineSmallFileOptions combineSmallFileOptions) {
    return new WriterOptions(
        this.ringCount,
        this.partitionColumns,
        this.sortColumns,
        this.distributionColumns,
        this.partitionDistributionStrategy,
        this.tableLocation,
        this.singleWriter,
        recordLimit,
        this.tableFormatOptions,
        this.extendedProperty,
        this.outputLimitEnabled,
        this.outputLimitSize,
        readSignatureSupport,
        version,
        combineSmallFileOptions,
        this.getTableProperties());
  }

  public TableFormatWriterOptions getTableFormatOptions() {
    return this.tableFormatOptions;
  }

  public RelTraitSet inferTraits(
      final RelTraitSet inputTraitSet, final RelDataType inputRowType, boolean roundRobinDefault) {
    final RelTraitSet relTraits = inputTraitSet.plus(Prel.PHYSICAL);

    if (hasDistributions()) {
      return relTraits.plus(hashDistributedOn(distributionColumns, inputRowType));
    }

    if (hasPartitions() && !hasNonIdentityPartitionColumns(getPartitionSpec())) {
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

    if (roundRobinDefault) {
      return relTraits.plus(DistributionTrait.ROUND_ROBIN);
    }
    return relTraits;
  }

  public ByteString getExtendedProperty() {
    return extendedProperty;
  }

  public boolean isReadSignatureSupport() {
    return readSignatureSupport;
  }

  public static DistributionTrait hashDistributedOn(
      final List<String> columns, final RelDataType inputRowType) {
    return new DistributionTrait(
        DistributionType.ADAPTIVE_HASH_DISTRIBUTED,
        FluentIterable.from(WriterUpdater.getFieldIndices(columns, inputRowType))
            .transform(input -> new DistributionField(input))
            .toList());
  }

  @JsonIgnore
  public SortOrder getDeserializedSortOrder() {
    String serializedSortOrder = null;
    String icebergSchema = null;

    if (tableFormatOptions.getIcebergSpecificOptions() != null
        && tableFormatOptions.getIcebergSpecificOptions().getIcebergTableProps() != null) {
      serializedSortOrder =
          tableFormatOptions.getIcebergSpecificOptions().getIcebergTableProps().getSortOrder();
      icebergSchema =
          tableFormatOptions.getIcebergSpecificOptions().getIcebergTableProps().getIcebergSchema();
    }

    return serializedSortOrder == null || icebergSchema == null
        ? null
        : IcebergSerDe.deserializeSortOrderFromJson(
            deserializedJsonAsSchema(icebergSchema), serializedSortOrder);
  }

  @Override
  public String toString() {
    return "WriterOptions{"
        + " tableLocation='"
        + tableLocation
        + ", outputLimitSize="
        + outputLimitSize
        + ", icebergWriterOptions="
        + tableFormatOptions
        + ", version="
        + version
        + '}';
  }

  public Map<String, String> getTableProperties() {
    return tableProperties;
  }
}
