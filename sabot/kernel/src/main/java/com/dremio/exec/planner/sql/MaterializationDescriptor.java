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
package com.dremio.exec.planner.sql;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * A wrapper around materialized SQL query, replacement and a handle that points to acceleration entry in persistent store.
 */
public class MaterializationDescriptor {
  private final String accelerationId;
  private final LayoutInfo layout;
  private final String materializationId;
  private final Long updateId;
  private final long expirationTimestamp;
  private final byte[] planBytes;
  private final List<String> path;
  private final double originalCost;
  private final IncrementalUpdateSettings incrementalUpdateSettings;

  @VisibleForTesting
  public MaterializationDescriptor(final String accelerationId,
                                   final LayoutInfo layout,
                                   final String materializationId,
                                   final Long updateId,
                                   final long expirationTimestamp,
                                   final byte[] planBytes,
                                   final List<String> path,
                                   final @Nullable Double originalCost) {
    this(accelerationId, layout, materializationId, updateId, expirationTimestamp, planBytes, path, originalCost, IncrementalUpdateSettings.NON_INCREMENTAL);
  }

  public MaterializationDescriptor(final String accelerationId,
                                   final LayoutInfo layout,
                                   final String materializationId,
                                   final Long updateId,
                                   final long expirationTimestamp,
                                   final byte[] planBytes,
                                   final List<String> path,
                                   final @Nullable Double originalCost,
                                   final IncrementalUpdateSettings incrementalUpdateSettings) {
    this.accelerationId = Preconditions.checkNotNull(accelerationId, "request id is required");
    this.layout = Preconditions.checkNotNull(layout, "acceleration id is required");
    this.materializationId = Preconditions.checkNotNull(materializationId);
    this.updateId = updateId;
    this.expirationTimestamp = expirationTimestamp;
    this.planBytes = Preconditions.checkNotNull(planBytes, "plan is required");
    this.path = ImmutableList.copyOf(Preconditions.checkNotNull(path, "path is required"));
    this.originalCost = originalCost == null ? 0 : originalCost;
    this.incrementalUpdateSettings = incrementalUpdateSettings;
  }

  public long getExpirationTimestamp() {
    return expirationTimestamp;
  }

  public String getMaterializationId() {
    return materializationId;
  }

  public Long getUpdateId() {
    return updateId;
  }

  public String getAccelerationId() {
    return accelerationId;
  }

  public String getLayoutId() {
    return layout.getLayoutId();
  }

  public LayoutInfo getLayoutInfo(){
    return layout;
  }

  public byte[] getPlan() {
    return planBytes;
  }

  public List<String> getPath() {
    return path;
  }

  public IncrementalUpdateSettings getIncrementalUpdateSettings() {
    return incrementalUpdateSettings;
  }

  /**
   * Returns original cost of running raw query defined at {@link #getPlan()} if defined or zero.
   */
  public double getOriginalCost() {
    return originalCost;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MaterializationDescriptor that = (MaterializationDescriptor) o;
    return Double.compare(that.originalCost, originalCost) == 0 &&
        Objects.equals(accelerationId, that.accelerationId) &&
        Objects.equals(layout, that.layout) &&
        Objects.deepEquals(planBytes, that.planBytes) &&
        Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accelerationId, layout, planBytes, path, originalCost);
  }

  public DremioRelOptMaterialization getMaterializationFor(SqlConverter converter) {
    final MaterializationExpander expander = MaterializationExpander.of(converter);
    return expander.expand(this).orNull();
  }

  public static class LayoutInfo {
    private final String layoutId;
    private final List<String> sortColumns;
    private final List<String> partitionColumns;
    private final List<String> distributionColumns;
    private final List<String> dimensions;
    private final List<String> measures;
    private final List<String> displayColumns;

    public LayoutInfo(String layoutId,
                      List<String> sortColumns,
                      List<String> partitionColumns,
                      List<String> distributionColumns,
                      List<String> dimensions,
                      List<String> measures,
                      List<String> displayColumns) {
      super();
      this.layoutId = layoutId;
      this.sortColumns = sortColumns == null ? ImmutableList.<String>of() : ImmutableList.<String>copyOf(sortColumns);
      this.partitionColumns = partitionColumns == null ? ImmutableList.<String>of() : ImmutableList.<String>copyOf(partitionColumns);;
      this.distributionColumns = distributionColumns == null ? ImmutableList.<String>of() : ImmutableList.<String>copyOf(distributionColumns);
      this.dimensions = dimensions == null ? ImmutableList.<String>of() : ImmutableList.<String>copyOf(dimensions);
      this.measures = measures == null ? ImmutableList.<String>of() : ImmutableList.<String>copyOf(measures);
      this.displayColumns = displayColumns == null ? ImmutableList.<String>of() : ImmutableList.<String>copyOf(displayColumns);
    }

    public String getLayoutId() {
      return layoutId;
    }

    public List<String> getDimensions() {
      return dimensions;
    }

    public List<String> getMeasures() {
      return measures;
    }

    public List<String> getDisplayColumns() {
      return displayColumns;
    }

    public List<String> getSortColumns() {
      return sortColumns;
    }

    public List<String> getPartitionColumns() {
      return partitionColumns;
    }

    public List<String> getDistributionColumns() {
      return distributionColumns;
    }


    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final LayoutInfo that = (LayoutInfo) o;
      return
          Objects.equals(layoutId, that.layoutId) &&
          Objects.equals(sortColumns, that.sortColumns) &&
          Objects.deepEquals(partitionColumns, that.partitionColumns) &&
          Objects.equals(distributionColumns, that.distributionColumns) &&
          Objects.equals(dimensions, that.dimensions) &&
          Objects.equals(measures, that.measures);
    }

    @Override
    public int hashCode() {
      return Objects.hash(layoutId, sortColumns, partitionColumns, distributionColumns);
    }

  }
}
