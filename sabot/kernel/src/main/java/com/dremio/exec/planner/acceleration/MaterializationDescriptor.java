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
package com.dremio.exec.planner.acceleration;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.proto.UserBitShared.MeasureColumn;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * A wrapper around materialized SQL query, replacement and a handle that points to acceleration entry in persistent store.
 */
public class MaterializationDescriptor {
  protected final ReflectionInfo reflection;
  protected final List<String> path;

  private final String materializationId;
  private final String version;
  private final long expirationTimestamp;
  private final byte[] planBytes;
  private final Long strippedPlanHash;
  private final int stripVersion;
  private final double originalCost;
  private final long jobStart;
  private final List<String> partition;
  private final IncrementalUpdateSettings incrementalUpdateSettings;
  private final JoinDependencyProperties joinDependencyProperties;

  public MaterializationDescriptor(
      final ReflectionInfo reflection,
      final String materializationId,
      final String version,
      final long expirationTimestamp,
      final byte[] planBytes,
      final List<String> path,
      final @Nullable Double originalCost,
      final long jobStart,
      final List<String> partition,
      final IncrementalUpdateSettings incrementalUpdateSettings,
      final JoinDependencyProperties joinDependencyProperties,
      Long strippedPlanHash,
      Integer stripVersion) {
    this.reflection = Preconditions.checkNotNull(reflection, "reflection info required");
    this.materializationId = Preconditions.checkNotNull(materializationId, "materialization id is required");
    this.version = version;
    this.expirationTimestamp = expirationTimestamp;
    this.planBytes = planBytes;
    this.path = ImmutableList.copyOf(Preconditions.checkNotNull(path, "path is required"));
    this.originalCost = originalCost == null ? 0 : originalCost;
    this.jobStart = jobStart;
    this.partition = partition;
    this.incrementalUpdateSettings = incrementalUpdateSettings;
    this.joinDependencyProperties = joinDependencyProperties;
    this.strippedPlanHash = strippedPlanHash;
    this.stripVersion = Optional.ofNullable(stripVersion).orElse(strippedPlanHash == null ? 0 : 1);
  }

  public ReflectionType getReflectionType() {
    return reflection.getType();
  }

  public Long getStrippedPlanHash() {
    return strippedPlanHash;
  }

  public int getStripVersion() {
    return stripVersion;
  }

  public String getVersion() {
    return version;
  }

  public long getExpirationTimestamp() {
    return expirationTimestamp;
  }

  public String getMaterializationId() {
    return materializationId;
  }

  public String getLayoutId() {
    return reflection.getReflectionId();
  }

  ReflectionInfo getLayoutInfo(){
    return reflection;
  }

  public byte[] getPlan() {
    return planBytes;
  }

  public List<String> getPath() {
    return path;
  }

  IncrementalUpdateSettings getIncrementalUpdateSettings() {
    return incrementalUpdateSettings;
  }

  JoinDependencyProperties getJoinDependencyProperties() {
    return joinDependencyProperties;
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
        Objects.equals(reflection, that.reflection) &&
        Objects.deepEquals(planBytes, that.planBytes) &&
        Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(reflection, Arrays.hashCode(planBytes), path, originalCost);
  }

  public DremioMaterialization getMaterializationFor(SqlConverter converter) {
    final MaterializationExpander expander = MaterializationExpander.of(converter);
    return expander.expand(this);
  }

  public long getJobStart() {
    return jobStart;
  }

  public List<String> getPartition() {
    return partition;
  }

  public static class ReflectionInfo {
    private final String reflectionId;
    private final ReflectionType type;
    private final String name;
    private final boolean arrowCachingEnabled;
    private final List<String> sortColumns;
    private final List<String> partitionColumns;
    private final List<String> distributionColumns;
    private final List<String> dimensions;
    private final List<MeasureColumn> measures;
    private final List<String> displayColumns;

    public ReflectionInfo(
        String reflectionId,
        ReflectionType type,
        String name,
        boolean arrowCachingEnabled,
        List<String> sortColumns,
        List<String> partitionColumns,
        List<String> distributionColumns,
        List<String> dimensions,
        List<MeasureColumn> measures,
        List<String> displayColumns) {
      super();
      this.name = name;
      this.type = type;
      this.reflectionId = reflectionId;
      this.arrowCachingEnabled = arrowCachingEnabled;
      this.sortColumns = sortColumns == null ? ImmutableList.of() : ImmutableList.copyOf(sortColumns);
      this.partitionColumns = partitionColumns == null ? ImmutableList.of() : ImmutableList.copyOf(partitionColumns);
      this.distributionColumns = distributionColumns == null ? ImmutableList.of() : ImmutableList.copyOf(distributionColumns);
      this.dimensions = dimensions == null ? ImmutableList.of() : ImmutableList.copyOf(dimensions);
      this.measures = measures == null ? ImmutableList.of() : ImmutableList.copyOf(measures);
      this.displayColumns = displayColumns == null ? ImmutableList.of() : ImmutableList.copyOf(displayColumns);
    }

    public String getReflectionId() {
      return reflectionId;
    }

    public List<String> getDimensions() {
      return dimensions;
    }

    public List<MeasureColumn> getMeasures() {
      return measures;
    }

    public List<String> getDisplayColumns() {
      return displayColumns;
    }

    public List<String> getSortColumns() {
      return sortColumns;
    }

    public String getName() {
      return name;
    }

    public boolean isArrowCachingEnabled() {
      return arrowCachingEnabled;
    }

    public ReflectionType getType() {
      return type;
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
      final ReflectionInfo that = (ReflectionInfo) o;
      return
          Objects.equals(reflectionId, that.reflectionId) &&
          Objects.equals(sortColumns, that.sortColumns) &&
          Objects.deepEquals(partitionColumns, that.partitionColumns) &&
          Objects.equals(distributionColumns, that.distributionColumns) &&
          Objects.equals(dimensions, that.dimensions) &&
          Objects.equals(measures, that.measures) &&
          Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(reflectionId, sortColumns, partitionColumns, distributionColumns, name);
    }

  }
}
