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

package com.dremio.exec.planner.acceleration.descriptor;

import com.dremio.exec.proto.UserBitShared.MeasureColumn;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

public class ReflectionInfo {
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
    this.partitionColumns =
        partitionColumns == null ? ImmutableList.of() : ImmutableList.copyOf(partitionColumns);
    this.distributionColumns =
        distributionColumns == null
            ? ImmutableList.of()
            : ImmutableList.copyOf(distributionColumns);
    this.dimensions = dimensions == null ? ImmutableList.of() : ImmutableList.copyOf(dimensions);
    this.measures = measures == null ? ImmutableList.of() : ImmutableList.copyOf(measures);
    this.displayColumns =
        displayColumns == null ? ImmutableList.of() : ImmutableList.copyOf(displayColumns);
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
    return Objects.equals(reflectionId, that.reflectionId)
        && Objects.equals(sortColumns, that.sortColumns)
        && Objects.deepEquals(partitionColumns, that.partitionColumns)
        && Objects.equals(distributionColumns, that.distributionColumns)
        && Objects.equals(dimensions, that.dimensions)
        && Objects.equals(measures, that.measures)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(reflectionId, sortColumns, partitionColumns, distributionColumns, name);
  }
}
