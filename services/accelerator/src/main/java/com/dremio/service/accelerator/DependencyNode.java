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
package com.dremio.service.accelerator;

import java.util.List;
import java.util.Objects;

import com.dremio.service.accelerator.proto.Layout;
import com.google.common.base.Preconditions;

/**
 * A node in the {@link DependencyGraph dependency graph}.
 */
public class DependencyNode {
  private final Layout layout;
  private final List<String> tableSchemaPath;
  private final long refreshPeriod; // in ms
  private final long gracePeriod; // in ms

  /**
   * Constructor for layout.
   *
   * @param layout layout
   */
  public DependencyNode(Layout layout) {
    this.layout = Preconditions.checkNotNull(layout);
    this.tableSchemaPath = null;
    this.refreshPeriod = -1;
    this.gracePeriod = -1;
  }

  /**
   * Constructor for physical data set.
   *
   * @param tableSchemaPath schema path
   * @param refreshPeriod refresh period
   */
  public DependencyNode(List<String> tableSchemaPath, long refreshPeriod, long gracePeriod) {
    this.tableSchemaPath = Preconditions.checkNotNull(tableSchemaPath);
    this.refreshPeriod = refreshPeriod;
    this.layout = null;
    this.gracePeriod = gracePeriod;
  }

  public Layout getLayout() {
    return layout;
  }

  public boolean isLayout() {
    return layout != null;
  }

  public boolean isPhysicalDataset() {
    return tableSchemaPath != null;
  }

  public long getRefreshPeriod() {
    return refreshPeriod;
  }

  public long getGracePeriod() {
    return gracePeriod;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    if (isLayout()) {
      builder.append("[layoutId = ")
      .append(layout.getId().getId())
      .append("]");
    } else {
      builder.append("[schemaPath = ")
          .append(tableSchemaPath.toString())
          .append(", ");
      builder.append("refreshPeriod = ")
          .append(refreshPeriod)
          .append(" ms]")
          .toString();
    }
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return isLayout() ? layout.getId().hashCode() : tableSchemaPath.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DependencyNode)) {
      return false;
    }
    DependencyNode that = (DependencyNode) obj;

    return Objects.equals(this.layout, that.layout) &&
        Objects.equals(this.tableSchemaPath, that.tableSchemaPath);
  }
}
