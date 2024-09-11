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

import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A wrapper around materialized SQL query, replacement and a handle that points to acceleration
 * entry in persistent store.
 */
public abstract class BaseMaterializationDescriptor implements MaterializationDescriptor {
  protected final ReflectionInfo reflection;
  protected final List<String> path;

  private final String materializationId;
  private final String version;
  private final long expirationTimestamp;
  private final double originalCost;
  private final long jobStart;
  private final List<String> partition;
  private final IncrementalUpdateSettings incrementalUpdateSettings;
  private final boolean isStale;

  public BaseMaterializationDescriptor(
      final ReflectionInfo reflection,
      final String materializationId,
      final String version,
      final long expirationTimestamp,
      final List<String> path,
      final @Nullable Double originalCost,
      final long jobStart,
      final List<String> partition,
      final IncrementalUpdateSettings incrementalUpdateSettings,
      final boolean isStale) {
    this.reflection = Preconditions.checkNotNull(reflection, "reflection info required");
    this.materializationId =
        Preconditions.checkNotNull(materializationId, "materialization id is required");
    this.version = version;
    this.expirationTimestamp = expirationTimestamp;
    this.path = ImmutableList.copyOf(Preconditions.checkNotNull(path, "path is required"));
    this.originalCost = originalCost == null ? 0 : originalCost;
    this.jobStart = jobStart;
    this.partition = partition;
    this.incrementalUpdateSettings = incrementalUpdateSettings;
    this.isStale = isStale;
  }

  @Override
  public ReflectionType getReflectionType() {
    return reflection.getType();
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Override
  public long getExpirationTimestamp() {
    return expirationTimestamp;
  }

  @Override
  public String getMaterializationId() {
    return materializationId;
  }

  @Override
  public String getLayoutId() {
    return reflection.getReflectionId();
  }

  @Override
  public ReflectionInfo getLayoutInfo() {
    return reflection;
  }

  @Override
  public List<String> getPath() {
    return path;
  }

  @Override
  public IncrementalUpdateSettings getIncrementalUpdateSettings() {
    return incrementalUpdateSettings;
  }

  /** Returns original cost of running raw query if defined or zero. */
  @Override
  public double getOriginalCost() {
    return originalCost;
  }

  @Override
  public long getJobStart() {
    return jobStart;
  }

  @Override
  public List<String> getPartition() {
    return partition;
  }

  @Override
  public boolean isApplicable(
      Set<SubstitutionUtils.VersionedPath> queryTablesUsed,
      Set<SubstitutionUtils.VersionedPath> queryVdsUsed,
      Set<SubstitutionUtils.ExternalQueryDescriptor> externalQueries) {
    return true;
  }

  @Override
  public boolean isStale() {
    return isStale;
  }
}
