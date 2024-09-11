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

import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.dremio.exec.planner.acceleration.JoinDependencyProperties;
import com.dremio.exec.planner.acceleration.MaterializationExpander;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.store.CatalogService;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

public class UnexpandedMaterializationDescriptor extends BaseMaterializationDescriptor {
  private final byte[] planBytes;
  private final int stripVersion;
  private final JoinDependencyProperties joinDependencyProperties;
  private final CatalogService catalogService;
  private final boolean forceExpansion;

  public UnexpandedMaterializationDescriptor(
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
      final Integer stripVersion,
      final boolean forceExpansion,
      final CatalogService catalogService,
      final boolean isStale) {
    super(
        reflection,
        materializationId,
        version,
        expirationTimestamp,
        path,
        originalCost,
        jobStart,
        partition,
        incrementalUpdateSettings,
        isStale);
    this.planBytes = planBytes;
    this.joinDependencyProperties = joinDependencyProperties;
    this.stripVersion = Optional.ofNullable(stripVersion).orElse(1);
    this.forceExpansion = forceExpansion;
    this.catalogService = catalogService;
  }

  public int getStripVersion() {
    return stripVersion;
  }

  public byte[] getPlan() {
    return planBytes;
  }

  public JoinDependencyProperties getJoinDependencyProperties() {
    return joinDependencyProperties;
  }

  @Override
  public DremioMaterialization getMaterializationFor(SqlConverter converter) {
    final MaterializationExpander expander = MaterializationExpander.of(converter, catalogService);
    return expander.expand(this);
  }
}
