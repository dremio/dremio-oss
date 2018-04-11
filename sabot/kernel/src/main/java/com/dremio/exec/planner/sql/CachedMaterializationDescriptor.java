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
package com.dremio.exec.planner.sql;

import org.apache.calcite.plan.CopyWithCluster;

import com.google.common.base.Preconditions;

/**
 * {@link MaterializationDescriptor} that caches the expanded {@link DremioRelOptMaterialization}
 */
public class CachedMaterializationDescriptor extends MaterializationDescriptor {

  private final DremioRelOptMaterialization materialization;

  public CachedMaterializationDescriptor(MaterializationDescriptor descriptor, DremioRelOptMaterialization materialization) {
    super(descriptor.getLayoutInfo(),
          descriptor.getMaterializationId(),
          descriptor.getVersion(),
          descriptor.getExpirationTimestamp(),
          descriptor.getPlan(),
          descriptor.getPath(),
          descriptor.getOriginalCost(),
          descriptor.getJobStart(),
          descriptor.getPartition(),
          descriptor.getIncrementalUpdateSettings(),
          descriptor.getJoinDependencyProperties());
    this.materialization = Preconditions.checkNotNull(materialization, "materialization is required");
  }

  @Override
  public DremioRelOptMaterialization getMaterializationFor(SqlConverter converter) {
    final CopyWithCluster copier = new CopyWithCluster(converter.getCluster());
    final DremioRelOptMaterialization copied = materialization.accept(copier);
    copier.validate();
    return copied;
  }

  public DremioRelOptMaterialization getMaterialization() {
    return materialization;
  }
}
