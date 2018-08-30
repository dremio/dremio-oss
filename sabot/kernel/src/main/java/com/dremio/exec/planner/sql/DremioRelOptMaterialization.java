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


import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;

import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.dremio.exec.planner.acceleration.JoinDependencyProperties;
import com.dremio.exec.planner.physical.visitor.CrelUniqifier;
import com.dremio.exec.planner.sql.MaterializationDescriptor.ReflectionInfo;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;

/**
 * This extension of RelOptMaterialization is used in Dremio acceleration. It stores and makes accessible information about
 * incremental updates
 */
public class DremioRelOptMaterialization extends org.apache.calcite.plan.RelOptMaterialization {
  private final IncrementalUpdateSettings incrementalUpdateSettings;
  private final JoinDependencyProperties joinDependencyProperties;
  private final ReflectionInfo layoutInfo;
  private final String materializationId;
  private final BatchSchema schema;
  private final long expirationTimestamp;
  private final boolean snowflake;

  public DremioRelOptMaterialization(RelNode tableRel,
                                     RelNode queryRel,
                                     IncrementalUpdateSettings incrementalUpdateSettings,
                                     JoinDependencyProperties joinDependencyProperties,
                                     ReflectionInfo layoutInfo,
                                     String materializationId,
                                     BatchSchema schema,
                                     long expirationTimestamp) {
    this(tableRel, queryRel, incrementalUpdateSettings, joinDependencyProperties, layoutInfo, materializationId, schema, expirationTimestamp, false);
  }

  public DremioRelOptMaterialization(RelNode tableRel,
                                     RelNode queryRel,
                                     IncrementalUpdateSettings incrementalUpdateSettings,
                                     JoinDependencyProperties joinDependencyProperties,
                                     ReflectionInfo layoutInfo,
                                     String materializationId,
                                     BatchSchema schema,
                                     long expirationTimestamp,
                                     boolean snowflake) {
    super(tableRel, queryRel, null, null);
    this.incrementalUpdateSettings = Preconditions.checkNotNull(incrementalUpdateSettings);
    this.joinDependencyProperties = joinDependencyProperties;
    this.materializationId = Preconditions.checkNotNull(materializationId);
    this.schema = schema;
    this.layoutInfo = Preconditions.checkNotNull(layoutInfo);
    this.expirationTimestamp = expirationTimestamp;
    this.snowflake = snowflake;
  }

  public long getExpirationTimestamp() {
    return expirationTimestamp;
  }

  public IncrementalUpdateSettings getIncrementalUpdateSettings() {
    return incrementalUpdateSettings;
  }

  public JoinDependencyProperties getJoinDependencyProperties() {
    return joinDependencyProperties;
  }

  public boolean isSnowflake() {
    return snowflake;
  }

  public DremioRelOptMaterialization uniqify() {
    return new DremioRelOptMaterialization(tableRel,
      CrelUniqifier.uniqifyGraph(queryRel),
      incrementalUpdateSettings,
      joinDependencyProperties,
      layoutInfo,
      materializationId,
      schema,
      expirationTimestamp,
      snowflake
    );
  }

  public String getMaterializationId() {
    return materializationId;
  }

  public ReflectionInfo getLayoutInfo() {
    return layoutInfo;
  }

  public String getReflectionId() {
    return layoutInfo.getReflectionId();
  }

  public DremioRelOptMaterialization cloneWithNewQuery(RelNode query) {
    return new DremioRelOptMaterialization(tableRel,
      query,
      incrementalUpdateSettings,
      joinDependencyProperties,
      layoutInfo,
      materializationId,
      schema,
      expirationTimestamp,
      snowflake
    );
  }

  public DremioRelOptMaterialization createSnowflakeMaterialization(RelNode query) {
    return new DremioRelOptMaterialization(tableRel,
      query,
      incrementalUpdateSettings,
      joinDependencyProperties,
      layoutInfo,
      materializationId,
      schema,
      expirationTimestamp,
      true
    );
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public DremioRelOptMaterialization accept(RelShuttle shuttle) {
    return new DremioRelOptMaterialization(
      tableRel.accept(shuttle),
      queryRel.accept(shuttle),
      incrementalUpdateSettings,
      joinDependencyProperties,
      layoutInfo,
      materializationId,
      schema,
      expirationTimestamp,
      snowflake
    );
  }
}
