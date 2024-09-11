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

import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.acceleration.descriptor.ReflectionInfo;
import com.dremio.exec.planner.physical.visitor.CrelUniqifier;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;
import java.time.Duration;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;

/**
 * This extension of RelOptMaterialization is used in Dremio acceleration. It stores and makes
 * accessible information about incremental updates.
 *
 * <p>A DremioMaterialization is immutable so as we transform and normalize target materializations,
 * we will clone the DremioMaterialization accordingly. The starting plan for each
 * DremioMaterialization is the expanded reflection plan and is stored in the original variable for
 * query profile purposes.
 */
public class DremioMaterialization {
  private final RelNode original;
  private final RelNode tableRel;
  private final RelNode queryRel;
  private final IncrementalUpdateSettings incrementalUpdateSettings;
  private final JoinDependencyProperties joinDependencyProperties;
  private final ReflectionInfo layoutInfo;
  private final String materializationId;
  private final BatchSchema schema;
  private final long expirationTimestamp;
  private final boolean snowflake;
  private final RelTransformer postStripTransformer;
  private boolean hasJoin;
  private boolean hasAgg;
  private String info;
  private Duration normalizationDuration;

  private int stripVersion;

  public DremioMaterialization(
      RelNode tableRel,
      RelNode queryRel,
      IncrementalUpdateSettings incrementalUpdateSettings,
      JoinDependencyProperties joinDependencyProperties,
      ReflectionInfo layoutInfo,
      String materializationId,
      BatchSchema schema,
      long expirationTimestamp,
      int stripVersion,
      RelTransformer postStripTransformer) {
    this(
        null,
        tableRel,
        queryRel,
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        false,
        stripVersion,
        postStripTransformer,
        "",
        Duration.ZERO);
  }

  private DremioMaterialization(
      RelNode original,
      RelNode tableRel,
      RelNode queryRel,
      IncrementalUpdateSettings incrementalUpdateSettings,
      JoinDependencyProperties joinDependencyProperties,
      ReflectionInfo layoutInfo,
      String materializationId,
      BatchSchema schema,
      long expirationTimestamp,
      boolean snowflake,
      int stripVersion,
      RelTransformer postStripTransformer,
      String info,
      Duration duration) {
    this.original = original == null ? queryRel : original;
    this.tableRel = tableRel;
    this.queryRel = queryRel;
    this.incrementalUpdateSettings = Preconditions.checkNotNull(incrementalUpdateSettings);
    this.joinDependencyProperties = joinDependencyProperties;
    this.materializationId = Preconditions.checkNotNull(materializationId);
    this.schema = schema;
    this.layoutInfo = Preconditions.checkNotNull(layoutInfo);
    this.expirationTimestamp = expirationTimestamp;
    this.snowflake = snowflake;
    this.stripVersion = stripVersion;
    this.postStripTransformer =
        postStripTransformer == null ? RelTransformer.NO_OP_TRANSFORMER : postStripTransformer;

    hasJoin = false;
    hasAgg = false;
    queryRel.accept(
        new RoutingShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            if (other instanceof LogicalJoin) {
              hasJoin = true;
              if (hasAgg) {
                return other;
              }
            } else if ((other instanceof LogicalAggregate) || (other instanceof LogicalFilter)) {
              hasAgg = true;
              if (hasJoin) {
                return other;
              }
            }
            return super.visit(other);
          }
        });
    this.info = info;
    this.normalizationDuration = duration;
  }

  public ReflectionType getReflectionType() {
    if (layoutInfo == null) {
      return null;
    }
    return layoutInfo.getType();
  }

  public RelNode getOriginal() {
    return original;
  }

  public RelNode getQueryRel() {
    return queryRel;
  }

  public RelNode getTableRel() {
    return tableRel;
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

  public DremioMaterialization uniqify() {
    return new DremioMaterialization(
        original,
        tableRel,
        CrelUniqifier.uniqifyGraph(queryRel),
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        snowflake,
        stripVersion,
        postStripTransformer,
        info,
        normalizationDuration);
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

  public boolean hasAgg() {
    return hasAgg;
  }

  public DremioMaterialization cloneWithNewQuery(RelNode query) {
    return new DremioMaterialization(
        original,
        tableRel,
        query,
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        snowflake,
        stripVersion,
        postStripTransformer,
        info,
        normalizationDuration);
  }

  public DremioMaterialization cloneWith(RelNode query, String info, Duration duration) {
    return new DremioMaterialization(
        original,
        tableRel,
        query,
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        snowflake,
        stripVersion,
        postStripTransformer,
        info,
        duration);
  }

  public DremioMaterialization cloneWith(RelNode tableRel, RelNode queryRel, Duration duration) {
    return new DremioMaterialization(
        original,
        tableRel,
        queryRel,
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        snowflake,
        stripVersion,
        postStripTransformer,
        info,
        duration);
  }

  public DremioMaterialization createSnowflakeMaterialization(RelNode query) {
    return new DremioMaterialization(
        query,
        tableRel,
        query,
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        true,
        stripVersion,
        postStripTransformer,
        info,
        normalizationDuration);
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public DremioMaterialization accept(RelShuttle shuttle) {
    return new DremioMaterialization(
        original,
        tableRel.accept(shuttle),
        queryRel.accept(shuttle),
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        snowflake,
        stripVersion,
        postStripTransformer,
        info,
        normalizationDuration);
  }

  public int getStripVersion() {
    return stripVersion;
  }

  public RelTransformer getPostStripTransformer() {
    return postStripTransformer;
  }

  public String getInfo() {
    return info;
  }

  public Duration getNormalizationDuration() {
    return normalizationDuration;
  }

  public RelWithInfo getQueryRelWithInfo() {
    return RelWithInfo.create(queryRel, info, normalizationDuration);
  }
}
