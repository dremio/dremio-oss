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

import java.time.Duration;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;

import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor.ReflectionInfo;
import com.dremio.exec.planner.physical.visitor.CrelUniqifier;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;

/**
 * This extension of RelOptMaterialization is used in Dremio acceleration. It stores and makes
 * accessible information about incremental updates.
 *
 * A DremioMaterialization is immutable so as we transform and normalize target materializations
 * we will clone the DremioMaterialization accordingly.
 */
public class DremioMaterialization {
  private final RelNode tableRel;
  private final RelNode queryRel;
  private final IncrementalUpdateSettings incrementalUpdateSettings;
  private final JoinDependencyProperties joinDependencyProperties;
  private final ReflectionInfo layoutInfo;
  private final String materializationId;
  private final BatchSchema schema;
  private final long expirationTimestamp;
  private final boolean snowflake;
  private final DremioMaterialization original;
  private final boolean alreadyStripped;
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
      boolean alreadyStripped,
      int stripVersion,
      RelTransformer postStripTransformer) {
    this(tableRel, queryRel, incrementalUpdateSettings, joinDependencyProperties, layoutInfo, materializationId, schema,
        expirationTimestamp, false, null, alreadyStripped, stripVersion, postStripTransformer, "", Duration.ZERO);
  }

  private DremioMaterialization(
      RelNode tableRel,
      RelNode queryRel,
      IncrementalUpdateSettings incrementalUpdateSettings,
      JoinDependencyProperties joinDependencyProperties,
      ReflectionInfo layoutInfo,
      String materializationId,
      BatchSchema schema,
      long expirationTimestamp,
      boolean snowflake,
      DremioMaterialization original,
      boolean alreadyStripped,
      int stripVersion,
      RelTransformer postStripTransformer,
      String info,
      Duration duration) {
    this.tableRel = tableRel;
    this.queryRel = queryRel;
    this.incrementalUpdateSettings = Preconditions.checkNotNull(incrementalUpdateSettings);
    this.joinDependencyProperties = joinDependencyProperties;
    this.materializationId = Preconditions.checkNotNull(materializationId);
    this.schema = schema;
    this.layoutInfo = Preconditions.checkNotNull(layoutInfo);
    this.expirationTimestamp = expirationTimestamp;
    this.snowflake = snowflake;
    this.original = original == null ? this : original;
    this.alreadyStripped = alreadyStripped;
    this.stripVersion = stripVersion;
    this.postStripTransformer = postStripTransformer == null ? RelTransformer.NO_OP_TRANSFORMER : postStripTransformer;

    hasJoin = false;
    hasAgg = false;
    queryRel.accept(new RoutingShuttle() {
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
    if(layoutInfo == null) {
      return null;
    }
    return layoutInfo.getType();
  }

  public boolean isAlreadyStripped() {
    return alreadyStripped;
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
    return new DremioMaterialization(tableRel,
        CrelUniqifier.uniqifyGraph(queryRel),
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        snowflake,
        original,
        alreadyStripped,
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
        tableRel,
        query,
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        snowflake,
        original,
        alreadyStripped,
        stripVersion,
        postStripTransformer,
        info,
      normalizationDuration);
  }

  public DremioMaterialization cloneWith(RelNode query, String info, Duration duration) {
    return new DremioMaterialization(
      tableRel,
      query,
      incrementalUpdateSettings,
      joinDependencyProperties,
      layoutInfo,
      materializationId,
      schema,
      expirationTimestamp,
      snowflake,
      original,
      alreadyStripped,
      stripVersion,
      postStripTransformer,
      info,
      duration);
  }

  public DremioMaterialization cloneWith(RelNode tableRel, RelNode queryRel, Duration duration) {
    return new DremioMaterialization(
      tableRel,
      queryRel,
      incrementalUpdateSettings,
      joinDependencyProperties,
      layoutInfo,
      materializationId,
      schema,
      expirationTimestamp,
      snowflake,
      original,
      alreadyStripped,
      stripVersion,
      postStripTransformer,
      info,
      duration);
  }


  public DremioMaterialization createSnowflakeMaterialization(RelNode query) {
    return new DremioMaterialization(tableRel,
        query,
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        true,
        null, // consider the new materialization as original for reporting purposes
        alreadyStripped,
        stripVersion,
        postStripTransformer,
        info,
      normalizationDuration);
  }

  public BatchSchema getSchema() {
    return schema;
  }

  /**
   * The original materialization before any transformations were done.
   * @return The original materialization (possibly the same as this object).
   */
  public DremioMaterialization getOriginal() {
    return original;
  }

  public DremioMaterialization accept(RelShuttle shuttle) {
    return new DremioMaterialization(
      tableRel.accept(shuttle),
      queryRel.accept(shuttle),
      incrementalUpdateSettings,
      joinDependencyProperties,
      layoutInfo,
      materializationId,
      schema,
      expirationTimestamp,
      snowflake,
      original,
      alreadyStripped,
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
