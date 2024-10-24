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
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;

/**
 * DremioMaterialization collects together everything Dremio knows about a materialization
 * including: - Definition of the reflection in ReflectionInfo - How it was refreshed in
 * IncrementalUpdateSettings - For expansion matching, a pair of query trees - one for matching,
 * another for replacement - For algebraic matching, a pair of query trees - one for matching,
 * another for replacement - Refresh time join operator stats in JoinDependencyProperties For
 * Snowflake reflections, the planner may update the matching plans after seeing the user query.
 */
public class DremioMaterialization {
  private final String materializationId;
  private final BatchSchema schema;
  private final long expirationTimestamp;
  private final ReflectionInfo layoutInfo;
  private final IncrementalUpdateSettings incrementalUpdateSettings;
  private final JoinDependencyProperties joinDependencyProperties;
  private final RelNode queryRel; // For view expansion matching
  private final RelNode stripFragmentOnTableRel; // For view expansion replacement
  private final RelNode strippedRel; // For algebraic matching
  private final RelNode tableRel; // For algebraic matching replacement
  private final boolean snowflake;
  private boolean hasJoin;
  private boolean hasAgg;

  private int stripVersion;

  public DremioMaterialization(
      RelNode tableRel,
      RelNode queryRel,
      RelNode strippedRel,
      RelNode stripFragmentOnTableRel,
      IncrementalUpdateSettings incrementalUpdateSettings,
      JoinDependencyProperties joinDependencyProperties,
      ReflectionInfo layoutInfo,
      String materializationId,
      BatchSchema schema,
      long expirationTimestamp,
      boolean snowflake,
      int stripVersion) {
    this.tableRel = tableRel;
    this.queryRel = queryRel;
    this.strippedRel = strippedRel;
    this.stripFragmentOnTableRel = stripFragmentOnTableRel;
    this.incrementalUpdateSettings = Preconditions.checkNotNull(incrementalUpdateSettings);
    this.joinDependencyProperties = joinDependencyProperties;
    this.materializationId = Preconditions.checkNotNull(materializationId);
    this.schema = schema;
    this.layoutInfo = Preconditions.checkNotNull(layoutInfo);
    this.expirationTimestamp = expirationTimestamp;
    this.snowflake = snowflake;
    this.stripVersion = stripVersion;

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
  }

  public ReflectionType getReflectionType() {
    if (layoutInfo == null) {
      return null;
    }
    return layoutInfo.getType();
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
        tableRel,
        CrelUniqifier.uniqifyGraph(queryRel),
        CrelUniqifier.uniqifyGraph(strippedRel),
        stripFragmentOnTableRel,
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        snowflake,
        stripVersion);
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

  public MaterializationTarget toAlgebraicMatchingTarget() {
    return new MaterializationTarget(this, strippedRel, tableRel);
  }

  public MaterializationTarget toExpansionMatchingTarget() {
    return new MaterializationTarget(this, queryRel, stripFragmentOnTableRel);
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public DremioMaterialization accept(RelShuttle shuttle) {
    return new DremioMaterialization(
        tableRel.accept(shuttle),
        queryRel.accept(shuttle),
        strippedRel.accept(shuttle),
        stripFragmentOnTableRel.accept(shuttle),
        incrementalUpdateSettings,
        joinDependencyProperties,
        layoutInfo,
        materializationId,
        schema,
        expirationTimestamp,
        snowflake,
        stripVersion);
  }

  public int getStripVersion() {
    return stripVersion;
  }
}
