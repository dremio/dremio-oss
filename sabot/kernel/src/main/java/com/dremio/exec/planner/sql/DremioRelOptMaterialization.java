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
package com.dremio.exec.planner.sql;


import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;

import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.dremio.exec.planner.physical.visitor.CrelUniqifier;
import com.dremio.exec.planner.sql.MaterializationDescriptor.LayoutInfo;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;

/**
 * This extension of RelOptMaterialization is used in Dremio acceleration. It stores and makes accessible information about
 * incremental updates
 */
public class DremioRelOptMaterialization extends org.apache.calcite.plan.RelOptMaterialization {
  private final IncrementalUpdateSettings incrementalUpdateSettings;
  private final LayoutInfo layoutInfo;
  private final String materializationId;
  private final BatchSchema schema;
  private final long expirationTimestamp;

  public DremioRelOptMaterialization(RelNode tableRel,
                                     RelNode queryRel,
                                     IncrementalUpdateSettings incrementalUpdateSettings,
                                     LayoutInfo layoutInfo,
                                     String materializationId,
                                     BatchSchema schema,
                                     long expirationTimestamp) {

    // Create a RelOptMaterialization by manually specifying the RelOptTable.
    // If the type casting has occurred, the RelOptTable will reside in the first input of the table rel.
    super(tableRel, queryRel, tableRel.getTable() != null ? tableRel.getTable() : tableRel.getInput(0).getTable(), null);
    this.incrementalUpdateSettings = Preconditions.checkNotNull(incrementalUpdateSettings);
    this.materializationId = Preconditions.checkNotNull(materializationId);
    this.schema = schema;
    this.layoutInfo = Preconditions.checkNotNull(layoutInfo);
    this.expirationTimestamp = expirationTimestamp;
  }

  public long getExpirationTimestamp() {
    return expirationTimestamp;
  }

  public IncrementalUpdateSettings getIncrementalUpdateSettings() {
    return incrementalUpdateSettings;
  }

  public DremioRelOptMaterialization uniqify() {
    return new DremioRelOptMaterialization(tableRel, CrelUniqifier.uniqifyGraph(queryRel), incrementalUpdateSettings, layoutInfo, materializationId, schema, expirationTimestamp);
  }

  public String getMaterializationId() {
    return materializationId;
  }

  public LayoutInfo getLayoutInfo() {
    return layoutInfo;
  }

  public String getLayoutId() {
    return layoutInfo.getLayoutId();
  }

  public DremioRelOptMaterialization cloneWithNewQuery(RelNode query) {
    return new DremioRelOptMaterialization(tableRel, query, incrementalUpdateSettings, layoutInfo, materializationId, schema, expirationTimestamp);
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public DremioRelOptMaterialization accept(RelShuttle shuttle) {
    return new DremioRelOptMaterialization(
      tableRel.accept(shuttle),
      queryRel.accept(shuttle),
      incrementalUpdateSettings,
      layoutInfo,
      materializationId,
      schema,
      expirationTimestamp
    );
  }
}
