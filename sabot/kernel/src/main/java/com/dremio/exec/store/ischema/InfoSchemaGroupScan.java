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
package com.dremio.exec.store.ischema;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ischema.tables.InfoSchemaTable;
import com.dremio.exec.store.schedule.SimpleCompleteWork;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

/**
 * InfoSchema group scan.
 */
public class InfoSchemaGroupScan extends AbstractBase implements GroupScan<SimpleCompleteWork> {

  private final InfoSchemaTable table;
  private final List<SchemaPath> columns;
  private final SearchQuery query;

  public InfoSchemaGroupScan(
      InfoSchemaTable table,
      SearchQuery query,
      List<SchemaPath> columns
      ) {
    super((String) null);
    this.table = table;
    this.columns = columns;
    this.query = query;
  }

  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  @JsonIgnore
  public int getMinParallelizationWidth() {
    return 1;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.SOFT;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.INFO_SCHEMA_SUB_SCAN_VALUE;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof InfoSchemaGroupScan)) {
      return false;
    }
    InfoSchemaGroupScan castOther = (InfoSchemaGroupScan) other;
    return Objects.equal(table, castOther.table);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(table);
  }

  @Override
  public String toString() {
    return table.name();
  }

  @Override
  public List<String> getTableSchemaPath() {
    return null;
  }

  /**
   * If distributed, the scan needs to happen on every node. Since width is enforced, the number of fragments equals
   * number of SabotNodes. And here we set, each endpoint as mandatory assignment required to ensure every
   * SabotNode executes a fragment.
   * @return the SabotNode endpoint affinities
   */
  @Override
  public Iterator<SimpleCompleteWork> getSplits(ExecutionNodeMap executionNodes) {
    return Collections.<SimpleCompleteWork>singletonList(new SimpleCompleteWork(1)).iterator();
  }

  @Override
  public SubScan getSpecificScan(List<SimpleCompleteWork> work) {
    return new InfoSchemaSubScan(table, query, getColumns());
  }

  @Override
  public BatchSchema getSchema() {
    return table.getSchema();
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitGroupScan(this, value);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext context) {
    return getSchema().maskAndReorder(getColumns());
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return this;
  }

}
