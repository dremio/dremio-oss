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
package com.dremio.exec.store.sys;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.schedule.SimpleCompleteWork;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;

/**
 * System group scan.
 */
public class SystemGroupScan extends AbstractBase implements GroupScan<SimpleCompleteWork>  {

  private final SystemTable table;
  private final List<SchemaPath> columns;
  private final int nodeCount;

  public SystemGroupScan(
      SystemTable table,
      List<SchemaPath> columns,
      int nodeCount
      ) {
    super((String) null);
    this.table = table;
    this.columns = columns;
    this.nodeCount = nodeCount;
  }

  // If distributed, the scan needs to happen on every node.
  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    return table.isDistributed() ? nodeCount : 1;
  }

  // If distributed, the scan needs to happen on every node.
  @Override
  @JsonIgnore
  public int getMinParallelizationWidth() {
    return table.isDistributed() ? nodeCount : 1;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return table.isDistributed() ? DistributionAffinity.HARD : DistributionAffinity.SOFT;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.SYSTEM_TABLE_SCAN_VALUE;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof SystemGroupScan)) {
      return false;
    }
    SystemGroupScan castOther = (SystemGroupScan) other;
    return Objects.equal(table, castOther.table);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(table);
  }

  @Override
  public String toString() {
    return table.getTableName();
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
    if (table.isDistributed()) {
      final List<NodeEndpoint> executors = executionNodes.getExecutors();
      final double affinityPerNode = 1d / executors.size();
      return FluentIterable.from(executors).transform(new Function<NodeEndpoint, SimpleCompleteWork>(){
        @Override
        public SimpleCompleteWork apply(NodeEndpoint input) {
          return new SimpleCompleteWork(1, new EndpointAffinity(input, affinityPerNode, true, 1));
        }}).iterator();
    } else {
      return Collections.<SimpleCompleteWork>singletonList(new SimpleCompleteWork(1)).iterator();
    }
  }

  @Override
  public SubScan getSpecificScan(List<SimpleCompleteWork> work) {
    return new SystemSubScan(table, getColumns());
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
    return new SystemGroupScan(table, getColumns(), nodeCount);
  }

}
