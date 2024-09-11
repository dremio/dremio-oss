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
package com.dremio.plugins.sysflight;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.SearchProtos.SearchQuery;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.schedule.SimpleCompleteWork;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** Flight group scan. */
public class SysFlightGroupScan extends AbstractBase implements GroupScan<SimpleCompleteWork> {

  private final List<SchemaPath> columns;
  private final SearchQuery query;
  private final List<String> datasetPath;
  private final BatchSchema schema;
  private final StoragePluginId pluginId;
  private final boolean isDistributed;
  private final int executorCount;
  private final SysTableFunctionCatalogMetadata tableFunctionMetadata;

  public SysFlightGroupScan(
      OpProps props,
      List<String> datasetPath,
      BatchSchema schema,
      List<SchemaPath> columns,
      SearchQuery query,
      StoragePluginId pluginId,
      boolean isDistributed,
      int executorCount,
      SysTableFunctionCatalogMetadata tableFunctionMetadata) {
    super(props);
    this.columns = columns;
    this.datasetPath = datasetPath;
    this.schema = schema;
    this.pluginId = pluginId;
    this.isDistributed = isDistributed;
    this.executorCount = executorCount;
    this.query = query;
    this.tableFunctionMetadata = tableFunctionMetadata;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public SearchQuery getQuery() {
    return query;
  }

  // If distributed, the scan needs to happen on every node.
  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    return isDistributed ? executorCount : 1;
  }

  // If distributed, the scan needs to happen on every node.
  @Override
  @JsonIgnore
  public int getMinParallelizationWidth() {
    return isDistributed ? executorCount : 1;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return isDistributed ? DistributionAffinity.HARD : DistributionAffinity.SOFT;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.FLIGHT_SUB_SCAN_VALUE;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof SysFlightGroupScan)) {
      return false;
    }
    SysFlightGroupScan castOther = (SysFlightGroupScan) other;
    return Objects.equal(datasetPath, castOther.datasetPath)
        && Objects.equal(schema, castOther.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(datasetPath, schema);
  }

  @Override
  public String toString() {
    return new EntityPath(datasetPath).getName();
  }

  /**
   * If distributed, the scan needs to happen on every node. Since width is enforced, the number of
   * fragments equals number of SabotNodes. And here we set, each endpoint as mandatory assignment
   * required to ensure every SabotNode executes a fragment.
   *
   * @return the SabotNode endpoint affinities
   */
  @Override
  public Iterator<SimpleCompleteWork> getSplits(ExecutionNodeMap executionNodes) {
    if (isDistributed) {
      final List<NodeEndpoint> executors = executionNodes.getExecutors();
      final double affinityPerNode = 1d / executors.size();
      return FluentIterable.from(executors)
          .transform(
              new Function<NodeEndpoint, SimpleCompleteWork>() {
                @Override
                public SimpleCompleteWork apply(NodeEndpoint input) {
                  return new SimpleCompleteWork(
                      1, new EndpointAffinity(input, affinityPerNode, true, 1));
                }
              })
          .iterator();
    } else {
      return Collections.<SimpleCompleteWork>singletonList(new SimpleCompleteWork(1)).iterator();
    }
  }

  @Override
  public SubScan getSpecificScan(List<SimpleCompleteWork> work) {
    return new SysFlightSubScan(
        props,
        new EntityPath(datasetPath).getComponents(),
        schema,
        getColumns(),
        query,
        pluginId,
        tableFunctionMetadata != null ? tableFunctionMetadata.getSysTableFunction() : null);
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitGroupScan(this, value);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return new SysFlightGroupScan(
        props,
        datasetPath,
        schema,
        columns,
        query,
        pluginId,
        isDistributed,
        executorCount,
        tableFunctionMetadata);
  }
}
