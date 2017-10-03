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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.pojo.PojoRecordReader;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.exec.store.schedule.SimpleCompleteWork;
import com.dremio.sabot.exec.context.OperatorContext;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("sys")
public class SystemTableScan extends OldAbstractGroupScan<CompleteWork> implements SubScan {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemTableScan.class);

  /****************************************
   * Members used in equals()/hashCode()!
   * If you add a new member that needs to be in equality, add them equals() and hashCode().
   ****************************************/
  private final SystemTable table;
  private final SystemTablePlugin plugin;

  /****************************************
   * Members NOT used in equals()/hashCode()!
   ****************************************/
  // NONE!

  @JsonCreator
  public SystemTableScan( //
      @JsonProperty("table") SystemTable table, //
      @JacksonInject StoragePluginRegistry engineRegistry //
  ) throws IOException, ExecutionSetupException {
    super(null, table.getSchema(), null);
    this.table = table;
    this.plugin = (SystemTablePlugin) engineRegistry.getPlugin(SystemTablePluginConfig.INSTANCE);
  }

  public SystemTableScan(SystemTable table, SystemTablePlugin plugin) {
    super(null, table.getSchema(), null);
    this.table = table;
    this.plugin = plugin;
  }

  /**
   * System tables do not need stats.
   * @return a trivial stats table
   */
  @Override
  public ScanStats getScanStats() {
    return ScanStats.TRIVIAL_TABLE;
  }

  @Override
  public SubScan getSpecificScan(List<CompleteWork> work) throws ExecutionSetupException {
    return this;
  }

  // If distributed, the scan needs to happen on every node.
  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    return table.isDistributed() ? plugin.getContext().getExecutors().size() : 1;
  }

  // If distributed, the scan needs to happen on every node.
  @Override
  @JsonIgnore
  public int getMinParallelizationWidth() {
    return table.isDistributed() ? plugin.getContext().getExecutors().size() : 1;
  }

  @Override
  @JsonIgnore
  public String getDigest() {
    return "SystemTableScan [table=" + table.name() +
      ", distributed=" + table.isDistributed() + "]";
  }

  @Override
  @JsonIgnore
  public int getOperatorType() {
    return CoreOperatorType.SYSTEM_TABLE_SCAN_VALUE;
  }

  /**
   * If distributed, the scan needs to happen on every node. Since width is enforced, the number of fragments equals
   * number of SabotNodes. And here we set, each endpoint as mandatory assignment required to ensure every
   * SabotNode executes a fragment.
   * @return the SabotNode endpoint affinities
   */
  @Override
  @JsonIgnore
  public Iterator<CompleteWork> getSplits(ExecutionNodeMap nodeMap) {
    if (table.isDistributed()) {
      List<CompleteWork> work = new ArrayList<>();
      final Collection<NodeEndpoint> bits = plugin.getContext().getExecutors();
      final double affinityPerNode = 1d / bits.size();
      for (final NodeEndpoint endpoint : bits) {
        work.add(new SimpleCompleteWork(1, new EndpointAffinity(endpoint, affinityPerNode, true, 1)));
      }
      return work.iterator();
    } else {
      return Collections.<CompleteWork>singletonList(new SimpleCompleteWork(1)).iterator();
    }
  }

  @Override
  @JsonIgnore
  public DistributionAffinity getDistributionAffinity() {
    return table.isDistributed() ? DistributionAffinity.HARD : DistributionAffinity.SOFT;
  }

  @Override
  @JsonIgnore
  public ScanCostFactor getScanCostFactor() {
    return ScanCostFactor.OTHER;
  }

  @Override
  public SystemTableScan clone(List<SchemaPath> columns) {
    return this;
  }

  public SystemTable getTable() {
    return table;
  }

  @SuppressWarnings("rawtypes")
  public RecordReader getRecordReader(OperatorContext context){
    return new PojoRecordReader(table.getPojoClass(), table.getIterator(plugin.getContext(), context));
  }

  @Override
  @JsonIgnore
  public SystemTablePlugin getPlugin() {
    return plugin;
  }

  /****************************************
   * Add members to this, do not auto-generate
   ****************************************/
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SystemTableScan that = (SystemTableScan) o;
    return table == that.table &&
        com.google.common.base.Objects.equal(plugin.getConfig(), that.plugin.getConfig());
  }

  /****************************************
   * Add members to this, do not auto-generate
   ****************************************/
  @Override
  public int hashCode() {
    return com.google.common.base.Objects.hashCode(table, plugin.getConfig());
  }

  @Override
  @JsonIgnore
  public List<String> getTableSchemaPath() {
    return Arrays.asList("sys", table.getTableName());
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext functionLookupContext) {
    return table.getSchema();
  }

  @Override
  @JsonIgnore
  public List<SchemaPath> getColumns() {
    return GroupScan.ALL_COLUMNS;
  }

  @Override
  public String toString(){
    return getTableSchemaPath().toString();
  }

}
