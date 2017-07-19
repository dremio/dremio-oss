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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.physical.base.ScanStats.GroupScanProperty;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.exec.store.schedule.SimpleCompleteWork;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;

@JsonTypeName("info-schema")
public class InfoSchemaGroupScan extends OldAbstractGroupScan<CompleteWork> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaGroupScan.class);

  /****************************************
   * Members used in equals()/hashCode()!
   * If you add a new member that needs to be in equality, add them eqauls() and hashCode().
   ****************************************/
  private final InfoSchemaTableType table;
  private final InfoSchemaFilter filter;
  private boolean isFilterPushedDown = false;

  /****************************************
   * Members NOT used in equals()/hashCode()!
   ****************************************/
  private final SabotContext context;

  public InfoSchemaGroupScan(InfoSchemaTableType table, InfoSchemaFilter filter, SabotContext context) {
    super((String)null, table.getSchema(), Arrays.asList(InfoSchemaConstants.IS_SCHEMA_NAME, table.name()));
    this.context = context;
    this.filter = filter;
    this.table = table;
  }

  @JsonCreator
  public InfoSchemaGroupScan(@JsonProperty("table") InfoSchemaTableType table,
                             @JsonProperty("filter") InfoSchemaFilter filter,
                             @JacksonInject StoragePluginRegistry pluginRegistry
                             ) throws ExecutionSetupException {
    this(table, filter, fromRegistry(pluginRegistry));
  }

  private InfoSchemaGroupScan(InfoSchemaGroupScan that) {
    super(that);
    this.table = that.table;
    this.filter = that.filter;
    this.context = that.context;
    this.isFilterPushedDown = that.isFilterPushedDown;
  }

  @JsonProperty("table")
  public InfoSchemaTableType getTable() {
    return table;
  }

  @JsonIgnore
  public SabotContext getContext() {
    return context;
  }

  @JsonProperty("filter")
  public InfoSchemaFilter getFilter() {
    return filter;
  }


  @Override
  public Iterator<CompleteWork> getSplits(ExecutionNodeMap enm) {
    return Collections.<CompleteWork>singletonList(new SimpleCompleteWork(1)).iterator();
  }

  @Override
  public SubScan getSpecificScan(List<CompleteWork> work) throws ExecutionSetupException {
    return new InfoSchemaSubScan(table, filter, context);
  }

  static SabotContext fromRegistry(StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    InfoSchemaStoragePlugin plugin = (InfoSchemaStoragePlugin) pluginRegistry.getPlugin(InfoSchemaConstants.IS_SCHEMA_NAME);
    return plugin.getContext();
  }

  @Override
  public StoragePlugin getPlugin() {
    return null;
  }

  public ScanStats getScanStats(){
    if (filter == null) {
      return ScanStats.TRIVIAL_TABLE;
    } else {
      // If the filter is available, return stats that is lower in cost than TRIVIAL_TABLE cost so that
      // Scan with Filter is chosen.
      return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, 10, 1, 0);
    }
  }

  @Override
  public ScanCostFactor getScanCostFactor() {
    return ScanCostFactor.OTHER;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public String getDigest() {
    return this.table.toString() + ", filter=" + filter;
  }

  @Override
  public InfoSchemaGroupScan clone(List<SchemaPath> columns) {
    InfoSchemaGroupScan  newScan = new InfoSchemaGroupScan (this);
    return newScan;
  }

  public void setFilterPushedDown(boolean status) {
    this.isFilterPushedDown = status;
  }

  @JsonIgnore
  public boolean isFilterPushedDown() {
    return isFilterPushedDown;
  }

  @Override
  public List<SchemaPath> getColumns() {
    return GroupScan.ALL_COLUMNS;
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
    InfoSchemaGroupScan that = (InfoSchemaGroupScan) o;
    return isFilterPushedDown == that.isFilterPushedDown &&
        table == that.table &&
        Objects.equal(filter, that.filter);
  }

  /****************************************
   * Add members to this, do not auto-generate
   ****************************************/
  @Override
  public int hashCode() {
    return Objects.hashCode(table, filter, isFilterPushedDown);
  }


}
