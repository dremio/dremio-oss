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
package com.dremio.exec.store.sys;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractSubScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.OpWithMinorSpecificAttrs;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.List;

@JsonTypeName("sys")
public class SystemSubScan extends AbstractSubScan implements OpWithMinorSpecificAttrs {

  private final List<SchemaPath> columns;
  private final SystemTable table;
  private final StoragePluginId pluginId;

  @JsonCreator
  public SystemSubScan(
      @JsonProperty("props") OpProps props,
      @JsonProperty("table") SystemTable table,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("pluginId") StoragePluginId pluginId) {
    super(props, table.getRecordSchema(), table.getDatasetPath().getComponents());
    this.columns = columns;
    this.table = table;
    this.pluginId = pluginId;
  }

  @Override
  @JsonIgnore
  public int getOperatorType() {
    return CoreOperatorType.SYSTEM_TABLE_SCAN_VALUE;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public SystemTable getTable() {
    return table;
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonIgnore
  @Override
  public Collection<List<String>> getReferencedTables() {
    return super.getReferencedTables();
  }

  @JsonIgnore
  @Override
  public List<String> getTableSchemaPath() {
    return super.getTableSchemaPath();
  }

  @JsonIgnore
  @Override
  public BatchSchema getFullSchema() {
    return super.getFullSchema();
  }

  @Override
  public String toString() {
    return new NamespaceKey(Iterables.getOnlyElement(getReferencedTables())).toString();
  }
}
