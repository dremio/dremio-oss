/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.Collection;
import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractSubScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ischema.tables.InfoSchemaTable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("ischema")
public class InfoSchemaSubScan extends AbstractSubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaSubScan.class);

  private final InfoSchemaTable table;
  private final SearchQuery query;
  private final List<SchemaPath> columns;
  private final StoragePluginId pluginId;

  @JsonCreator
  public InfoSchemaSubScan(
      @JsonProperty("props") OpProps props,
      @JsonProperty("table") InfoSchemaTable table,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("query") SearchQuery query,
      @JsonProperty("pluginId") StoragePluginId pluginId
      ) {
    super(props, table.getRecordSchema(), Arrays.asList("INFORMATION_SCHEMA", table.name()));
    this.table = table;
    this.columns = columns;
    this.query = query;
    this.pluginId = pluginId;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public InfoSchemaTable getTable() {
    return table;
  }

  public SearchQuery getQuery() {
    return query;
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

  @Override
  public int getOperatorType() {
    return CoreOperatorType.INFO_SCHEMA_SUB_SCAN_VALUE;
  }

  @JsonIgnore // no need to serialize.
  @Override
  public BatchSchema getFullSchema() {
    return super.getFullSchema();
  }
}
