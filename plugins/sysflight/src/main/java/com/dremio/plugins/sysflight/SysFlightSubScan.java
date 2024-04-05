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

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractSubScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.OpWithMinorSpecificAttrs;
import com.dremio.exec.proto.FlightProtos.CoordinatorFlightTicket;
import com.dremio.exec.proto.FlightProtos.SysFlightTicket;
import com.dremio.exec.proto.SearchProtos.SearchQuery;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.List;

/** SysFlight sub scan */
@JsonTypeName("flight-scan")
public class SysFlightSubScan extends AbstractSubScan implements OpWithMinorSpecificAttrs {

  private final List<SchemaPath> columns;
  private final List<String> datasetPath;
  private final StoragePluginId pluginId;
  private final BatchSchema schema;
  private final SearchQuery query;

  @JsonCreator
  public SysFlightSubScan(
      @JsonProperty("props") OpProps props,
      @JsonProperty("datasetPath") List<String> datasetPath,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("query") SearchQuery query,
      @JsonProperty("pluginId") StoragePluginId pluginId) {
    super(props, schema, datasetPath);
    this.schema = schema;
    this.columns = columns;
    this.datasetPath = datasetPath;
    this.pluginId = pluginId;
    this.query = query;
  }

  CoordinatorFlightTicket getTicket() {
    SysFlightTicket.Builder ticketBuilder =
        SysFlightTicket.newBuilder()
            .setDatasetName(String.join(".", datasetPath.subList(1, datasetPath.size())))
            .setUserName(props.getUserName());
    if (query != null) {
      ticketBuilder.setQuery(query);
    }
    return CoordinatorFlightTicket.newBuilder().setSyFlightTicket(ticketBuilder.build()).build();
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.FLIGHT_SUB_SCAN_VALUE;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public SearchQuery getQuery() {
    return query;
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public Collection<List<String>> getReferencedTables() {
    return super.getReferencedTables();
  }

  @Override
  public List<String> getTableSchemaPath() {
    return super.getTableSchemaPath();
  }

  @Override
  public BatchSchema getFullSchema() {
    return super.getFullSchema();
  }

  @Override
  public String toString() {
    return new NamespaceKey(Iterables.getOnlyElement(getReferencedTables())).toString();
  }

  public List<String> getDatasetPath() {
    return datasetPath;
  }

  public BatchSchema getSchema() {
    return schema;
  }
}
