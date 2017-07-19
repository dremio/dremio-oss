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

import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractSubScan;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePluginRegistry;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InfoSchemaSubScan extends AbstractSubScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaSubScan.class);

  private final InfoSchemaTableType table;
  private final InfoSchemaFilter filter;
  private final SabotContext context;

  @JsonCreator
  public InfoSchemaSubScan(@JsonProperty("table") InfoSchemaTableType table,
                           @JsonProperty("filter") InfoSchemaFilter filter,
                           @JacksonInject StoragePluginRegistry pluginRegistry
      ) throws ExecutionSetupException {
    this(table, filter, InfoSchemaGroupScan.fromRegistry(pluginRegistry));
  }

  public InfoSchemaSubScan(
      InfoSchemaTableType table,
      InfoSchemaFilter filter,
      SabotContext context
      ) throws ExecutionSetupException {
    super(null, table.getSchema(), null);
    this.table = table;
    this.filter = filter;
    this.context = context;
  }

  @JsonIgnore
  public SabotContext getContext() {
    return context;
  }

  @JsonProperty("table")
  public InfoSchemaTableType getTable() {
    return table;
  }

  @JsonProperty("filter")
  public InfoSchemaFilter getFilter() {
    return filter;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.INFO_SCHEMA_SUB_SCAN_VALUE;
  }

  @Override
  @JsonIgnore
  public List<SchemaPath> getColumns() {
    return GroupScan.ALL_COLUMNS;
  }
}
