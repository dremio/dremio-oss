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

import java.util.Arrays;
import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractSubScan;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("sys")
public class SystemSubScan extends AbstractSubScan {

  private final List<SchemaPath> columns;
  private final SystemTable table;

  @JsonCreator
  public SystemSubScan(
      @JsonProperty("table") SystemTable table,
      @JsonProperty("columns") List<SchemaPath> columns
      ) {
    super(null, table.getSchema(), Arrays.asList("sys", table.getTableName()));
    this.columns = columns;
    this.table = table;
  }

  @Override
  @JsonIgnore
  public int getOperatorType() {
    return CoreOperatorType.SYSTEM_TABLE_SCAN_VALUE;
  }

  public SystemTable getTable() {
    return table;
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext functionLookupContext) {
    return getSchema().maskAndReorder(getColumns());
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonIgnore
  @Override
  public List<String> getTableSchemaPath() {
    return super.getTableSchemaPath();
  }

  @JsonIgnore
  @Override
  public BatchSchema getSchema() {
    return super.getSchema();
  }

  @Override
  public String toString(){
    return new NamespaceKey(getTableSchemaPath()).toString();
  }

}
