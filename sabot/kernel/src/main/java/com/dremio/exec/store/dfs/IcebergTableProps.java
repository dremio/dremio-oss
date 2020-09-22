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
package com.dremio.exec.store.dfs;

import java.util.List;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergOperation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class IcebergTableProps {

  private String tableLocation;
  private final String uuid;
  private BatchSchema fullSchema;
  private List<String> partitionColumnNames;
  private IcebergOperation.Type icebergOpType;

  @JsonCreator
  public IcebergTableProps(
    @JsonProperty("tableLocation") String tableLocation,
    @JsonProperty("uuid") String uuid,
    @JsonProperty("fullSchema") BatchSchema fullSchema,
    @JsonProperty("partitionColumnNames") List<String> partitionColumnNames,
    @JsonProperty("icebergOpType") IcebergOperation.Type icebergOpType
    ) {
      this.tableLocation = tableLocation;
      this.uuid = uuid;
      this.fullSchema = fullSchema;
      this.partitionColumnNames = partitionColumnNames;
      this.icebergOpType = icebergOpType;
  }

  public IcebergTableProps(final IcebergTableProps other){
    this.partitionColumnNames = other.partitionColumnNames == null ? null : ImmutableList.copyOf(other.partitionColumnNames);
    this.tableLocation = other.tableLocation;
    this.uuid = other.uuid;
    this.fullSchema = other.fullSchema;
    this.icebergOpType = other.icebergOpType;
  }

  public String getTableLocation() {
    return tableLocation;
  }

  public String getUuid() {
    return uuid;
  }

  public BatchSchema getFullSchema() {
    return fullSchema;
  }

  public IcebergOperation.Type getIcebergOpType() {
    return icebergOpType;
  }

  public void setFullSchema(BatchSchema schema) {
    this.fullSchema = schema;
  }

  void setTableLocation(String tableFolder) {
    this.tableLocation = tableFolder;
  }

  public List<String> getPartitionColumnNames() {
    return partitionColumnNames;
  }

  public void setPartitionColumnNames(List<String> partitionColumnNames) {
    this.partitionColumnNames = ImmutableList.copyOf(partitionColumnNames);
  }
}
