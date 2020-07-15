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
package com.dremio.exec.physical.config;

import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetSubScan;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.protostuff.ByteString;

@JsonTypeName("boost-parquet")
public class BoostPOP extends ParquetSubScan {
  public BoostPOP(OpProps props,
                  FileConfig formatSettings,
                  List<SplitAndPartitionInfo> splits,
                  BatchSchema fullSchema,
                  List<List<String>> tablePath,
                  List<ParquetFilterCondition> conditions,
                  StoragePluginId pluginId,
                  List<SchemaPath> columns,
                  List<String> partitionColumns,
                  List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns,
                  ByteString extendedProperty) {
    super(props, formatSettings, splits, fullSchema, tablePath, conditions, pluginId, columns,
        partitionColumns, globalDictionaryEncodedColumns, extendedProperty);
  }

  @JsonCreator
  public BoostPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("formatSettings") FileConfig formatSettings,
      @JsonProperty("schema") BatchSchema fullSchema,
      @JsonProperty("referencedTables") List<List<String>> tablePath,
      @JsonProperty("conditions") List<ParquetFilterCondition> conditions,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("globalDictionaryEncodedColumns") List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns,
      @JsonProperty("extendedProperty") ByteString extendedProperty) {

    this(props, formatSettings, null, fullSchema, tablePath, conditions, pluginId, columns, partitionColumns,
        globalDictionaryEncodedColumns, extendedProperty);
  }

  @Override
  public boolean mayLearnSchema() {
    return false;
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.BOOST_PARQUET_VALUE;
  }
}
