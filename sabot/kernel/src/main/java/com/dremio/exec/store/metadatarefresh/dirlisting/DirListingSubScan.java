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
package com.dremio.exec.store.metadatarefresh.dirlisting;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.planner.fragment.SplitNormalizer;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Collection;
import java.util.List;

@JsonTypeName("dirlisting-subscan")
public class DirListingSubScan extends SubScanWithProjection {
  private final StoragePluginId pluginId;
  private final BatchSchema tableSchema;
  private final boolean allowRecursiveListing;
  private final boolean incrementalBatchSize;

  @JsonIgnore private List<SplitAndPartitionInfo> splits;

  public DirListingSubScan(
      OpProps props,
      List<SplitAndPartitionInfo> splits,
      Collection<List<String>> referencedTables,
      List<SchemaPath> columns,
      BatchSchema schema,
      BatchSchema tableSchema,
      StoragePluginId pluginId,
      boolean allowRecursiveListing,
      boolean incrementalBatchSize) {
    super(props, schema, referencedTables, columns);
    this.pluginId = pluginId;
    this.splits = splits;
    this.tableSchema = tableSchema;
    this.allowRecursiveListing = allowRecursiveListing;
    this.incrementalBatchSize = incrementalBatchSize;
  }

  @JsonCreator
  public DirListingSubScan(
      @JsonProperty("props") OpProps props,
      @JsonProperty("referencedTables") Collection<List<String>> referencedTables,
      @JsonProperty("fullSchema") BatchSchema schema,
      @JsonProperty("tableSchema") BatchSchema tableSchema,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("allowRecursiveListing") boolean allowRecursiveListing,
      @JsonProperty("incrementalBatchSize") boolean incrementalBatchSize) {

    super(props, schema, referencedTables, columns);
    this.pluginId = pluginId;
    this.tableSchema = tableSchema;
    this.allowRecursiveListing = allowRecursiveListing;
    this.incrementalBatchSize = incrementalBatchSize;
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.DIR_LISTING_SUB_SCAN_VALUE;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public List<SplitAndPartitionInfo> getSplits() {
    return splits;
  }

  public boolean isAllowRecursiveListing() {
    return allowRecursiveListing;
  }

  public BatchSchema getTableSchema() {
    return tableSchema;
  }

  public boolean isIncrementalBatchSize() {
    return incrementalBatchSize;
  }

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) {
    SplitNormalizer.write(getProps(), writer, splits);
  }

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
    splits = SplitNormalizer.read(getProps(), reader);
  }
}
