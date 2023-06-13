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
package com.dremio.exec.store.iceberg;

import java.util.List;

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
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;

/**
 * Iceberg snapshots subscan POP
 */
@JsonTypeName("iceberg-snapshots-sub-scan")
public class IcebergSnapshotsSubScan extends SubScanWithProjection {
  private final StoragePluginId pluginId;
  private final StoragePluginId datasourcePluginId;
  private final IcebergTableProps icebergTableProps;
  private final SnapshotsScanOptions snapshotsScanOptions;

  @JsonIgnore
  private List<SplitAndPartitionInfo> splits;

  public IcebergSnapshotsSubScan(
    @JsonProperty("props") OpProps props,
    @JsonProperty("fullSchema") BatchSchema fullSchema,
    @JsonProperty("tableSchemaPath") List<String> tablePath,
    @JsonProperty("pluginId") StoragePluginId pluginId,
    @JsonProperty("datasourcePluginId") StoragePluginId datasourcePluginId,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("icebergTableProps") IcebergTableProps icebergTableProps,
    @JsonProperty("snapshotsScanOptions") SnapshotsScanOptions snapshotsScanOptions) {
    this(props, fullSchema, null, tablePath, pluginId, datasourcePluginId, columns, icebergTableProps, snapshotsScanOptions);
  }

  public IcebergSnapshotsSubScan(
    OpProps props,
    BatchSchema fullSchema,
    List<SplitAndPartitionInfo> splits,
    List<String> tablePath,
    StoragePluginId pluginId,
    StoragePluginId datasourcePluginId,
    List<SchemaPath> columns,
    IcebergTableProps icebergTableProps,
    SnapshotsScanOptions snapshotsScanOptions) {
    super(props, fullSchema, (tablePath == null) ? null : ImmutableList.of(tablePath), columns);
    this.pluginId = pluginId;
    this.datasourcePluginId = datasourcePluginId;
    this.splits = splits;
    this.icebergTableProps = icebergTableProps;
    this.snapshotsScanOptions = snapshotsScanOptions;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public StoragePluginId getDatasourcePluginId() {
    return datasourcePluginId;
  }

  public IcebergTableProps getIcebergTableProps() {
    return icebergTableProps;
  }

  public List<SplitAndPartitionInfo> getSplits() {
    return splits;
  }

  public SnapshotsScanOptions getSnapshotsScanOptions() {
    return snapshotsScanOptions;
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.ICEBERG_SNAPSHOTS_SUB_SCAN_VALUE;
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
