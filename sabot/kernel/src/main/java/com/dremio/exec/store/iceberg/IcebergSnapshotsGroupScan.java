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

import static com.dremio.exec.store.iceberg.IcebergUtils.getMetadataLocation;
import static com.dremio.exec.store.iceberg.IcebergUtils.getSplitAndPartitionInfo;

import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.IcebergTableProps;

/**
 * Iceberg snapshots group scan
 */
public class IcebergSnapshotsGroupScan extends AbstractGroupScan {
  private final IcebergTableProps icebergTableProps;
  private final SnapshotsScanOptions snapshotsScanOptions;

  public IcebergSnapshotsGroupScan(OpProps props, TableMetadata dataset, IcebergTableProps icebergTableProps,
                                         List<SchemaPath> columns, SnapshotsScanOptions snapshotsScanOptions) {
    super(props, dataset, columns);
    this.icebergTableProps = icebergTableProps;
    this.snapshotsScanOptions = snapshotsScanOptions;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.ICEBERG_SNAPSHOTS_SUB_SCAN_VALUE;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> works) throws ExecutionSetupException {
    final StoragePluginId pluginId;
    if (dataset instanceof InternalIcebergScanTableMetadata) {
      InternalIcebergScanTableMetadata icebergDataset = (InternalIcebergScanTableMetadata) dataset;
      pluginId = icebergDataset.getIcebergTableStoragePlugin();
    } else {
      pluginId = dataset.getStoragePluginId();
    }

    final String metadataLocation = getMetadataLocation(dataset, works);
    return new IcebergSnapshotsSubScan(
      props,
      props.getSchema(),
      getSplitAndPartitionInfo(metadataLocation),
      getDataset().getName().getPathComponents(),
      pluginId,
      dataset.getStoragePluginId(),
      columns,
      icebergTableProps,
      snapshotsScanOptions);
  }
}
