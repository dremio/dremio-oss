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

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.IcebergDatasetSplitXAttr;

public class SingleTableIcebergExpirySnapshotsReader extends IcebergExpirySnapshotsReader {

  private final IcebergProtobuf.IcebergDatasetSplitXAttr splitXAttr;

  public SingleTableIcebergExpirySnapshotsReader(
      OperatorContext context,
      IcebergDatasetSplitXAttr splitXAttr,
      SupportsIcebergMutablePlugin icebergMutablePlugin, OpProps props,
      SnapshotsScanOptions snapshotsScanOptions) {
    super(context, icebergMutablePlugin, props, snapshotsScanOptions);
    this.splitXAttr = splitXAttr;
  }

  @Override
  protected void setupNextExpiryAction() {
    super.setupFsIfNecessary(splitXAttr.getPath());

    TableMetadata tableMetadata = TableMetadataParser.read(io, splitXAttr.getPath());
    boolean commitExpiry = true;
    boolean isExpireSnapshots = true;
    boolean isRemoveOrphanFiles = false;
    if (SnapshotsScanOptions.Mode.ALL_SNAPSHOTS.equals(snapshotsScanOptions.getMode())) {
      isExpireSnapshots = false;
      isRemoveOrphanFiles = true;
      commitExpiry = false;
    } else if (SnapshotsScanOptions.Mode.EXPIRED_SNAPSHOTS.equals(snapshotsScanOptions.getMode())) {
      commitExpiry = false;
    }
    VacuumOptions options = new VacuumOptions(isExpireSnapshots, isRemoveOrphanFiles,
      snapshotsScanOptions.getOlderThanInMillis(), snapshotsScanOptions.getRetainLast(), null, null);

    currentExpiryAction = new IcebergExpiryAction(icebergMutablePlugin, props,
      context, options, tableMetadata, splitXAttr.getTableName(), splitXAttr.getDbName(), null, io, commitExpiry);
    noMoreActions = true;
  }
}
