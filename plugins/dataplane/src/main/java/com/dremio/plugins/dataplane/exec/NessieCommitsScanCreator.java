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
package com.dremio.plugins.dataplane.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.iceberg.NessieCommitsSubScan;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions.Mode;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import java.util.ArrayList;
import java.util.List;

/**
 * NessieCommitsScan creator - creates a ScanOperator with a single {@link
 * NessieCommitsRecordReader} It scans commits for remove orphan as well as do the tree travers for
 * all the tables.
 */
public class NessieCommitsScanCreator implements ProducerOperator.Creator<NessieCommitsSubScan> {

  @Override
  public ProducerOperator create(
      FragmentExecutionContext fec, OperatorContext context, NessieCommitsSubScan config)
      throws ExecutionSetupException {

    List<RecordReader> recordReaders = new ArrayList<>();
    final SupportsIcebergMutablePlugin plugin = fec.getStoragePlugin(config.getPluginId());
    if (Mode.LIVE_SNAPSHOTS.equals(config.getSnapshotsScanOptions().getMode())) {
      // Expire the applicable snapshots and send live to next layer from branch heads
      recordReaders.add(
          new NessieIcebergExpirySnapshotsReader(
              context, plugin, config.getProps(), config.getSnapshotsScanOptions()));
    }

    // Scan remaining commits for applicable snapshots
    RecordReader nessieCommitsRecordReader =
        config.isLeanSchema()
            ? new LeanNessieCommitsRecordReader(fec, context, config)
            : new NessieCommitsRecordReader(fec, context, config);
    recordReaders.add(nessieCommitsRecordReader);

    return new ScanOperator(
        fec, config, context, RecordReaderIterator.from(recordReaders.listIterator()));
  }
}
