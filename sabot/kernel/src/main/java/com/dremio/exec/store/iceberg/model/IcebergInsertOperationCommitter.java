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
package com.dremio.exec.store.iceberg.model;

import static com.dremio.sabot.op.writer.WriterCommitterOperator.SnapshotCommitStatus.COMMITTED;
import static com.dremio.sabot.op.writer.WriterCommitterOperator.SnapshotCommitStatus.NONE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.manifestwriter.IcebergCommitOpHelper;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.dremio.sabot.op.writer.WriterCommitterOperator.SnapshotCommitStatus;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Class used to commit insert into table operation
 */
public class IcebergInsertOperationCommitter implements IcebergOpCommitter {
  private static final Logger logger = LoggerFactory.getLogger(IcebergInsertOperationCommitter.class);
  private List<ManifestFile> manifestFileList = new ArrayList<>();

  private final IcebergCommand icebergCommand;
  private final OperatorStats operatorStats;
  private final String prevMetadataRootPointer;

  public IcebergInsertOperationCommitter(IcebergCommand icebergCommand, OperatorStats operatorStats) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    this.icebergCommand = icebergCommand;
    this.icebergCommand.beginTransaction();
    this.operatorStats = operatorStats;
    this.prevMetadataRootPointer = icebergCommand.getRootPointer();
  }

  @Override
  public Snapshot commit() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Snapshot currentSnapshot = icebergCommand.getCurrentSnapshot();
    if (manifestFileList.size() > 0) {
      icebergCommand.beginInsert();
      logger.debug("Committing manifest files list [Path , filecount] {} ",
        manifestFileList.stream().map(l -> new ImmutablePair(l.path(), l.addedFilesCount())).collect(Collectors.toList()));
      icebergCommand.consumeManifestFiles(manifestFileList);
      icebergCommand.finishInsert();
    }
    Snapshot snapshot = icebergCommand.endTransaction().currentSnapshot();
    SnapshotCommitStatus commitStatus = (currentSnapshot != null) &&
      (snapshot.snapshotId() == currentSnapshot.snapshotId()) ? NONE : COMMITTED;
    IcebergOpCommitter.writeSnapshotStats(operatorStats, commitStatus, snapshot);
    long totalCommitTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_COMMIT_TIME, totalCommitTime);
    return snapshot;
  }

  @Override
  public void consumeManifestFile(ManifestFile icebergManifestFile) {
    manifestFileList.add(icebergManifestFile);
  }

  @Override
  public void consumeDeleteDataFile(DataFile icebergDeleteDatafile) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Delete data file Operation is not allowed for Insert Transaction");
  }

  @Override
  public void consumeDeleteDataFilePath(String icebergDeleteDatafilePath) {
    throw new UnsupportedOperationException("Delete data file Operation is not allowed for Insert Transaction");
  }

  @Override
  public void updateSchema(BatchSchema newSchema) {
    throw new UnsupportedOperationException("Updating schema is not supported for update table Transaction");
  }

  @Override
  public String getRootPointer() {
    return icebergCommand.getRootPointer();
  }

  @Override
  public Map<Integer, PartitionSpec> getCurrentSpecMap() {
    return icebergCommand.getPartitionSpecMap();
  }

  @Override
  public Schema getCurrentSchema() {
    return icebergCommand.getIcebergSchema();
  }

  @Override
  public void cleanup(FileIO fileIO) {
    IcebergCommitOpHelper.deleteManifestFiles(fileIO, manifestFileList, true);
  }

  @Override
  public boolean isIcebergTableUpdated() {
    return !icebergCommand.getRootPointer().equals(prevMetadataRootPointer);
  }
}
