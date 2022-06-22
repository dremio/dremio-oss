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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.manifestwriter.IcebergCommitOpHelper;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Class used to commit DML (delete, update, merge) table operation, which has two update operation
 *  DELETE Followed by INSERT
 */
public class IcebergDmlOperationCommitter implements IcebergOpCommitter {
  private static final Logger logger = LoggerFactory.getLogger(IcebergDmlOperationCommitter.class);

  private List<ManifestFile> manifestFileList = new ArrayList<>();
  private List<String> deletedDataFilePathList = new ArrayList<>();
  private Set<Long> snapshotsToExpire = new HashSet<>();

  private final IcebergCommand icebergCommand;
  private final OperatorStats operatorStats;
  private final String prevMetadataRootPointer;

  public IcebergDmlOperationCommitter(IcebergCommand icebergCommand,
                                      OperatorStats operatorStats,
                                      DatasetConfig datasetConfig) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    Preconditions.checkNotNull(datasetConfig.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation());
    this.operatorStats = operatorStats;
    this.icebergCommand = icebergCommand;
    this.prevMetadataRootPointer = datasetConfig.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation();
  }

  @Override
  public Snapshot commit() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      beginDmlOperationTransaction();
      performUpdates();
      return endDmlOperationTransaction().currentSnapshot();
    } finally {
      long totalCommitTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_COMMIT_TIME, totalCommitTime);
    }
  }

  private boolean hasAnythingChanged() {
    return deletedDataFilePathList.size() + manifestFileList.size() > 0;
  }

  @VisibleForTesting
  public void beginDmlOperationTransaction() {
    icebergCommand.beginTransaction();
    if (isIcebergTableUpdated()) {
      String metadataFiles = String.format("Expected metadataRootPointer: %s, Found metadataRootPointer: %s",
        prevMetadataRootPointer, getRootPointer());
      logger.error(CONCURRENT_DML_OPERATION_ERROR + metadataFiles);
      throw UserException.concurrentModificationError().message(CONCURRENT_DML_OPERATION_ERROR).buildSilently();
    }

    if (hasAnythingChanged()) {
      Snapshot currentSnapshot = icebergCommand.getCurrentSnapshot();
      Preconditions.checkArgument(currentSnapshot != null, "Iceberg metadata does not have a snapshot");
      long currentSnapshotId = currentSnapshot.snapshotId();
      // Mark the transaction as a read-modify-write transaction. When performing DML (DELETE, UPDATE, MERGE) operations
      // to update an iceberg table, the version of the table while updating should be the same as the version that was read.
      Snapshot snapshot = icebergCommand.setIsReadModifyWriteTransaction(currentSnapshotId);
      if (snapshot.snapshotId() != currentSnapshotId) {
        snapshotsToExpire.add(snapshot.snapshotId());
      }
    }
  }

  @VisibleForTesting
  public Table endDmlOperationTransaction() {
    try {
      if (!snapshotsToExpire.isEmpty()) {
        icebergCommand.expireSnapshots(snapshotsToExpire);
      }
      return icebergCommand.endTransaction();
    } catch (ValidationException e) {
      logger.error(CONCURRENT_DML_OPERATION_ERROR, e);
      throw UserException.concurrentModificationError().message(CONCURRENT_DML_OPERATION_ERROR).buildSilently();
    }
  }

  @VisibleForTesting
  public List<String> getDeletedDataFilePaths() {
    return deletedDataFilePathList;
  }

  @VisibleForTesting
  public void performUpdates() {
    long finalSnapshotId = -1;
    if (deletedDataFilePathList.size() > 0) {
      icebergCommand.beginDelete();
      logger.debug("Committing delete data files, file count: {} ", deletedDataFilePathList.size());
      icebergCommand.consumeDeleteDataFilesByPaths(deletedDataFilePathList);
      finalSnapshotId = icebergCommand.finishDelete().snapshotId();
      snapshotsToExpire.add(finalSnapshotId);
    }

    if (manifestFileList.size() > 0) {
      icebergCommand.beginInsert();
      if (logger.isDebugEnabled()) {
        logger.debug("Committing {} manifest files.", manifestFileList.size());
        manifestFileList.stream().forEach(l -> logger.debug("Committing manifest file: {}, with {} added files.",
          l.path(), l.addedFilesCount()));
      }
      icebergCommand.consumeManifestFiles(manifestFileList);
      finalSnapshotId = icebergCommand.finishInsert().snapshotId();
      snapshotsToExpire.add(finalSnapshotId);
    }
    snapshotsToExpire.remove(finalSnapshotId);
  }

  @Override
  public void cleanup(DremioFileIO dremioFileIO) {
    IcebergCommitOpHelper.deleteManifestFiles(dremioFileIO, manifestFileList, true);
  }

  @Override
  public void consumeManifestFile(ManifestFile icebergManifestFile) {
    manifestFileList.add(icebergManifestFile);
  }

  @Override
  public void consumeDeleteDataFile(DataFile icebergDeleteDatafile) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Deleting data file by DataFile object is not supported in DML table Transaction");
  }

  @Override
  public void consumeDeleteDataFilePath(String icebergDeleteDatafilePath)throws UnsupportedOperationException {
    deletedDataFilePathList.add(icebergDeleteDatafilePath);
  }

  @Override
  public void updateSchema(BatchSchema newSchema) {
    throw new UnsupportedOperationException("Updating schema is not supported for DML table Transaction");
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
  public boolean isIcebergTableUpdated() {
    return !Path.getContainerSpecificRelativePath(Path.of(getRootPointer()))
      .equals(Path.getContainerSpecificRelativePath(Path.of(prevMetadataRootPointer)));
  }
}
