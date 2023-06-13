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
import static com.dremio.sabot.op.writer.WriterCommitterOperator.SnapshotCommitStatus.SKIPPED;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergOptimizeSingleFileTracker;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.ImmutableWriterCommitterRecord;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.dremio.sabot.op.writer.WriterCommitterOutputHandler;
import com.dremio.sabot.op.writer.WriterCommitterRecord;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;

/**
 * Class used to commit OPTIMIZE TABLE operation, which typically rewrites the data files in the optimal form.
 */
public class IcebergOptimizeOperationCommitter implements IcebergOpCommitter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergOptimizeOperationCommitter.class);
  private static final Set<Field> WRITE_FIELDS = ImmutableSet.of(RecordWriter.RECORDS, RecordWriter.OPERATION_TYPE);

  private final Set<DataFile> addedDataFiles = new HashSet<>();
  private final Set<DataFile> removedDataFiles = new HashSet<>();
  private final Set<DeleteFile> removedDeleteFiles = new HashSet<>();

  private final IcebergCommand icebergCommand;
  private final OperatorStats operatorStats;
  private final String prevMetadataRootPointer;
  private final Optional<Long> minInputFiles;
  private final IcebergOptimizeSingleFileTracker singleRewriteTracker;
  private final IcebergTableProps tableProps;
  private final FileSystem fs;
  private final Long startingSnapshotId;

  public IcebergOptimizeOperationCommitter(IcebergCommand icebergCommand,
                                           OperatorStats operatorStats,
                                           DatasetConfig datasetConfig,
                                           Long minInputFiles,
                                           Long snapshotId,
                                           IcebergTableProps tableProps,
                                           FileSystem fs) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    Preconditions.checkNotNull(datasetConfig.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation());
    this.operatorStats = operatorStats;
    this.icebergCommand = icebergCommand;
    this.prevMetadataRootPointer = datasetConfig.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation();
    this.minInputFiles = Optional.ofNullable(minInputFiles);
    this.singleRewriteTracker = new IcebergOptimizeSingleFileTracker();
    this.tableProps = tableProps;
    this.fs = fs;
    this.startingSnapshotId = snapshotId;
  }

  @Override
  public Snapshot commit() {
    throw new IllegalStateException(this.getClass().getName() + " requires access to outgoing vectors for writing the output");
  }

  @Override
  public Snapshot commit(WriterCommitterOutputHandler outputHandler) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Snapshot snapshot = null;
    WriterCommitterOperator.SnapshotCommitStatus commitStatus = NONE;
    try {
      Set<String> skippedSingleRewrites = singleRewriteTracker.removeSingleFileChanges(addedDataFiles, removedDataFiles);
      boolean shouldCommit = hasAnythingChanged() && hasMinInputFilesCriteriaPassed();
      snapshot = shouldCommit ?
        icebergCommand.rewriteFiles(removedDataFiles, removedDeleteFiles, addedDataFiles, Collections.EMPTY_SET, startingSnapshotId)
        : icebergCommand.loadTable().currentSnapshot();
      writeOutput(outputHandler, !shouldCommit, removedDataFiles.size(), removedDeleteFiles.size(), addedDataFiles.size());

      LOGGER.info("OPTIMIZE ACTION: Rewritten data files count - {}, Rewritten delete files count - {}, Added data files count - {}, Min input files - {}, Commit skipped {}",
        removedDataFiles.size(), removedDeleteFiles.size(), addedDataFiles.size(), minInputFiles.map(String::valueOf).orElse("NONE"), !shouldCommit);
      clear(shouldCommit, skippedSingleRewrites);
      commitStatus = shouldCommit ? COMMITTED : SKIPPED;

      return snapshot;
    } finally {
      long totalCommitTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_COMMIT_TIME, totalCommitTime);
      IcebergOpCommitter.writeSnapshotStats(operatorStats, commitStatus, snapshot);
    }
  }

  private void writeOutput(WriterCommitterOutputHandler outputHandler, boolean commitSkipped,
                           long rewrittenFilesCount, long rewrittenDeleteFilesCount, long addedFilesCount) {
    if (commitSkipped) {
      rewrittenFilesCount = 0L;
      rewrittenDeleteFilesCount = 0L;
      addedFilesCount = 0L;
    }

    WriterCommitterRecord rewrittenFiles = new ImmutableWriterCommitterRecord.Builder()
      .setOperationType(OperationType.DELETE_DATAFILE.value).setRecords(rewrittenFilesCount).build();
    outputHandler.write(rewrittenFiles);

    WriterCommitterRecord rewrittenDeleteFiles = new ImmutableWriterCommitterRecord.Builder()
      .setOperationType(OperationType.DELETE_DELETEFILE.value).setRecords(rewrittenDeleteFilesCount).build();
    outputHandler.write(rewrittenDeleteFiles);

    WriterCommitterRecord addedFiles = new ImmutableWriterCommitterRecord.Builder()
      .setOperationType(OperationType.ADD_DATAFILE.value).setRecords(addedFilesCount).build();
    outputHandler.write(addedFiles);
  }

  private void clear(boolean isCommitted, Set<String> skippedFiles) {
    Stopwatch stopwatch = Stopwatch.createStarted();

    skippedFiles.forEach(this::deleteOrphan);
    if (!isCommitted) {
      // Remove new files, as they're now orphan
      addedDataFiles.forEach(file
        -> deleteOrphan(file.path().toString()));
      final String orphanDir = Path.of(tableProps.getTableLocation()).resolve(tableProps.getUuid()).toString();
      deleteOrphan(orphanDir);
    }
    removedDataFiles.clear();
    removedDeleteFiles.clear();
    addedDataFiles.clear();

    long clearTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    operatorStats.addLongStat(WriterCommitterOperator.Metric.CLEAR_ORPHANS_TIME, clearTime);
  }

  private void deleteOrphan(String path) {
    try {
      LOGGER.debug("Removing orphan file: " + path);
      fs.delete(Path.of(path), true);
    } catch (IOException e) {
      LOGGER.warn("Unable to delete newly added files {}", path);
      // Not an error condition if cleanup fails; VACUUM can be used to remove left-over orphan files.
    }
  }

  @Override
  public void cleanup(FileIO fileIO) {
    addedDataFiles.forEach(addedDataFile -> fileIO.deleteFile(addedDataFile.path().toString()));
  }

  @Override
  public void consumeManifestFile(ManifestFile icebergManifestFile) {
    throw new UnsupportedOperationException("OPTIMIZE TABLE can't consume pre-prepared manifest files");
  }

  @Override
  public void consumeAddDataFile(DataFile addDataFile) throws UnsupportedOperationException {
    this.addedDataFiles.add(addDataFile);
    this.singleRewriteTracker.consumeAddDataFile(addDataFile);
  }

  @Override
  public void consumeDeleteDataFile(DataFile deleteDataFile) throws UnsupportedOperationException {
    this.removedDataFiles.add(deleteDataFile);
    this.singleRewriteTracker.consumeDeletedDataFile(deleteDataFile);
  }

  @Override
  public void consumeDeleteDataFilePath(String icebergDeleteDatafilePath) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("OPTIMIZE TABLE can't consume string paths");
  }

  @Override
  public void consumeDeleteDeleteFile(DeleteFile deleteFile) throws UnsupportedOperationException {
    this.removedDeleteFiles.add(deleteFile);
    this.singleRewriteTracker.consumeDeletedDeleteFile(deleteFile);
  }

  @Override
  public void updateSchema(BatchSchema newSchema) {
    throw new UnsupportedOperationException("Updating schema is not supported for OPTIMIZE TABLE transaction");
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

  private boolean hasAnythingChanged() {
    return !removedDataFiles.isEmpty() && !addedDataFiles.isEmpty();
  }

  /*
  * MIN_INPUT_FILES is applied to a total of removed files (both data and delete files).
  * e.g. 1 Data file with 4 Delete files linked to it would qualify the default MIN_INPUT_FILES criteria of 5.
   */
  private boolean hasMinInputFilesCriteriaPassed() {
    return minInputFiles.map(m -> m > 0 && (removedDataFiles.size() + removedDeleteFiles.size()) >= m).orElse(true);
  }

  @VisibleForTesting
  Set<DataFile> getAddedDataFiles() {
    return ImmutableSet.copyOf(addedDataFiles);
  }

  @VisibleForTesting
  Set<DataFile> getRemovedDataFiles() {
    return ImmutableSet.copyOf(removedDataFiles);
  }

  @VisibleForTesting
  Set<DeleteFile> getRemovedDeleteFiles() {
    return ImmutableSet.copyOf(removedDeleteFiles);
  }
}
