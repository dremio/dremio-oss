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
import static com.dremio.sabot.op.writer.WriterCommitterOperator.SnapshotCommitStatus.SKIPPED;

import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.io.FileIO;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.manifestwriter.IcebergCommitOpHelper;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Class used to commit CTAS operation
 */
public class IcebergTableCreationCommitter implements IcebergOpCommitter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergTableCreationCommitter.class);
  private final List<ManifestFile> manifestFileList = new ArrayList<>();
  protected final IcebergCommand icebergCommand;
  private final OperatorStats operatorStats;

  public IcebergTableCreationCommitter(String tableName, BatchSchema batchSchema, List<String> partitionColumnNames,
                                       IcebergCommand icebergCommand, Map<String, String> tableProperties, OperatorStats operatorStats, PartitionSpec partitionSpec, SortOrder sortOrder) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    Preconditions.checkState(batchSchema != null, "Schema must be present");
    Preconditions.checkState(tableName != null, "Table name must be present");
    this.icebergCommand = icebergCommand;
    this.icebergCommand.beginCreateTableTransaction(tableName, batchSchema, partitionColumnNames, tableProperties, partitionSpec, sortOrder);
    this.operatorStats = operatorStats;
  }

  public IcebergTableCreationCommitter(String tableName, BatchSchema batchSchema, List<String> partitionColumnNames, IcebergCommand icebergCommand, OperatorStats operatorStats, PartitionSpec partitionSpec, SortOrder sortOrder) {
    this(tableName, batchSchema, partitionColumnNames, icebergCommand, Collections.emptyMap(), operatorStats, partitionSpec, sortOrder);
  }

  @Override
  public Snapshot commit() {
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      icebergCommand.beginInsert();
      logger.debug("Committing manifest files [Path , filecount] {} ",
        manifestFileList.stream().map(l -> new ImmutablePair(l.path(), l.addedFilesCount())).collect(Collectors.toList()));
      icebergCommand.consumeManifestFiles(manifestFileList);
      icebergCommand.finishInsert();
      Snapshot snapshot = icebergCommand.endTransaction().currentSnapshot();
      long totalCommitTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      /* OperatorStats are null when create empty table is executed via Coordinator*/
      if(operatorStats != null) {
        operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_COMMIT_TIME, totalCommitTime);
        IcebergOpCommitter.writeSnapshotStats(operatorStats, COMMITTED, snapshot);
      }

      return snapshot;
    }catch(Exception e){
      try {
        icebergCommand.deleteTable();
      }catch(Exception i){
        logger.warn("Failure during cleaning up the unwanted files", i);
      }
      if(operatorStats != null) {
        operatorStats.addLongStat(WriterCommitterOperator.Metric.SNAPSHOT_COMMIT_STATUS, SKIPPED.value());
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup(FileIO fileIO) {
    IcebergCommitOpHelper.deleteManifestFiles(fileIO, manifestFileList, true);
  }

  @Override
  public void consumeManifestFile(ManifestFile icebergManifestFile) {
    manifestFileList.add(icebergManifestFile);
  }

  @Override
  public void consumeDeleteDataFile(DataFile icebergDeleteDatafile) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Delete data file operation not allowed in Create table Transaction");
  }

  @Override
  public void consumeDeleteDataFilePath(String icebergDeleteDatafilePath) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Delete data file operation not allowed in Create table Transaction");
  }

  @Override
  public void updateSchema(BatchSchema newSchema) {
    throw new UnsupportedOperationException("Updating schema is not supported for Creation table Transaction");
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
    return icebergCommand.getRootPointer() != null;
  }
}
