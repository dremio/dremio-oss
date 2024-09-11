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
package com.dremio.exec.store.dfs.copyinto;

import static com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.COPY_FILE_HISTORY_TABLE_NAME;
import static com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.COPY_JOB_HISTORY_TABLE_NAME;

import com.dremio.common.AutoCloseables;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.planner.sql.handlers.SystemIcebergTableOpUtils;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.dfs.HistoryEventHandler;
import com.dremio.exec.store.dfs.system.SystemIcebergTableManager;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for handling and processing the copy into file load event information.
 * It implements the {@link HistoryEventHandler} interface with the specific type of history event
 * information being {@link CopyIntoFileLoadInfo}.
 */
public class CopyIntoHistoryEventHandler implements HistoryEventHandler<CopyIntoFileLoadInfo> {

  private static final Logger logger = LoggerFactory.getLogger(CopyIntoHistoryEventHandler.class);
  private SystemIcebergTableManager copyJobHistoryTableManager;
  private SystemIcebergTableManager copyFileHistoryTableManager;
  private final VectorContainer jobHistoryContainer;
  private final VectorContainer fileHistoryContainer;
  private final Table copyJobHistoryTable;
  private final Table copyFileHistoryTable;
  private final OperatorContext context;
  private long numberOfRejectedRecords;
  private long numberOfLoadedRecords;
  private final long batchSize;
  private final SystemIcebergTableMetadata jobHistoryTableMetadata;
  private final SystemIcebergTableMetadata fileHistoryTableMetadata;
  private CopyIntoFileLoadInfo fileLoadInfo;
  private long processingStartTime = System.currentTimeMillis();
  private Long recordTimestamp;
  private final List<ValueVector> jobHistoryPartitionVectors;
  private final List<ValueVector> fileHistoryPartitionVectors;

  /**
   * Constructs a new instance of {@link CopyIntoHistoryEventHandler} with the provided {@link
   * SystemIcebergTablesStoragePlugin}.
   *
   * @param plugin The {@link SystemIcebergTablesStoragePlugin} instance to be used for hosting copy
   *     history system table.
   * @throws NullPointerException if the provided plugin is null.
   */
  public CopyIntoHistoryEventHandler(
      OperatorContext context, SystemIcebergTablesStoragePlugin plugin) {
    Preconditions.checkNotNull(plugin, "system_iceberg_tables plugin must be not null");
    this.context = context;
    this.jobHistoryTableMetadata =
        plugin.getTableMetadata(ImmutableList.of(COPY_JOB_HISTORY_TABLE_NAME));
    this.copyJobHistoryTable =
        Preconditions.checkNotNull(
            plugin.getTable(jobHistoryTableMetadata.getTableLocation()),
            "Failed to load JOB HISTORY table.");
    this.copyJobHistoryTableManager =
        new SystemIcebergTableManager(context, plugin, jobHistoryTableMetadata);
    this.fileHistoryTableMetadata =
        plugin.getTableMetadata(ImmutableList.of(COPY_FILE_HISTORY_TABLE_NAME));
    this.copyFileHistoryTableManager =
        new SystemIcebergTableManager(context, plugin, fileHistoryTableMetadata);
    this.copyFileHistoryTable =
        Preconditions.checkNotNull(
            plugin.getTable(fileHistoryTableMetadata.getTableLocation()),
            "Failed to load FILE HISTORY table.");
    this.batchSize =
        context.getOptions().getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_WRITE_BATCH_SIZE);
    this.jobHistoryContainer =
        CopyJobHistoryTableVectorContainerBuilder.initializeContainer(
            context.getAllocator(), jobHistoryTableMetadata.getIcebergSchema());
    this.fileHistoryContainer =
        CopyFileHistoryTableVectorContainerBuilder.initializeContainer(
            context.getAllocator(), fileHistoryTableMetadata.getIcebergSchema());
    this.jobHistoryPartitionVectors =
        CopyJobHistoryPartitionTransformVectorBuilder.initializeValueVectors(
            context.getAllocator(), jobHistoryTableMetadata.getPartitionSpec());
    this.fileHistoryPartitionVectors =
        CopyFileHistoryPartitionTransformVectorBuilder.initializeValueVectors(
            context.getAllocator(), fileHistoryTableMetadata.getPartitionSpec());
  }

  @VisibleForTesting
  protected void setCopyJobHistoryTableManager(
      SystemIcebergTableManager copyJobHistoryTableManager) {
    this.copyJobHistoryTableManager = copyJobHistoryTableManager;
  }

  @VisibleForTesting
  protected void setCopyFileHistoryTableManager(
      SystemIcebergTableManager copyFileHistoryTableManager) {
    this.copyFileHistoryTableManager = copyFileHistoryTableManager;
  }

  /**
   * Commits the records to the Iceberg tables for both copy job history and copy file history. This
   * method writes the records to the corresponding tables and commits the changes.
   *
   * @throws Exception if an error occurs during writing, committing, rolling back, or refreshing
   *     the dataset metadata.
   */
  @Override
  public void commit() throws Exception {
    long processingTime = System.currentTimeMillis() - processingStartTime;
    CopyJobHistoryTableVectorContainerBuilder.writeRecordToContainer(
        jobHistoryContainer,
        fileLoadInfo,
        numberOfLoadedRecords,
        numberOfRejectedRecords,
        processingTime,
        recordTimestamp);
    CopyJobHistoryPartitionTransformVectorBuilder.transformValueVectors(
        jobHistoryContainer,
        jobHistoryPartitionVectors,
        jobHistoryTableMetadata.getIcebergSchema(),
        jobHistoryTableMetadata.getPartitionSpec());
    logger.debug("Writing copy jobs history to iceberg table");
    copyJobHistoryTableManager.writeRecords(jobHistoryContainer, jobHistoryPartitionVectors);
    copyJobHistoryTableManager.commit();

    if (fileHistoryContainer.getRecordCount() == 0) {
      // it means we reached the batch threshold, and some records were already written out in the
      // process() phase
      copyFileHistoryTableManager.commit();
    } else {
      CopyFileHistoryPartitionTransformVectorBuilder.transformValueVectors(
          fileHistoryContainer,
          fileHistoryPartitionVectors,
          fileHistoryTableMetadata.getIcebergSchema(),
          fileHistoryTableMetadata.getPartitionSpec());
      logger.debug("Writing copy file history to iceberg table");
      try {
        copyFileHistoryTableManager.writeRecords(fileHistoryContainer, fileHistoryPartitionVectors);
        copyFileHistoryTableManager.commit();
      } catch (RuntimeException runtimeException) {
        String queryId = QueryIdHelper.getQueryId(context.getFragmentHandle().getQueryId());
        SystemIcebergTableOpUtils.getStampedAppendSnapshot(copyJobHistoryTable, queryId)
            .ifPresent(
                snapshot ->
                    IcebergUtils.revertSnapshotFiles(copyJobHistoryTable, snapshot, queryId));
      }
    }
    // note: after having rows written and committed (by an executor) here, a dataset refresh is
    // still needed
    // currently taken care by {@link
    // com.dremio.exec.work.protector.Foreman.Observer#handleSystemIcebergTableRefreshes()}
  }

  /**
   * Processes a {@link CopyIntoFileLoadInfo} object. This method accumulates information from the
   * {@link CopyIntoFileLoadInfo} objects, updates the counts of rejected and loaded records, and
   * prepares data for insertion into the copy file history table. When the number of processed
   * {@link CopyIntoFileLoadInfo} reaches the batch size threshold, a vector container is created
   * from the accumulated {@link CopyIntoFileLoadInfo} and written to the copy file history table.
   * The newly written out data will be committed when all the {@link CopyIntoFileLoadInfo} object
   * are processed and the {@link CopyIntoHistoryEventHandler#commit()} is called.
   *
   * @param fileLoadInfo The {@link CopyIntoFileLoadInfo} object to be processed.
   * @throws Exception if an error occurs during the processing or writing of the file load info
   *     event information.
   */
  @Override
  public void process(CopyIntoFileLoadInfo fileLoadInfo) throws Exception {
    if (this.fileLoadInfo == null) {
      this.fileLoadInfo = fileLoadInfo;
    }
    numberOfRejectedRecords += fileLoadInfo.getRecordsRejectedCount();
    numberOfLoadedRecords += fileLoadInfo.getRecordsLoadedCount();
    processingStartTime = Math.min(processingStartTime, fileLoadInfo.getProcessingStartTime());
    recordTimestamp = System.currentTimeMillis();
    logger.debug(
        "Processed copy into file load info object:\n{}\nNew number of rejected records: {}.\nNew number of loaded records: {}.\n ",
        fileLoadInfo,
        numberOfRejectedRecords,
        numberOfLoadedRecords);
    CopyFileHistoryTableVectorContainerBuilder.writeRecordToContainer(
        fileHistoryContainer, fileLoadInfo, recordTimestamp);
    if (fileHistoryContainer.getRecordCount() >= batchSize) {
      // if we reach the batchSize threshold create a vector container using the FileLoadInfo
      // object's list and
      // write it out to the copy_file_history table.
      CopyFileHistoryPartitionTransformVectorBuilder.transformValueVectors(
          fileHistoryContainer,
          fileHistoryPartitionVectors,
          fileHistoryTableMetadata.getIcebergSchema(),
          fileHistoryTableMetadata.getPartitionSpec());
      try {
        copyFileHistoryTableManager.writeRecords(fileHistoryContainer, fileHistoryPartitionVectors);
      } finally {
        fileHistoryContainer.setAllCount(0);
        fileHistoryPartitionVectors.forEach(ValueVector::close);
      }
    }
  }

  @Override
  public void revert(String queryId, Exception ex) {
    logger.warn("Attempting to revert commits to copy history tables by job {}", queryId);

    if (copyFileHistoryTable != null) {
      revertSnapshotsFromTable(copyFileHistoryTable, queryId, ex);
    }
    if (copyJobHistoryTable != null) {
      revertSnapshotsFromTable(copyJobHistoryTable, queryId, ex);
    }
  }

  @VisibleForTesting
  protected long getNumberOfRejectedRecords() {
    return numberOfRejectedRecords;
  }

  @VisibleForTesting
  protected long getNumberOfLoadedRecords() {
    return numberOfLoadedRecords;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close((AutoCloseable) jobHistoryContainer, fileHistoryContainer);
    AutoCloseables.close(jobHistoryPartitionVectors);
    AutoCloseables.close(fileHistoryPartitionVectors);
  }

  private void revertSnapshotsFromTable(Table table, String queryId, Exception ex) {
    try {
      table.refresh();
      SystemIcebergTableOpUtils.getStampedAppendSnapshot(table, queryId)
          .ifPresent(snapshot -> IcebergUtils.revertSnapshotFiles(table, snapshot, queryId));
    } catch (Exception e) {
      ex.addSuppressed(e);
    }
  }
}
