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

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.copyinto.CopyIntoErrorInfo;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.dfs.ErrorHandler;
import com.dremio.exec.store.dfs.system.SystemIcebergTableManager;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * This class is responsible for handling and processing the copy into error information. It implements
 * the ErrorHandler interface with the specific type of error information being CopyIntoErrorInfo.
 */
public class CopyIntoErrorsHandler implements ErrorHandler<CopyIntoErrorInfo> {

  private static final Logger logger = LoggerFactory.getLogger(CopyIntoErrorsHandler.class);
  private SystemIcebergTableManager copyJobHistoryTableManager;
  private SystemIcebergTableManager copyFileHistoryTableManager;
  private final SystemIcebergTableMetadata copyJobHistoryTableMetadata;
  private final SystemIcebergTableMetadata copyFileHistoryTableMetadata;
  private final OperatorContext context;
  private final List<CopyIntoErrorInfo> errorInfos = new LinkedList<>();
  private long numberOfRejectedRecords;
  private long numberOfLoadedRecords;
  private final long batchSize;
  private CopyIntoErrorInfo errorInfo;

  /**
   * Constructs a new instance of {@link CopyIntoErrorsHandler} with the provided {@link SystemIcebergTablesStoragePlugin}.
   *
   * @param plugin The {@link SystemIcebergTablesStoragePlugin} instance to be used for hosting errors system table.
   * @throws NullPointerException if the provided plugin is null.
   */
  public CopyIntoErrorsHandler(OperatorContext context, SystemIcebergTablesStoragePlugin plugin) {
    Preconditions.checkNotNull(plugin, "Copy into error plugin must be not null");
    this.copyJobHistoryTableMetadata = plugin.getTableMetadata(ImmutableList.of(COPY_JOB_HISTORY_TABLE_NAME));
    this.copyJobHistoryTableManager = new SystemIcebergTableManager(context, plugin, copyJobHistoryTableMetadata);
    this.copyFileHistoryTableMetadata = plugin.getTableMetadata(ImmutableList.of(COPY_FILE_HISTORY_TABLE_NAME));
    this.copyFileHistoryTableManager = new SystemIcebergTableManager(context, plugin, copyFileHistoryTableMetadata);
    this.context = context;
    this.batchSize = context.getOptions().getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_WRITE_BATCH_SIZE);
  }

  @VisibleForTesting
  protected void setCopyJobHistoryTableManager(SystemIcebergTableManager copyJobHistoryTableManager) {
    this.copyJobHistoryTableManager = copyJobHistoryTableManager;
  }

  @VisibleForTesting
  protected void setCopyFileHistoryTableManager(SystemIcebergTableManager copyFileHistoryTableManager) {
    this.copyFileHistoryTableManager = copyFileHistoryTableManager;
  }

  /**
   * Commits the records to the Iceberg tables for both copy job history and copy file history.
   * This method writes the records to the corresponding tables and commits the changes.
   *
   * @throws Exception if an error occurs during writing, committing, rolling back, or refreshing
   *                   the dataset metadata.
   */
  @Override
  public void commit() throws Exception {
    try (VectorContainer jobHistoryContainer = CopyJobHistoryTableRecordBuilder
      .buildVector(context.getAllocator(), copyJobHistoryTableMetadata.getSchemaVersion(), errorInfo,
        numberOfLoadedRecords, numberOfRejectedRecords);
         ) {
      logger.debug("Writing copy jobs history to iceberg table");
      copyJobHistoryTableManager.writeRecords(jobHistoryContainer);
      copyJobHistoryTableManager.commit();
    }

    if (errorInfos.isEmpty()) {
      // it means we reached the batch threshold, and some records were already written out in the process() phase
      copyFileHistoryTableManager.commit();
    } else {
      try (VectorContainer fileHistoryContainer = CopyFileHistoryTableRecordBuilder
        .buildVector(context.getAllocator(), copyFileHistoryTableMetadata.getSchemaVersion(), errorInfos)) {
        logger.debug("Writing copy file history to iceberg table");
        copyFileHistoryTableManager.writeRecords(fileHistoryContainer);
        copyFileHistoryTableManager.commit();
      }
    }
    // note: after having rows written and committed (by an executor) here, a dataset refresh is still needed
    // currently taken care by {@link com.dremio.exec.work.protector.Foreman.Observer#handleSystemIcebergTableRefreshes()}
  }

  /**
   * Processes a {@link CopyIntoErrorInfo} object. This method accumulates information from the error
   * info objects, updates the counts of rejected and loaded records, and prepares data for insertion
   * into the copy file history table. When the number of processed error info objects reaches the batch
   * size threshold, a vector container is created from the accumulated error info objects and written to
   * the copy file history table. The newly written out data will be committed when all the {@link CopyIntoErrorInfo}
   * object are processed and the {@link CopyIntoErrorsHandler#commit()} is called.
   *
   * @param errorInfo The {@link CopyIntoErrorInfo} object to be processed.
   * @throws Exception if an error occurs during the processing or writing of the error information.
   */
  @Override
  public void process(CopyIntoErrorInfo errorInfo) throws Exception {
    if (this.errorInfo == null) {
      this.errorInfo = errorInfo;
    }
    numberOfRejectedRecords += errorInfo.getRecordsRejectedCount();
    numberOfLoadedRecords += errorInfo.getRecordsLoadedCount();
    logger.debug("Processed copy into error info object:\n{}\nNew number of rejected records: {}.\nNew number of loaded records: {}.\n ",
      errorInfo, numberOfRejectedRecords, numberOfLoadedRecords);
    errorInfos.add(errorInfo);
    if (errorInfos.size() >= batchSize) {
      // if we reach the batchSize threshold create a vector container using the errorInfos list and write it out to the
      // copy_file_history table.
      try (VectorContainer fileHistoryContainer = CopyFileHistoryTableRecordBuilder
        .buildVector(context.getAllocator(), copyFileHistoryTableMetadata.getSchemaVersion(), errorInfos)) {
        copyFileHistoryTableManager.writeRecords(fileHistoryContainer);
      } finally {
        errorInfos.clear();
      }
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

  @VisibleForTesting
  protected List<CopyIntoErrorInfo> getErrorInfos() {
    return errorInfos;
  }

}
