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

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.util.Tasks;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.util.VectorUtil;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.base.Stopwatch;

/**
 * A table function that deletes the orphan files
 */
public class IcebergOrphanFileDeleteTableFunction extends AbstractTableFunction {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergOrphanFileDeleteTableFunction.class);
  private static final int DELETE_NUM_RETRIES = 3;

  private final FragmentExecutionContext fragmentExecutionContext;
  private final OpProps props;
  private final OperatorStats operatorStats;
  private SupportsIcebergMutablePlugin icebergMutablePlugin;
  private FileSystem fs;

  private VarCharVector inputFilePath;
  private VarCharVector inputFileType;

  private VarCharVector outputFilePath;
  private VarCharVector outputFileType;
  private BigIntVector outputRecords;
  private int inputIndex;
  private boolean doneWithRow;

  public IcebergOrphanFileDeleteTableFunction(
    FragmentExecutionContext fragmentExecutionContext,
    OperatorContext context,
    OpProps props,
    TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    this.fragmentExecutionContext = fragmentExecutionContext;
    this.props = props;
    this.operatorStats = context.getStats();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);
    icebergMutablePlugin = fragmentExecutionContext.getStoragePlugin(functionConfig.getFunctionContext().getPluginId());
    fs = icebergMutablePlugin.createFSWithAsyncOptions(
      functionConfig.getFunctionContext().getFormatSettings().getLocation(), props.getUserName(), context);

    inputFilePath = (VarCharVector) getVectorFromSchemaPath(incoming, SystemSchemas.FILE_PATH);
    inputFileType = (VarCharVector) getVectorFromSchemaPath(incoming, SystemSchemas.FILE_TYPE);

    outputFilePath = (VarCharVector) VectorUtil.getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_PATH);
    outputFileType = (VarCharVector) VectorUtil.getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_TYPE);
    outputRecords = (BigIntVector) VectorUtil.getVectorFromSchemaPath(outgoing, SystemSchemas.RECORDS);

    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    inputIndex = row;
    doneWithRow = false;
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    if (doneWithRow) {
      return 0;
    }
    byte[] filePathBytes = inputFilePath.get(inputIndex);
    String orphanFilePath = new String(filePathBytes, StandardCharsets.UTF_8);
    byte[] fileTypeBytes = inputFileType.get(inputIndex);
    String orphanFileType = new String(fileTypeBytes, StandardCharsets.UTF_8);
    int deleteRecord = deleteFile(orphanFilePath, orphanFileType);
    outputRecords.setSafe(inputIndex, deleteRecord);
    outputFilePath.setSafe(inputIndex, inputFilePath.get(inputIndex));
    outputFileType.setSafe(inputIndex, inputFileType.get(inputIndex));
    outgoing.setAllCount(inputIndex + 1);
    doneWithRow = true;
    return 1;
  }

  @Override
  public void startBatch(int records) {
    outgoing.allocateNew();
  }

  @Override
  public void closeRow() throws Exception {
  }

  private int deleteFile(String orphanFilePath, String fileType) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    // The orphan file list could have duplicate files. When fs.delete() tries to delete the duplicate file, it will
    // return FALSE, if the file is already deleted. Here, we use 'deleteStatus' to track it.
    // In addition, when fs.delete() tries to delete a file, it could fail with exception and the file is not deleted.
    // In this case, we use 'failedToDelete' to track it and this type of failure could be logged into metric.
    AtomicBoolean deleteStatus = new AtomicBoolean(true);
    AtomicBoolean failedToDelete = new AtomicBoolean(false);
    Tasks.foreach(orphanFilePath)
      .retry(DELETE_NUM_RETRIES)
      .stopRetryOn(NotFoundException.class)
      .suppressFailureWhenFinished()
      .onFailure(
        (filePath, exc) -> {
          logger.warn("Delete failed for {}: {}", fileType, filePath, exc);
          failedToDelete.set(true);
        })
      .run(
        filePath -> {
          try {
            String containerRelativePath = Path.getContainerSpecificRelativePath(Path.of(filePath));
            boolean deleted = fs.delete(Path.of(containerRelativePath), false);
            deleteStatus.set(deleted);
          } catch (IOException e) {
            logger.warn("Delete failed for {}: {}", fileType, filePath, e);
            failedToDelete.set(true);
            deleteStatus.set(false);
          }
        });
    long deleteTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    operatorStats.addLongStat(TableFunctionOperator.Metric.DELETE_ORPHAN_FILES_TIME, deleteTime);

    // Track whether a file is deleted successfully.
    if (deleteStatus.get()) {
      operatorStats.addLongStat(TableFunctionOperator.Metric.NUM_ORPHAN_FILES_DELETED, 1);
      return 1;
    } else if (failedToDelete.get()){
      operatorStats.addLongStat(TableFunctionOperator.Metric.NUM_ORPHAN_FILES_FAIL_TO_DELETE, 1);
    }
    return 0;
  }
}
