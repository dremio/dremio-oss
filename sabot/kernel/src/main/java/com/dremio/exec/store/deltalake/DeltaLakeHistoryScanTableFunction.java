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
package com.dremio.exec.store.deltalake;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.util.VectorUtil;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;

public class DeltaLakeHistoryScanTableFunction extends AbstractTableFunction {
  private final OpProps props;
  private FileSystem fs;
  private final SupportsInternalIcebergTable storagePlugin;

  // inputs
  private VarCharVector pathInputVector;

  // outputs
  private TimeStampMilliVector timestampOutputVector;
  private BigIntVector versionOutputVector;
  private VarCharVector operationOutputVector;

  private int currentRow;
  private boolean rowProcessed;

  public DeltaLakeHistoryScanTableFunction(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    this.props = props;
    try {
      storagePlugin = fec.getStoragePlugin(functionConfig.getFunctionContext().getPluginId());
    } catch (ExecutionSetupException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  private FileSystem getFS(String path) {
    if (fs != null) {
      return fs;
    }

    try {
      fs = storagePlugin.createFS(path, props.getUserName(), context);
      return fs;
    } catch (IOException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);

    // input vectors
    this.pathInputVector =
        (VarCharVector)
            VectorUtil.getVectorFromSchemaPath(
                incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);

    // output vectors
    this.timestampOutputVector =
        (TimeStampMilliVector)
            VectorUtil.getVectorFromSchemaPath(outgoing, SystemSchemas.TIMESTAMP);
    this.versionOutputVector =
        (BigIntVector) VectorUtil.getVectorFromSchemaPath(outgoing, SystemSchemas.VERSION);
    this.operationOutputVector =
        (VarCharVector) VectorUtil.getVectorFromSchemaPath(outgoing, SystemSchemas.OPERATION);

    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    this.rowProcessed = false;
    this.currentRow = row;
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) {
    if (rowProcessed) {
      return 0;
    }

    try {
      String commitPath = new String(pathInputVector.get(currentRow), StandardCharsets.UTF_8);
      DeltaLogSnapshot snapshot =
          DeltaLogCommitJsonReader.parseCommitInfo(getFS(commitPath), Path.of(commitPath));
      writeFileToOutput(startOutIndex, snapshot);
    } catch (Exception e) {
      throw UserException.validationError(e).buildSilently();
    } finally {
      rowProcessed = true;
    }

    outgoing.forEach(vw -> vw.getValueVector().setValueCount(startOutIndex + 1));
    outgoing.setRecordCount(startOutIndex + 1);
    return 1;
  }

  private void writeFileToOutput(int startOutIndex, DeltaLogSnapshot snapshot) {
    versionOutputVector.setSafe(startOutIndex, snapshot.getVersionId());
    timestampOutputVector.setSafe(startOutIndex, snapshot.getTimestamp());
    byte[] operationBytes = snapshot.getOperationType().getBytes(StandardCharsets.UTF_8);
    operationOutputVector.setSafe(startOutIndex, operationBytes, 0, operationBytes.length);
  }

  @Override
  public void closeRow() throws Exception {}

  @Override
  public long getFirstRowSize() {
    long firstRowSize = 16L; // timestamp and version are 8 byte longs
    firstRowSize += operationOutputVector.get(0).length;
    return firstRowSize;
  }
}
