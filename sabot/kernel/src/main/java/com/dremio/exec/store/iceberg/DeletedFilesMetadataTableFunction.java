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

import static com.dremio.exec.store.OperationType.DELETE_DATAFILE;

import java.util.Optional;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import org.apache.iceberg.DataFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.physical.config.DeletedFilesMetadataTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.util.VectorUtil;
import com.dremio.exec.vector.OptionalVarBinaryVectorHolder;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * A table function that handles updating of the metadata for deleted data files.
 */
public class DeletedFilesMetadataTableFunction extends AbstractTableFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeletedFilesMetadataTableFunction.class);

  private IntVector outputOperationTypeVector;
  private VarCharVector inputFilePathVector;
  private VarCharVector outputPathVector;
  private BigIntVector inputRowCountVector;
  private BigIntVector outputRowCountVector;

  private OptionalVarBinaryVectorHolder inputIcebergMetadataVector;
  private OptionalVarBinaryVectorHolder outputIcebergMetadataVector;

  private BigIntVector outputFileSizeVector; //the file size for the deleted files
  private long currentRowCount;

  private Text inputFilePath;
  private boolean doneWithRow;
  private Optional<byte[]> icebergMetadataBytes;
  private OperationType operationType;

  public DeletedFilesMetadataTableFunction(OperatorContext context, TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    operationType = ((DeletedFilesMetadataTableFunctionContext) functionConfig.getFunctionContext()).getOperationType();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);

    outputOperationTypeVector = (IntVector) VectorUtil.getVectorFromSchemaPath(outgoing, RecordWriter.OPERATION_TYPE_COLUMN);

    inputFilePathVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(incoming, ColumnUtils.FILE_PATH_COLUMN_NAME);
    outputPathVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(outgoing, RecordWriter.PATH_COLUMN);

    inputRowCountVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(incoming, ColumnUtils.ROW_COUNT_COLUMN_NAME);
    outputRowCountVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(outgoing, RecordWriter.RECORDS_COLUMN);

    inputIcebergMetadataVector = new OptionalVarBinaryVectorHolder(incoming, SystemSchemas.ICEBERG_METADATA);
    outputIcebergMetadataVector = new OptionalVarBinaryVectorHolder(outgoing, RecordWriter.ICEBERG_METADATA_COLUMN);

    outputFileSizeVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(outgoing, RecordWriter.FILESIZE_COLUMN);

    return outgoing;
  }

  @Override
  public void startBatch(int records) {
    outgoing.allocateNew();
  }

  @Override
  public void startRow(int row) throws Exception {
    inputFilePath = inputFilePathVector.getObject(row);
    doneWithRow = false;
    currentRowCount = inputRowCountVector.get(row);

    icebergMetadataBytes = inputIcebergMetadataVector.get(row);
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    if (doneWithRow) {
      return 0;
    } else {
      // OperationType is always OperationType.DELETE_DATAFILE.
      outputOperationTypeVector.setSafe(startOutIndex, this.operationType.value);
      // inputFilePath is null for inserted rows in Merge query, skip
      if (inputFilePath != null) {
        outputPathVector.setSafe(startOutIndex, inputFilePath);
      }
      outputRowCountVector.setSafe(startOutIndex, currentRowCount);
      icebergMetadataBytes.ifPresent(bytes -> outputIcebergMetadataVector.setSafe(startOutIndex, () -> bytes));

      if(icebergMetadataBytes.isPresent() && this.operationType == DELETE_DATAFILE){
        final IcebergMetadataInformation icebergMetadataInformation = IcebergSerDe.deserializeFromByteArray(icebergMetadataBytes.get());

        final DataFile currentDataFile = IcebergSerDe.deserializeDataFile(icebergMetadataInformation.getIcebergMetadataFileByte());
        outputFileSizeVector.setSafe(startOutIndex,currentDataFile.fileSizeInBytes());
      } else {
        outputFileSizeVector.setSafe(startOutIndex,0);
      }

      outgoing.setAllCount(startOutIndex + 1);
      doneWithRow = true;
      return 1;
    }
  }

  @Override
  public void closeRow() throws Exception {
  }
}
