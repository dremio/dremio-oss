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

import java.util.Arrays;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;

import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Specialized implementation of a streaming list aggregation for delete file paths associated with a data file.  Input
 * to this table function are rows with the following columns, clustered by datafilePath:
 *   - datafilePath
 *   - fileSize
 *   - partitionInfo
 *   - colIds
 *   - deletefilePath
 *
 * Input columns other than deletefilePath are assumed to only change when the datafilePath changes.  Only datafilePath
 * is used for grouping purposes.
 *
 * Null delete file paths will not be added to the list.  In the case where a data file has no applicable delete
 * files - there is one input row for a data file with a null delete file path - it will output an empty list.
 *
 * As an example, given the following input rows:
 *
 *   data1.parquet, size1, partition1, colids1, delete1.parquet
 *   data1.parquet, size1, partition1, colids1, delete2.parquet
 *   data2.parquet, size2, partition2, colids2, delete1.parquet
 *   data3.parquet, size3, partition3, colids3, delete2.parquet
 *   data3.parquet, size3, partition3, colids3, delete3.parquet
 *   data4.parquet, size4, partition4, colids4, null
 *
 * It would output:
 *
 *   data1.parquet, size1, partition1, colids1, [ delete1.parquet, delete2.parquet ]
 *   data2.parquet, size2, partition2, colids2, [ delete1.parquet ]
 *   data3.parquet, size3, partition3, colids3, [ delete2.parquet, delete3.parquet ]
 *   data4.parquet, size4, partition4, colids4, []
 *
 * The aggregation is done in a set of temporary current value holders - including both the grouping/non-aggregate
 * columns and the deleted data files list aggregation.  Once all rows for the current data file are processed,
 * the aggregated row is output and the new data file's column values are copied to the current value holders and the
 * process repeats.
 */
public class IcebergDeleteFileAggTableFunction extends AbstractTableFunction {

  private enum State {
    /**
     * The START_NEW_AGG state is responsible for populating the current value holders with the data file path, other
     * non-aggregate columns (fileSize, partitionInfo, and colIds), and starting a new delete file path list.
     * START_NEW_AGG transitions to the CONTINUE_AGG state.
     */
    START_NEW_AGG,

    /**
     * The CONTINUE_AGG state checks whether the new input row is for a different data file than the previous row.
     * If it is different, it copies values from the "current" holders into the output row and transitions to the
     * START_NEW_AGG state.  Otherwise, it adds the delete file path for the input row to the "current" delete file
     * list.
     */
    CONTINUE_AGG,

    /**
     * OUTPUT_LAST_AGG is responsible for producing the final aggregate output row once there is no more input data
     * to consume.  hasBufferedRemaining() will transition into this state from the CONTINUE_AGG state.
     * OUTPUT_LAST_AGG transitions to the DONE state.
     */
    OUTPUT_LAST_AGG,

    /**
     * Once the last output row has been written, the DONE state ensures we return 0 from processRow to communicate
     * we are done to the caller.
     */
    DONE
  };

  private VarCharVector inputDataFilePath;
  private BigIntVector inputFileSize;
  private VarBinaryVector inputPartitionInfo;
  private VarBinaryVector inputColIds;
  private StructVector inputDeleteFile;
  private IntVector inputSpecId;
  private VarCharVector outputDataFilePath;
  private BigIntVector outputFileSize;
  private VarBinaryVector outputPartitionInfo;
  private VarBinaryVector outputColIds;
  private ListVector outputDeleteFiles;
  private IntVector outputSpecId;
  private int inputIndex;
  private byte[] currentDataFilePath;
  private Long currentFileSize;
  private byte[] currentPartitionInfo;
  private byte[] currentColIds;
  private int currentSpecID;
  private ListVector currentDeleteFiles;
  private UnionListWriter currentDeleteFilesWriter;
  private State state;

  public IcebergDeleteFileAggTableFunction(OperatorContext context, TableFunctionConfig functionConfig) {
    super(context, functionConfig);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);

    inputDataFilePath = (VarCharVector) getVectorFromSchemaPath(incoming, SystemSchemas.DATAFILE_PATH);
    inputFileSize = (BigIntVector) getVectorFromSchemaPath(incoming, SystemSchemas.FILE_SIZE);
    inputPartitionInfo = (VarBinaryVector) getVectorFromSchemaPath(incoming, SystemSchemas.PARTITION_INFO);
    inputColIds = (VarBinaryVector) getVectorFromSchemaPath(incoming, SystemSchemas.COL_IDS);
    inputDeleteFile = (StructVector) getVectorFromSchemaPath(incoming, SystemSchemas.DELETE_FILE);
    inputSpecId = (IntVector) getVectorFromSchemaPath(incoming, SystemSchemas.PARTITION_SPEC_ID);
    outputDataFilePath = (VarCharVector) getVectorFromSchemaPath(outgoing, SystemSchemas.DATAFILE_PATH);
    outputFileSize = (BigIntVector) getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_SIZE);
    outputPartitionInfo = (VarBinaryVector) getVectorFromSchemaPath(outgoing, SystemSchemas.PARTITION_INFO);
    outputColIds = (VarBinaryVector) getVectorFromSchemaPath(outgoing, SystemSchemas.COL_IDS);
    outputDeleteFiles = (ListVector) getVectorFromSchemaPath(outgoing, SystemSchemas.DELETE_FILES);
    outputSpecId = (IntVector) getVectorFromSchemaPath(outgoing, SystemSchemas.PARTITION_SPEC_ID);

    currentDeleteFiles = ListVector.empty(SystemSchemas.DELETE_FILES, context.getAllocator());
    currentDeleteFilesWriter = currentDeleteFiles.getWriter();

    state = State.START_NEW_AGG;
    return outgoing;
  }

  @Override
  public void startBatch(int records) {
    outgoing.allocateNew();
  }

  @Override
  public void startRow(int row) throws Exception {
    inputIndex = row;
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    int outputCount = 0;

    switch (state) {
      case START_NEW_AGG:
        // when starting a new aggregation, copy all input columns to the "current" value holders
        copyInputToCurrent(inputIndex);
        state = State.CONTINUE_AGG;
        break;

      case CONTINUE_AGG:
        // continuation of the current aggregate - if datafilePath is the same as the previous row, continue
        // the aggregation by appending deletefilePath to the list, otherwise output the current aggregation and
        // switch to the START_NEW_AGG state
        byte[] nextPath = inputDataFilePath.get(inputIndex);
        if (Arrays.equals(currentDataFilePath, nextPath)) {
          appendInputDeleteFileToCurrent(inputIndex);
        } else {
          copyCurrentToOutput(startOutIndex);
          outputCount = 1;
          // no need to copy the current input row to the "current" value holders, we will be called again for this
          // input row with the START_NEW_AGG state
          state = State.START_NEW_AGG;
        }
        break;

      case OUTPUT_LAST_AGG:
        // no more input available, output the current agg
        copyCurrentToOutput(startOutIndex);
        outputCount = 1;
        state = State.DONE;
        break;

      case DONE:
        // do nothing
        break;

      default:
        throw new AssertionError("Invalid state in processRow: " + state);
    }

    return outputCount;
  }

  @Override
  public void closeRow() throws Exception {
  }

  @Override
  public boolean hasBufferedRemaining() {
    switch (state) {
      case CONTINUE_AGG:
        // state should be CONTINUE_AGG once input is fully consumed - switch to OUTPUT_LAST_AGG so we flush the last
        // aggregate row
        state = State.OUTPUT_LAST_AGG;
        return true;
      case START_NEW_AGG:
      case DONE:
        // if there were zero input rows from upstream (we will be in START_NEW_AGG state in this case), or once the
        // last row is flushed (DONE), tell the operator we're done
        return false;
      default:
        throw new AssertionError("Invalid state in hasBufferedRemaining: " + state);
    }
  }

  @Override
  public void close() throws Exception {
    currentDeleteFiles.close();
    super.close();
  }

  private void copyInputToCurrent(int inputIndex) {
    currentDataFilePath = inputDataFilePath.get(inputIndex);
    currentFileSize = inputFileSize.get(inputIndex);
    currentPartitionInfo = inputPartitionInfo.get(inputIndex);
    currentColIds = inputColIds.get(inputIndex);
    currentSpecID = inputSpecId.get(inputIndex);
    currentDeleteFilesWriter.setPosition(0);
    currentDeleteFilesWriter.startList();
    appendInputDeleteFileToCurrent(inputIndex);
  }

  private void appendInputDeleteFileToCurrent(int inputIndex) {
    if (inputDeleteFile.isSet(inputIndex) == 1) {
      currentDeleteFilesWriter.struct();
      FieldReader reader = inputDeleteFile.getReader();
      reader.setPosition(inputIndex);
      ComplexCopier.copy(reader, currentDeleteFilesWriter);
    }
  }

  private void copyCurrentToOutput(int outputIndex) {
    outputDataFilePath.setSafe(outputIndex, currentDataFilePath);
    outputFileSize.setSafe(outputIndex, currentFileSize);
    outputPartitionInfo.setSafe(outputIndex, currentPartitionInfo);
    outputColIds.setSafe(outputIndex, currentColIds);
    outputSpecId.setSafe(outputIndex, currentSpecID);

    currentDeleteFilesWriter.endList();
    outputDeleteFiles.copyFromSafe(0, outputIndex, currentDeleteFiles);

    outgoing.setAllCount(outputIndex + 1);
  }
}
