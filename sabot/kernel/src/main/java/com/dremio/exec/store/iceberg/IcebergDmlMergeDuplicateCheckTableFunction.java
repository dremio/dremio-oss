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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.BasicTypeHelper;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * A table function that'll detect duplicate rows for the MERGE DML (using the file path and the row index).
 * Currently, this only impacts MERGE with UPDATEs.
 */
public class IcebergDmlMergeDuplicateCheckTableFunction extends AbstractTableFunction {

  private final List<TransferPair> transfers = new ArrayList<>();

  private VarCharVector filePathVector;
  private BigIntVector rowIndexVector;

  private ArrowBuf previousFilePathBuf;
  private long previousFilePathBufLength = 0;
  private Long previousRowIndex = null;

  private boolean doneWithRow;

  public IcebergDmlMergeDuplicateCheckTableFunction(OperatorContext context, TableFunctionConfig functionConfig) {
    super(context, functionConfig);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);

    previousFilePathBuf = context.getManagedBuffer();

    for (Field field : incoming.getSchema()) {
      transfers.add(getVectorFromSchemaPath(incoming, field.getName())
        .makeTransferPair(getVectorFromSchemaPath(outgoing, field.getName())));
    }

    // Since we will transfer all data immediately, we'll get data from the outgoing vectors.
    filePathVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(outgoing, ColumnUtils.FILE_PATH_COLUMN_NAME);
    rowIndexVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(outgoing, ColumnUtils.ROW_INDEX_COLUMN_NAME);

    return outgoing;
  }

  @Override
  public void startBatch(int records) {
    // We immediately transfer all the input vectors to the output vectors because this table function is basically
    // a pass-through table function where we do a check amongst the consecutive rows.
    transfers.forEach(TransferPair::transfer);
    outgoing.setAllCount(records);
  }

  @Override
  public void startRow(int row) throws Exception {
    doneWithRow = false;
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    if (doneWithRow) {
      return 0;
    } else {
      // We only need to check the previous file path and the row index because everything is sequential!
      Long currentRowIndex = rowIndexVector.getObject(startOutIndex);
      NullableVarCharHolder currentFilePathVectorHolder = (NullableVarCharHolder) BasicTypeHelper.getValue(filePathVector, startOutIndex);
      int currentFilePathBufLength = currentFilePathVectorHolder.end - currentFilePathVectorHolder.start;

      if (previousRowIndex != null && previousRowIndex.equals(currentRowIndex)
        && previousFilePathBufLength > 0
        && currentFilePathVectorHolder.isSet == 1
        && previousFilePathBufLength == currentFilePathBufLength
        && ByteFunctionHelpers.compare(currentFilePathVectorHolder.buffer, currentFilePathVectorHolder.start, currentFilePathVectorHolder.end, previousFilePathBuf, 0, previousFilePathBufLength) == 0) {
            throw UserException.validationError().message("A target row matched more than once. Please update your query.").buildSilently();
      }

      if (currentFilePathVectorHolder.isSet == 1) {
        previousFilePathBufLength = currentFilePathBufLength;
        previousFilePathBuf.setBytes(0, currentFilePathVectorHolder.buffer, currentFilePathVectorHolder.start, previousFilePathBufLength);
      } else {
        previousFilePathBufLength = 0;
      }

      previousRowIndex = currentRowIndex;

      doneWithRow = true;
      return 1;
    }
  }

  @Override
  public void closeRow() throws Exception {
  }
}
