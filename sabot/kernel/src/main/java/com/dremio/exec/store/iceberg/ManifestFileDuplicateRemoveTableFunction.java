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

import static com.dremio.exec.store.SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import com.dremio.common.expression.BasePath;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.BasicTypeHelper;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A table function that'll detect duplicate manifest file paths for the Manifest List TF (using the
 * file path and the file type). This TF is used in VACUUM plan, e.g., EXPIRE SNAPSHOTS.
 */
public class ManifestFileDuplicateRemoveTableFunction extends AbstractTableFunction {

  private int inputIndex;
  private byte[] currentFilePath;

  private List<TransferPair> transfers = new ArrayList<>();
  private List<TransferPair> transfersWithoutFilePathType = new ArrayList<>();

  private VarCharVector inputFilePath;
  private VarCharVector inputFileType;

  private ArrowBuf previousFilePathBuf;
  private long previousFilePathBufLength = 0;
  private IcebergFileType previousFileType = null;

  private boolean doneWithRow;

  public ManifestFileDuplicateRemoveTableFunction(
      OperatorContext context, TableFunctionConfig functionConfig) {
    super(context, functionConfig);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);

    previousFilePathBuf = context.getManagedBuffer();

    for (Field field : incoming.getSchema()) {
      transfers.add(
          getVectorFromSchemaPath(incoming, field.getName())
              .makeTransferPair(getVectorFromSchemaPath(outgoing, field.getName())));
      if (CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA.getFieldId(BasePath.getSimple(field.getName()))
          == null) {
        transfersWithoutFilePathType.add(
            getVectorFromSchemaPath(incoming, field.getName())
                .makeTransferPair(getVectorFromSchemaPath(outgoing, field.getName())));
      }
    }

    // Since we will transfer all data immediately, we'll get data from the outgoing vectors.
    inputFilePath = (VarCharVector) getVectorFromSchemaPath(incoming, SystemSchemas.FILE_PATH);
    inputFileType = (VarCharVector) getVectorFromSchemaPath(incoming, SystemSchemas.FILE_TYPE);
    return outgoing;
  }

  @Override
  public void startBatch(int records) {
    outgoing.allocateNew();
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
    } else {
      // All input rows are sorted by file path and file type columns.
      // We check these two columns' values to skip duplicate manifest file paths.
      currentFilePath = inputFilePath.get(inputIndex);
      NullableVarCharHolder currentFilePathVectorHolder =
          (NullableVarCharHolder) BasicTypeHelper.getValue(inputFilePath, inputIndex);

      IcebergFileType currentFileType = null;
      if (!inputFileType.isNull(inputIndex)) {
        currentFileType =
            IcebergFileType.valueOf(
                new String(inputFileType.get(inputIndex), StandardCharsets.UTF_8));
      }

      int currentFilePathBufLength =
          currentFilePathVectorHolder.end - currentFilePathVectorHolder.start;

      // Current file is a manifest, and it is the same manifest file as previous one.
      // So, it is the duplicate manifest and doesn't to include this manifest file.
      boolean skipRow = false;
      if (currentFileType != null
          && currentFileType.equals(IcebergFileType.MANIFEST)
          && previousFileType != null
          && previousFileType.equals(currentFileType)
          && previousFilePathBufLength > 0
          && currentFilePathVectorHolder.isSet == 1
          && previousFilePathBufLength == currentFilePathBufLength
          && ByteFunctionHelpers.compare(
                  currentFilePathVectorHolder.buffer,
                  currentFilePathVectorHolder.start,
                  currentFilePathVectorHolder.end,
                  previousFilePathBuf,
                  0,
                  previousFilePathBufLength)
              == 0) {
        // skip it
        skipRow = true;
      } else {
        // We need to skip to add the filePath and fileType info back to the output columns for
        // MANIFEST file paths. Otherwise, InputCarryForwardTableFunctionDecorator.startRow will
        // mark the manifest file path row as carry-forward row, and will SKIP to process this
        // manifest files and miss to add data files, etc. into the plan.
        if (currentFileType.equals(IcebergFileType.MANIFEST)) {
          transfersWithoutFilePathType.forEach(
              transfer -> transfer.copyValueSafe(inputIndex, startOutIndex));
        } else {
          transfers.forEach(transfer -> transfer.copyValueSafe(inputIndex, startOutIndex));
        }
        outgoing.setAllCount(startOutIndex + 1);
      }

      if (currentFilePathVectorHolder.isSet == 1) {
        previousFilePathBufLength = currentFilePathBufLength;
        previousFilePathBuf = previousFilePathBuf.reallocIfNeeded(currentFilePathBufLength);
        previousFilePathBuf.setBytes(
            0,
            currentFilePathVectorHolder.buffer,
            currentFilePathVectorHolder.start,
            previousFilePathBufLength);
        previousFileType = currentFileType;
      } else {
        previousFilePathBufLength = 0;
      }

      doneWithRow = true;
      return skipRow ? 0 : 1;
    }
  }

  @Override
  public void closeRow() throws Exception {}
}
