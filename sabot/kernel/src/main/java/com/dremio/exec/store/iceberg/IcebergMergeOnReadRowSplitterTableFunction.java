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

import com.dremio.exec.physical.config.MergeOnReadRowSplitterTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.physical.DmlPlanGeneratorBase;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Splits the content of all incoming rows into 2 separate rows - one for the datafile-content and
 * one for the positional-deletefile-content. The schema remains the same for each, however the
 * content within each row contains nulls for the columns that are not relevant/important to that
 * row.
 *
 * <p>This Row Splitter addresses any case where the user is performing Merge-On-Read DML on an
 * iceberg table that meets all the following conditions:
 *
 * <ul>
 *   <li>The Table is Partitioned
 *   <li>The DML operation is an UPDATE, or a MERGE-update_only, or a MERGE-update_insert
 *   <li>A column that will undergo a data-modification is partition column
 * </ul>
 *
 * Conceptually, we'll hyper-focus on the dml'd partition column(s) to understand why this case
 * requires special treatment. Assuming the following conditions above are met, we need to do two
 * things:
 *
 * <ol>
 *   <li>Generate a delete-file for the old row, writing the <b>old partition value</b>
 *   <li>Generate a data-file, which is written to the <b>updated (new) partition value</b>
 * </ol>
 *
 * Without a splitter, performing steps 1 and 2 is not possible. Sending the data through the
 * planner on a single row means the old value and new value are being tied to the same partition
 * during output. Thus, we must distinguish the old partition from the new partition by splitting
 * the system columns (coupled by the old partition values) to their own row, so they can be
 * properly sorted by their respective partition the writer-phase is reached
 *
 * <p>In essence, this TableFunction serves as an alternative to the Column Projection mechanism in
 * the {@link DmlPlanGeneratorBase#getPlan()}
 */
public class IcebergMergeOnReadRowSplitterTableFunction extends AbstractTableFunction {

  public enum BufferState {
    CAN_CONSUME,
    CAN_PRODUCE,
    LAST_CAN_PRODUCE,
    DONE
  }

  private final List<TransferPair> insertTransfers = new ArrayList<>();
  private final List<TransferPair> updateDataFileTransfers = new ArrayList<>();
  private final List<TransferPair> updateDeleteFileTransfers = new ArrayList<>();

  /** handles the edge case where the row split will exceed the maxOutput */
  private final List<TransferPair> bufferTransfers = new ArrayList<>();

  private VectorContainer buffer;
  private final OperatorContext context;
  private final TableFunctionConfig functionConfig;
  private VarCharVector filePathVector;
  private final Map<String, Integer> updateColumnsWithIndex;
  private final Set<String> outdatedTargetColumnNames;
  private final int insertColumnCount;
  private int inputIndex;
  private final List<String> partitionColumnNames;
  private boolean doneWithRow = false;
  private BufferState bufferState = BufferState.CAN_CONSUME;

  public IcebergMergeOnReadRowSplitterTableFunction(
      OperatorContext context, TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    this.context = context;
    this.functionConfig = functionConfig;

    MergeOnReadRowSplitterTableFunctionContext functionContext =
        (MergeOnReadRowSplitterTableFunctionContext) (functionConfig.getFunctionContext());

    List<String> getPartitionColumns = functionConfig.getFunctionContext().getPartitionColumns();
    partitionColumnNames = getPartitionColumns == null ? List.of() : getPartitionColumns;
    updateColumnsWithIndex = functionContext.getUpdateColumnsWithIndex();
    outdatedTargetColumnNames = functionContext.getOutdatedTargetColumnNames();
    insertColumnCount = functionContext.getInsertColumnCount();
  }

  /**
   * There are 2 types of rows which we could expect to see:
   *
   * <ol>
   *   <li>Insert Rows: Does not contain System Columns (i.e. delete columns). No spitting needed
   *   <li>Update Rows: Will contain user-facing Data-columns and System Columns (i.e. delete
   *       columns). Splitting occurs here
   * </ol>
   *
   * We will translate these cases into 3 separate transfer pairs:
   *
   * <ol>
   *   <li><b>Transfer Pair 1:</b> Delete Rows Containing Delete Columns
   *   <li><b>Transfer Pair 2:</b> Data Rows Containing Update Columns
   *   <li><b>Transfer Pair 3:</b> Data Rows Containing Insert Columns
   * </ol>
   *
   * @param accessible VectorAccessible with input schema
   * @return the output vector with the official user-facing output schema + system columns
   * @throws Exception
   */
  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    this.incoming = accessible;
    this.outgoing = context.createOutputVectorContainer();
    outgoing.addSchema(functionConfig.getTableSchema());
    outgoing.buildSchema();

    // build the buffer's schema now in case we'll need it
    buffer = new VectorContainer(context.getAllocator());
    buffer.addSchema(outgoing.getSchema());
    buffer.buildSchema();

    int incomingColumnCount = incoming.getSchema().getFieldCount();
    int targetColumnCount =
        incomingColumnCount
            - insertColumnCount
            - updateColumnsWithIndex.size()
            + outdatedTargetColumnNames.size();

    int targetColIdx = 0;
    int insertColIdx = incomingColumnCount - updateColumnsWithIndex.size() - insertColumnCount;

    for (Field field : outgoing.getSchema()) {

      // Handle System columns For ALL 3 Transfer Pairs.
      // If field is a system column, project it for delete row. For data row, keep null (continue)
      if (Objects.equals(field.getName(), ColumnUtils.FILE_PATH_COLUMN_NAME)
          || Objects.equals(field.getName(), ColumnUtils.ROW_INDEX_COLUMN_NAME)) {
        updateDeleteFileTransfers.add(
            getVectorFromSchemaPath(incoming, field.getName())
                .makeTransferPair(getVectorFromSchemaPath(outgoing, field.getName())));

        // set-up to send content to a class-level field 'buffer'
        // to ensure data is safely cached upon new 'incoming'
        bufferTransfers.add(
            getVectorFromSchemaPath(incoming, field.getName())
                .makeTransferPair(getVectorFromSchemaPath(buffer, field.getName())));

        continue;
      }

      // TP 1 - For transfer pair: Delete rows
      if (partitionColumnNames.contains(
          field.getName())) { // transfer old partition column values. all other fields are null.
        // recall, Sys cols are checked for at beginning of loop
        updateDeleteFileTransfers.add(
            getVectorFromSchemaPath(
                    incoming, incoming.getSchema().getColumn(targetColIdx).getName())
                .makeTransferPair(getVectorFromSchemaPath(outgoing, field.getName())));

        // set-up to send content to a class-level field 'buffer'
        // to ensure data is safely cached upon new 'incoming'
        bufferTransfers.add(
            getVectorFromSchemaPath(
                    incoming, incoming.getSchema().getColumn(targetColIdx).getName())
                .makeTransferPair(getVectorFromSchemaPath(buffer, field.getName())));
      }

      // TP 2 - For transfer pair: Update-Data rows
      if (updateColumnsWithIndex.get(field.getName())
          != null) { // if field found in update columns, project it
        updateDataFileTransfers.add(
            getVectorFromSchemaPath(
                    incoming,
                    incoming
                        .getSchema()
                        .getFields()
                        .get(
                            targetColumnCount
                                - outdatedTargetColumnNames.size()
                                + insertColumnCount
                                + updateColumnsWithIndex.get(field.getName()))
                        .getName())
                .makeTransferPair(getVectorFromSchemaPath(outgoing, field.getName())));
        if (!outdatedTargetColumnNames.contains(field.getName())) {
          // if field is not considered an 'outdated' column, increment targetColIdx.
          targetColIdx++;
        }
      } else { // otherwise, project the target column version
        updateDataFileTransfers.add(
            getVectorFromSchemaPath(
                    incoming, incoming.getSchema().getColumn(targetColIdx).getName())
                .makeTransferPair(getVectorFromSchemaPath(outgoing, field.getName())));
        targetColIdx++;
      }

      // TP 3 - For transfer pair: Insert-Data rows
      if (insertColumnCount > 0) {
        insertTransfers.add( // add all insert columns. index starts at 'insertColIdx'
            getVectorFromSchemaPath(
                    incoming, incoming.getSchema().getColumn(insertColIdx).getName())
                .makeTransferPair(getVectorFromSchemaPath(outgoing, field.getName())));
        insertColIdx++;
      }
    }

    // Since we will transfer all data immediately, we'll get data from the outgoing vectors.
    filePathVector =
        (VarCharVector)
            VectorUtil.getVectorFromSchemaPath(incoming, ColumnUtils.FILE_PATH_COLUMN_NAME);

    return outgoing;
  }

  /** declare start of batch */
  @Override
  public void startBatch(int records) {
    outgoing.allocateNew();
  }

  /** declare start of row */
  @Override
  public void startRow(int row) throws Exception {
    inputIndex = row;
    doneWithRow = false;
  }

  /**
   * Process the row by splitting it into two, if necessary.
   *
   * <p>if the current row is an UPDATE row, then we must split. Set row 1 as the data-row with new
   * partition values. Set row 2 as the delete row with old partition values.
   *
   * <p>Each row will use its respective transfer pair.
   *
   * @param startOutIndex start index in output vector
   * @param maxRecords maximum number of output records that can be produced
   * @return the number of rows generated. Will be 1 or 2 depending on if we split a row
   * @throws Exception
   */
  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    if (doneWithRow) {
      return 0;
    }

    int increment = 0;

    // Occurs when: A row was not fully processed in the last iteration due to a split row not
    // fitting in the targetBatchSize. When we reach this, we must offload buffer immediately.
    if (bufferState != BufferState.CAN_CONSUME) {

      // buffer has been written and noMoreToConsume was hit. Dead-end state.
      if (bufferState == BufferState.DONE) {
        return 0;
      }

      // safe-copy of final row generated from split that couldn't fit in the previous 'outgoing'.
      List<TransferPair> bufferToOutgoing = new ArrayList<>();
      for (Field field : outgoing.getSchema()) {
        bufferToOutgoing.add(
            getVectorFromSchemaPath(buffer, field.getName())
                .makeTransferPair(getVectorFromSchemaPath(outgoing, field.getName())));
      }

      // input index from source-VectorContainer will always be 0
      // because buffer contains at-max 1 record. Row can only be split into 2.
      bufferToOutgoing.forEach(transfer -> transfer.copyValueSafe(0, startOutIndex));
      outgoing.setAllCount(startOutIndex + 1);

      // designates the content in buffer is the final. noMoreToConsume was hit.
      if (bufferState == BufferState.LAST_CAN_PRODUCE) {
        bufferState = BufferState.DONE;
        doneWithRow = true;
        return 1;
      }

      increment++;

      bufferState = BufferState.CAN_CONSUME;
    }

    // We need to use the adjustedStartOutIndex to account for the edge case
    // where we added a pending row
    int adjustedStartOutIndex = (increment == 0) ? startOutIndex : startOutIndex + 1;

    if (filePathVector.get(inputIndex) == null) {
      // INSERT ONLY Data (i.e Data Only)
      insertTransfers.forEach(
          transfer -> transfer.copyValueSafe(inputIndex, adjustedStartOutIndex));
      outgoing.setAllCount(startOutIndex + 1);
      increment++;
    } else {

      // The start of the 'pending row' edge case
      if (increment + 2 > maxRecords) {

        // only transfer one of the two rows into output because we dont have room for both
        updateDataFileTransfers.forEach(
            transfer -> transfer.copyValueSafe(inputIndex, adjustedStartOutIndex));
        outgoing.setAllCount(adjustedStartOutIndex + 1);

        buffer.allocateNew();
        bufferTransfers.forEach(transfer -> transfer.copyValueSafe(inputIndex, 0));
        buffer.setAllCount(1);

        // LOGIC TO STORE PENDING SPLIT ROW IN BUFFER HERE
        // since the row is contained in updateDeleteFileTransfers, we will copy it to the buffer
        // during the next iteration of outputData. Since we know the inputIndex, we will simply
        // do inputIndex - 1 to re-sync back to the pending row we wish to retrieve
        bufferState = BufferState.CAN_PRODUCE;

        increment++;
      } else {
        // UPDATE data (i.e Data + Delete Cols)
        updateDataFileTransfers.forEach(
            transfer -> transfer.copyValueSafe(inputIndex, adjustedStartOutIndex));
        updateDeleteFileTransfers.forEach(
            transfer -> transfer.copyValueSafe(inputIndex, adjustedStartOutIndex + 1));
        outgoing.setAllCount(adjustedStartOutIndex + 2);
        increment += 2;
      }
    }
    doneWithRow = true;
    return increment;
  }

  @Override
  public void closeRow() throws Exception {
    // no-op
  }

  /** Current data inside the buffer (if exists) will be the last. */
  @Override
  public void noMoreToConsume() {
    if (bufferState == BufferState.CAN_PRODUCE) {
      bufferState = BufferState.LAST_CAN_PRODUCE;
    }
  }

  /** notifies the writer if we have a buffered row to produce before finishing */
  @Override
  public boolean hasBufferedRemaining() {
    return bufferState == BufferState.LAST_CAN_PRODUCE;
  }

  @Override
  public void close() throws Exception {
    buffer.clear();
    super.close();
  }
}
