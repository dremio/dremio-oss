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
package com.dremio.exec.store.parquet;

import static com.dremio.common.map.CaseInsensitiveImmutableBiMap.newImmutableMap;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.common.map.CaseInsensitiveImmutableBiMap;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.impl.ByteArrayWrapper;
import com.dremio.exec.physical.base.TableFormatWriterOptions.TableFormatOperation;
import com.dremio.exec.planner.physical.visitor.WriterUpdater;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.store.EventBasedRecordWriter;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.SVFilteredEventBasedRecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.filter.VectorContainerWithSV;
import com.dremio.sabot.op.sort.external.VectorSorter;
import com.dremio.sabot.op.spi.SingleInputOperator.State;
import com.dremio.sabot.op.writer.WriterOperator;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.iceberg.Schema;

/**
 * Record Writer for Merge On Read DML operations.
 *
 * <p>This writer serves as a container for two unique ParquetRecordWriters:
 *
 * <ul>
 *   <li><b>dataFileRecordWriter:</b> Responsible for writing data files.
 *   <li><b>deleteFileRecordWriter:</b> Responsible for writing positional delete files.
 * </ul>
 *
 * <p>It is essential to note that each positional delete file must be written with entries sorted
 * by 'file_path', 'pos' in ascending (ASC) order. For partitioned-tables, this is handled during
 * planning - We added the system columns to the Sort Operator during partition-based sort. For
 * un-partitioned-tables or partitioned-and-sortOrder-defined-tables, delete files are written in a
 * two-step process:
 *
 * <ol>
 *   <li>First, the delete records are stored in memory using a {@link VectorSorter}. The sorter
 *       will continue to consume incoming batches until notified to stop. Once notified to stop,
 *       the sorter will sort the consumed in-memory, then notify the operator that it has a pending
 *       output awaiting to write, then finally be passed to a {@link WritableVectorSorter} queue.
 *       <p><b>The sorter is notified its done consuming (i.e. CAN_PRODUCE) under any of the
 *       following conditions </b>:
 *       <ul>
 *         <li>The sorter's memory is full
 *         <li>The sorter's memory has hit its row-count maximum
 *         <li>There is no more data to consume from the Operator
 *       </ul>
 *   <li>Second, the sorted delete records are written to the delete file. This occurs whenever the
 *       operator has moved to the CAN_PRODUCE state.
 * </ol>
 */
public class MergeOnReadRecordWriter implements RecordWriter {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MergeOnReadRecordWriter.class);
  private static final List<Ordering> ORDERINGS = buildDeleteFileOrdering();
  public static final int DELETE_FILE_BLOCK_SIZE_TO_ROW_COUNT_RATIO = 4;
  private static final String DELETE_FILE_SUFFIX = "_delete";
  private ParquetRecordWriter dataFileRecordWriter;
  private ParquetRecordWriter deleteFileRecordWriter;
  private VectorContainer dataFileVectorContainer;
  private VectorContainer deleteFileVectorContainer;
  private VectorSorter vectorSorter;
  private final Queue<WritableVectorSorter> writableVectorSorters = new LinkedList<>();
  private VarCharVector filePathVector;
  private BigIntVector posVector;
  private VectorContainerWithSV filteredContainerForRowSplitDataFile;
  private final boolean isRowSplitterTableFunctionEnabled;
  private WritePartition previousPartition = null;
  private WritePartition defaultPartition;
  private WritableVectorSorter currWritableSorter = null;
  private final Set<ByteArrayWrapper> referencedDataFiles = new HashSet<>();
  private WriterOperator.Listener listener;

  // Iceberg column ID map for positional delete file writer
  private static final CaseInsensitiveImmutableBiMap<Integer>
      icebergPositionalDeleteFileColumnIDMap =
          newImmutableMap(IcebergUtils.getColIDMapWithReservedDeleteFields(new Schema()));

  // ToDo: we could make this static since the delete-file schema is static

  // class-level filtered container to assess a partition-based batch.
  private VectorContainerWithSV filteredContainerForPartitionedTables;
  // For writing partition-based positional-delete rows.
  protected EventBasedRecordWriter positionalDeleteFileEventBasedRecordWriter;

  // used to write partition-based data-rows affected by row-splitter TableFunction output
  protected EventBasedRecordWriter dataFileEventBasedRecordWriter;
  private final boolean isPartitioned;

  /**
   * If possible, we avoid use of the VectorSorter by addressing the sorting of positional-deletes
   * during {@link WriterUpdater}. It is better for performance and memory usage to avoid sorting
   * during the writer.
   *
   * <ol>
   *   <b>Below are the following cases where we can <i>avoid</i> using VectorSorter:</b>
   *   <li>The table is partitioned with no defined iceberg-sort-order.
   *   <li>The table is partitioned and are only writing delete-files.
   *   <li>Simple Case: INSERT-ONLY Merge DML. No Delete files are written thus never a need for
   *       sort.
   * </ol>
   *
   * If none of the above conditions apply, a VectorSorter is <b>required</b> to write
   * positional-delete files.
   */
  private boolean isVectorSorterRequired;

  /**
   * Defines the conditions under which specific file operations occur. The operations are
   * categorized as follows:
   *
   * <ul>
   *   <li><b>DELETE_FILE_ONLY</b> - This condition is met when there is a need to only delete
   *       files. This includes operations like DELETE, and MERGE with delete-only operations.
   *   <li><b>DATA_FILE_ONLY</b> - This condition occurs when the operation involves only writing
   *       data files. This is typical for MERGE operations that are insert-only.
   *   <li><b>DATA_AND_DELETE</b> - This condition applies when there is a need to write both data
   *       and delete files. This covers cases such as UPDATE, MERGE with update-only, and MERGE
   *       operations that involve both insertions and deletions.
   * </ul>
   */
  public enum WriterMode {
    DELETE_FILES_ONLY,
    DATA_FILES_ONLY,
    DATA_AND_DELETE_FILES,
    INVALID
  }

  /**
   * Defines the state of the MergeOnReadDeleteWriter.
   *
   * <p>The MergeOnReadDeleteWriter can be in one of the following states:
   *
   * <ul>
   *   <li><b>{@link MergeOnReadDeleteWriterState#NOT_SET_UP}</b> - The writer not initialized and
   *       signifies a no-op if kept in this over state.
   *   <li><b>{@link MergeOnReadDeleteWriterState#CAN_CONSUME}</b> - The writer is ready to consume
   *       incoming data. Positional-Delete content from incoming batch is expected to be consumed
   *       by the VectorSorter.
   *   <li><b>{@link MergeOnReadDeleteWriterState#CAN_PRODUCE}</b> - The writer is ready to produce
   *       output. WritableVectorSorters are likely present, meaning sorted positional-delete data
   *       is ready to produce.
   *   <li><b>{@link MergeOnReadDeleteWriterState#DONE}</b> - All outstanding WritableVectorSorters
   *       have been offloaded and written to parquet-files. The WriterOperator is is ready to move
   *       on from it's CAN_PRODUCE state.
   *   <li><b>{@link MergeOnReadDeleteWriterState#ERROR}</b> - The writer has encountered an error.
   *       This is used as a safety-pin to ensure proper resource de-allocation when closing
   *   <li><b>{@link MergeOnReadDeleteWriterState#NOT_NEEDED}</b> - The writer is not needed. This
   *       state is used for partitioned tables where the VectorSorter is unnecessary.
   * </ul>
   */
  public enum MergeOnReadDeleteWriterState {
    NOT_SET_UP,
    CAN_CONSUME,
    CAN_PRODUCE,
    DONE,
    ERROR,
    NOT_NEEDED
  }

  /**
   * Defines the mode of the write batch operation. The write batch operation can be in one of the
   * following modes:
   *
   * <ul>
   *   <li><b>{@link WriteBatchMode#DATA_FILE}</b> - The write batch operation is for writing data
   *       files.
   *   <li><b>{@link WriteBatchMode#DELETE_FILE}</b> - The write batch operation is for writing
   *       positional delete files.
   *   <li><b>{@link WriteBatchMode#NOT_SPECIFIED}</b> - The write batch operation mode has not been
   *       specified. This should never happen
   * </ul>
   */
  public enum WriteBatchMode {
    DATA_FILE,
    DELETE_FILE,
    NOT_SPECIFIED
  }

  private WriterMode writerMode = WriterMode.INVALID;
  private MergeOnReadDeleteWriterState mergeOnReadDeleteWriterState =
      MergeOnReadDeleteWriterState.NOT_SET_UP;
  private WriteBatchMode writeBatchMode = WriteBatchMode.NOT_SPECIFIED;
  private final OperatorContext context;
  private final ParquetWriter writer;
  private final ParquetFormatConfig config;

  /**
   * Constructor for MergeOnReadRecordWriter.
   *
   * <p>It initializes the operator context, parquet writer and parquet format config. It also
   * checks if the row-splitter table function is enabled.
   *
   * @param operatorContext The operator context.
   * @param writer The parquet writer.
   * @param config The parquet format config.
   * @throws OutOfMemoryException If there is not enough memory to initialize the writer.
   */
  public MergeOnReadRecordWriter(
      OperatorContext operatorContext, ParquetWriter writer, ParquetFormatConfig config)
      throws OutOfMemoryException {
    this.context = operatorContext;
    this.writer = writer;
    this.isRowSplitterTableFunctionEnabled = writer.getOptions().isMergeOnReadRowSplitterMode();
    this.isPartitioned = writer.getOptions().hasPartitions();
    this.config = config;
  }

  /**
   * Builds the delete file ordering.
   *
   * <p><b>Note:</b> The list ordering for delete records is static and is always sorted by
   * 'file_path', 'pos'.
   *
   * @return A list of orderings for sorting delete records.
   */
  private static List<Ordering> buildDeleteFileOrdering() {
    FieldReference filePathFieldRef = new FieldReference(ColumnUtils.FILE_PATH_COLUMN_NAME);
    FieldReference rowIndexFieldRef = new FieldReference(ColumnUtils.ROW_INDEX_COLUMN_NAME);
    return List.of(
        new Ordering(Direction.ASCENDING, filePathFieldRef),
        new Ordering(Direction.ASCENDING, rowIndexFieldRef));
  }

  /**
   * Validates the state of the delete writer.
   *
   * <p>Checks if the current state of the delete writer is one of the expected states.
   *
   * @param expectedState The expected states of the delete writer.
   * @throws IllegalStateException If the current state of the delete writer is not one of the
   *     expected states.
   */
  private void validateState(MergeOnReadDeleteWriterState... expectedState) {
    boolean isValidated = false;

    for (MergeOnReadDeleteWriterState state : expectedState) {
      if (mergeOnReadDeleteWriterState == state) {
        isValidated = true;
        break;
      }
    }
    if (!isValidated) {
      throw new IllegalStateException(
          String.format(
              "Merge On Read DML Writer reached an invalid state. Accepted States: %s, Actual: %s",
              Arrays.toString(expectedState), mergeOnReadDeleteWriterState));
    }
  }

  /**
   * Sets up the record writers.
   *
   * <p>Evaluates the writer mode based on the incoming schema and command type. It then sets up the
   * appropriate data and/or delete file record writers.
   *
   * @param incoming The incoming vector accessible.
   * @param listener The output entry listener.
   * @param statsListener The write stats listener.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public void setup(
      VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener)
      throws IOException {
    try {
      logger.debug("Setting up Merge-On-Read Writer");
      evaluateWriterMode(
          incoming.getSchema(), writer.getOptions().getTableFormatOptions().getOperation());
      this.listener = (WriterOperator.Listener) listener;

      // Only 1 of the following must be true:
      // 1) Vector-Sorter-On-Partitioned-Tables-With-Undefined-Sorter is ON
      // 2) Is Not Partitioned
      // 3) Is Partitioned AND Iceberg Sort Order is Defined
      // ('isPartitioned' is implied in 3rd conditional)
      this.isVectorSorterRequired =
          (context
                  .getOptions()
                  .getOption(
                      ExecConstants
                          .FORCE_USE_MOR_VECTOR_SORTER_FOR_PARTITION_TABLES_WITH_UNDEFINED_SORT_ORDER))
              || (!isPartitioned
                  || (writerMode == WriterMode.DATA_AND_DELETE_FILES
                      && !writer.getOptions().getSortColumns().isEmpty()));

      if (isVectorSorterRequired) {
        logger.debug(
            "Vector Sorter is required. "
                + "Positional Delete-Files will use Vector Sorters to sort content prior to writing.");
      } else {
        logger.debug(
            "Vector Sorter not required. "
                + "Positional Delete content already adheres to the iceberg sort requirements.");
      }

      if (isRowSplitterTableFunctionEnabled) {
        logger.debug("Merge-On-Read Row-Splitter enabled");
      }

      switch (writerMode) {
        case DELETE_FILES_ONLY:
          setupDeleteFileWriter(incoming, listener, statsListener);
          break;
        case DATA_AND_DELETE_FILES:
          setupDataFileWriter(incoming, listener, statsListener);
          setupDeleteFileWriter(incoming, listener, statsListener);
          break;
        case DATA_FILES_ONLY:
          setupDataFileWriter(incoming, listener, statsListener);
          break;

        default:
          throw new UnexpectedException("Merge On Read DML Writer reached an invalid state.");
      }
    } catch (Exception e) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.ERROR;
      cleanupResources();
      throw new IOException("Unable to set up Merge-On-Read Record Writer", e);
    }
  }

  /**
   * Declares the expected content of the incoming batch. see {@link WriterMode}
   *
   * @param schema the full DML Physical plan schema provided by the {@link
   *     com.dremio.exec.planner.physical.DmlPositionalMergeOnReadPlanGenerator}.
   */
  private void evaluateWriterMode(BatchSchema schema, TableFormatOperation commandType) {

    // delete operations will only propagate the delete-writer. Schema: System Columns
    if (commandType == TableFormatOperation.DELETE) {
      writerMode = WriterMode.DELETE_FILES_ONLY;

      // insert-only operations will only propagate the data-writer.
      // Schema will never contain sys-cols. It will only contain user-facing data columns.
    } else if (!schema
        .getColumn(schema.getFieldCount() - 1)
        .getName()
        .equals(ColumnUtils.ROW_INDEX_COLUMN_NAME)) {
      writerMode = WriterMode.DATA_FILES_ONLY;

      // update || merge_update || merge_update-insert operations will propogate both writers.
      // incoming schema will contain both user-facing data (target/source)
      // columns and system-columns
    } else {
      writerMode = WriterMode.DATA_AND_DELETE_FILES;
    }
    logger.debug(
        String.format("Merge-On-Read DML Writer Mode configured to: '%s'", writerMode.name()));
  }

  /**
   * Builds the dataFile Record Writer.
   *
   * @param incoming The incoming vector accessible from the operator.
   * @param listener The output entry listener. Used to notify the operator if ready for output.
   * @param statsListener The write stats listener.
   * @throws IOException
   */
  private void setupDataFileWriter(
      VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener)
      throws IOException {
    buildDataFileVectorContainer(incoming);
    dataFileRecordWriter = new ParquetRecordWriter(context, writer, config);
    dataFileRecordWriter.setup(dataFileVectorContainer, listener, statsListener);
  }

  /**
   * Builds the deleteFile Record Writer and the vectorSorter used to properly write the delete
   * col's content by 'file_path', 'pos'.
   *
   * @param incoming The incoming vector accessible from the operator. Schema always contains the
   *     sys-cols 'file_path', 'pos'.
   * @param listener The output entry listener. Used to notify the operator when positional deletes
   *     held within the sorter are awaiting output.
   * @param statsListener The write stats listener.
   * @throws IOException
   */
  private void setupDeleteFileWriter(
      VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener)
      throws IOException {
    buildDeleteFileVectorContainer(incoming);
    deleteFileRecordWriter =
        new ParquetRecordWriter(context, writer, config)
            .withStaticIcebergColumnIDMap(icebergPositionalDeleteFileColumnIDMap)
            .withBatchSchema(ColumnUtils.DELETE_FILE_SCHEMA)
            .withParquetFileNameSuffix(DELETE_FILE_SUFFIX)
            .withBlockSize(
                context
                    .getOptions()
                    .getOption(ExecConstants.PARQUET_DELETE_FILE_BLOCK_SIZE_VALIDATOR),
                !isVectorSorterRequired) // Vector Sorter already handles block sizing.
            // Disable when VectorSorters are in use.
            .withOperationType(OperationType.ADD_DELETEFILE);
    deleteFileRecordWriter.setup(deleteFileVectorContainer, listener, statsListener);
    // handles VectorSorter write states. Only required when VectorSorter is necessary.
    mergeOnReadDeleteWriterState =
        isVectorSorterRequired
            ? MergeOnReadDeleteWriterState.NOT_SET_UP
            : MergeOnReadDeleteWriterState.NOT_NEEDED;
  }

  /**
   * starts a new partition for each respective record writer.
   *
   * <p>If required, Generates a new vector sorter if a new partition is encountered.
   *
   * @param partition The write partition to start.
   * @throws Exception If an error occurs.
   */
  @Override
  public void startPartition(WritePartition partition) throws Exception {
    try {
      switch (writerMode) {
        case DATA_FILES_ONLY:
          setWriteBatchMode(WriteBatchMode.DATA_FILE);
          startPartitionImpl(partition);
          break;

        case DELETE_FILES_ONLY:
          setWriteBatchMode(WriteBatchMode.DELETE_FILE);
          startPartitionImpl(partition);
          break;

        case DATA_AND_DELETE_FILES:
          setWriteBatchMode(WriteBatchMode.DATA_FILE);
          startPartitionImpl(partition);

          setWriteBatchMode(WriteBatchMode.DELETE_FILE);
          startPartitionImpl(partition);
          break;
        default:
          throw new UnexpectedException("Merge On Read DML Writer reached an invalid state.");
      }
      previousPartition = partition;
    } catch (Exception e) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.ERROR;
      cleanupResources();
      throw new IOException("Unable to start partition during Merge On Read DML.", e);
    }
  }

  /**
   * starts a new partition.
   *
   * <p>If starting a new position on a delete-file writer, track reference file list and vector
   * sorter state.
   *
   * @param partition The write partition for coming batches
   * @throws Exception
   */
  private void startPartitionImpl(WritePartition partition) throws Exception {
    switch (writeBatchMode) {
      case DATA_FILE:
        dataFileRecordWriter.startPartition(partition);
        break;
      case DELETE_FILE:
        if (isVectorSorterRequired) {
          generateNewVectorSorterIfNeeded(
              partition); // only non-partitioned tables use vectorSorter
        }
        this.deleteFileRecordWriter.startPartition(partition);
        break;
      default:
        throw new UnexpectedException("Merge On Read DML Writer reached an invalid state.");
    }
  }

  /**
   * Handles the operation of 'writeBatch' method from {@link
   * com.dremio.sabot.op.writer.WriterOperator#consumeData}. It determines the write batch mode and
   * then calls the appropriate implementation to write the batch.
   *
   * @param offset The offset of the batch to start writing from.
   * @param length The number of records to write.
   * @return The number of records written.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public int writeBatch(int offset, int length) throws IOException {
    try {
      switch (writerMode) {
        case DATA_FILES_ONLY:
          setWriteBatchMode(WriteBatchMode.DATA_FILE);
          return writeBatchImpl(offset, length);

        case DELETE_FILES_ONLY:
          setWriteBatchMode(WriteBatchMode.DELETE_FILE);
          return writeBatchImpl(offset, length);

        case DATA_AND_DELETE_FILES:

          // Data-Writer must go first because Sys Cols will be consumed by delete writer.
          // Sys Cols will be needed by data-writer during if row-splitter TableFunction required
          setWriteBatchMode(WriteBatchMode.DATA_FILE);
          int numDataRowsWritten = writeBatchImpl(offset, length);

          setWriteBatchMode(WriteBatchMode.DELETE_FILE);
          // will always return 0 on non-partitioned tables.
          // The records for delete files are written during the writer operator's CAN_PROCUCE state
          writeBatchImpl(offset, length);

          return numDataRowsWritten;
        default:
          throw new UnexpectedException("Merge On Read DML Writer reached an invalid state.");
      }
    } catch (Exception e) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.ERROR;
      cleanupResources();
      throw new IOException("Unable to write batch during Merge On Read DML.", e);
    } finally {
      resetWriteBatchMode();
    }
  }

  /**
   * Consumes data records and delete records.
   *
   * <p>This method processes two types of records:
   *
   * <ul>
   *   <li><b>Data Records:</b> These are processed according to the behavior defined in {@link
   *       ParquetRecordWriter}. Common row-split case is handled in-class.
   *   <li><b>Delete Records:</b> Unless the table is partitioned with no defined sort-order, These
   *       are temporarily stored in {@link VectorSorter}'s memory. The sorted records in memory are
   *       later offloaded and written during the execution of {@code handlePositionalDeletes}.
   * </ul>
   *
   * <p>Usage of this method ensures that both data and delete records are handled appropriately,
   * with delete records being sorted and stored for subsequent operations.
   *
   * @param offset The offset of the batch to start writing from.
   * @param length The number of records to write.
   */
  private int writeBatchImpl(int offset, int length) throws IOException {
    try {
      switch (writeBatchMode) {
        case DATA_FILE:
          if (isRowSplitterTableFunctionEnabled) { // handles row-split data
            return writeBatchContainingSplitRows(offset, length);
          }
          return dataFileRecordWriter.writeBatch(offset, length);
        case DELETE_FILE:
          if (isVectorSorterRequired) {
            addBatchIntoMemory(offset, length);
          } else {
            // only possible if table is partitioned and has no defined iceberg sort order
            writePartitionBasedBatch(offset, length);
          }
          return 0;
        default:
          throw new UnexpectedException("Merge On Read Write Batch Type Not Specified.");
      }
    } catch (Exception e) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.ERROR;
      cleanupResources();
      throw new IOException("Unable to write batch during Merge On Read DML.", e);
    }
  }

  /**
   * Write Positional-Delete data to the writer
   *
   * <p>Uses SelectionVector to exclude any data-rows within the batch. Data rows are identifiable
   * when the System columns are null. When one System column is null, both are. We use 'filePath'
   * for our monitoring.
   *
   * @param offset start of partition index
   * @param length number of records to parse
   * @throws IOException @Return void because record count is not required on partition-based writes
   *     for Positional-Deletes.
   */
  private void writePartitionBasedBatch(int offset, int length) throws IOException {
    validateState(MergeOnReadDeleteWriterState.NOT_NEEDED);
    // get the Sys Col FilePathVector to determine incoming record count
    ValueVector sysColFilePathVector =
        getVectorFromSchemaPath(deleteFileVectorContainer, ColumnUtils.FILE_PATH_COLUMN_NAME);
    deleteFileVectorContainer.setRecordCount(sysColFilePathVector.getValueCount());

    try (SelectionVector2 deleteFileSV2 =
        filteredContainerForPartitionedTables == null
            ? new SelectionVector2(context.getAllocator())
            : filteredContainerForPartitionedTables.getSelectionVector2()) {
      // filteredContainer is the incoming consumable container with Sv2 attached.
      // sv2 filters out nulls in the file-path column
      // because nulls in file-path column indicate data rows.
      // We only want positional-delete rows.
      filteredContainerForPartitionedTables =
          prepareIncomingSVContainerForPositionalDeletes(
              filteredContainerForPartitionedTables,
              offset,
              length,
              deleteFileSV2,
              deleteFileVectorContainer);

      // indicates that the selection-vector is empty or has filtered out all records.
      // Nothing to consume.
      if (filteredContainerForPartitionedTables.getSelectionVector2().getCount() < 1) {
        return;
      }

      sendReferencesToDeleteWriter(referencedDataFiles);

      if (this.positionalDeleteFileEventBasedRecordWriter == null) {
        this.positionalDeleteFileEventBasedRecordWriter =
            new SVFilteredEventBasedRecordWriter(
                filteredContainerForPartitionedTables, deleteFileRecordWriter);
      } else {
        ((SVFilteredEventBasedRecordWriter) positionalDeleteFileEventBasedRecordWriter)
            .setBatch(filteredContainerForPartitionedTables);
      }
      // Write the batch using the event-based record writer.
      positionalDeleteFileEventBasedRecordWriter.write(offset, length);
    } finally {
      // filter must be kept alive until the batch's lifecycle
      if (offset + length == deleteFileVectorContainer.getRecordCount()) {
        filteredContainerForPartitionedTables.close();
        filteredContainerForPartitionedTables = null;
      }
    }
  }

  /**
   * Send the set of distinct data file paths to the delete-parquet writer. The Writer also excludes
   * duplicates.
   */
  private void sendReferencesToDeleteWriter(Set<ByteArrayWrapper> referencedDataFiles) {
    deleteFileRecordWriter.appendValueVectorCollection(new HashSet<>(referencedDataFiles));
    referencedDataFiles.clear();
  }

  /**
   * Builds the user-facing data-columns from 'incoming' to write in accordance of DataFile Schema.
   * (i.e. filter out system columns).
   *
   * @param incoming The incoming vector accessible from the operator.
   */
  private void buildDataFileVectorContainer(VectorAccessible incoming) {
    dataFileVectorContainer = new VectorContainer();

    Predicate<String> condition = name -> !ColumnUtils.isDmlSystemColumn(name);

    Streams.stream(incoming)
        .filter(wrapper -> condition.test(wrapper.getField().getName()))
        .forEach(wrapper -> dataFileVectorContainer.add(wrapper.getValueVector()));

    dataFileVectorContainer.buildSchema();
  }

  /**
   * Gets the System columns from 'incoming' to write in accordance of DeleteFile Schema (i.e.
   * 'file_path', 'pos').
   *
   * @param incoming The incoming vector accessible from the operator.
   * @return The vector container for the positional delete-file writer.
   */
  private void buildDeleteFileVectorContainer(VectorAccessible incoming) {
    filePathVector =
        (VarCharVector) getVectorFromSchemaPath(incoming, ColumnUtils.FILE_PATH_COLUMN_NAME);
    posVector = (BigIntVector) getVectorFromSchemaPath(incoming, ColumnUtils.ROW_INDEX_COLUMN_NAME);

    deleteFileVectorContainer = new VectorContainer(context.getAllocator());
    deleteFileVectorContainer.add(filePathVector);
    deleteFileVectorContainer.add(posVector);
    deleteFileVectorContainer.buildSchema();
  }

  /**
   * Build a New Vector Sorter for each new partition.
   *
   * @param partition The partition to build the vector sorter for.
   * @throws Exception
   */
  private void generateNewVectorSorterIfNeeded(WritePartition partition) throws Exception {

    // First partition is encountered in batch,
    // ...or previous vectorSorter/partition was immediately consumed --> sealed
    if (vectorSorter == null) {
      logger.debug(
          String.format("starting new partition: %s", partition.getIcebergPartitionData()));
      vectorSorter = new VectorSorter(context, ORDERINGS);
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.CAN_CONSUME;
      return;
    }

    // must check if sorter has been set up because some partitions may not have any delete content.
    // This can happen when the current partition's content contains only data-content.
    if (partition != previousPartition && vectorSorter.getState() == State.CAN_CONSUME) {
      logger.debug(
          String.format("starting new partition: %s", partition.getIcebergPartitionData()));
      vectorSorter.noMoreToConsume();
      writableVectorSorters.add(
          new WritableVectorSorter(
              vectorSorter, previousPartition, new HashSet<>(referencedDataFiles)));
      referencedDataFiles.clear();
      vectorSorter = new VectorSorter(context, ORDERINGS);
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.CAN_CONSUME;
    }
  }

  /**
   * Adds a batch of records into a VectorSorter's memory, handling their preparation, validation,
   * and processing within the delete file writer's context.
   *
   * <p>This method serves as the parent of handling consumption of incoming delete-records. There
   * is a lot of logic in this method, so here is a breakdown of the steps:
   *
   * <ol>
   *   <li>Prepare the incoming batch for consumption. This involves multi-step row-based filtering
   *       to ensure All data entering a respective sorter is only consuming delete-content rows.
   *       i.e rows where System columns are non-null.
   *   <li>Inject the SV2 prepared-batch into the sorter
   *   <li>Handle the sorter's consumption of the SV2 prepared-batch
   *   <li>Close resources that are no longer needed after adding a batch
   * </ol>
   *
   * @throws RuntimeException If there is an error during the batch processing, indicating a failure
   *     in handling the batch correctly or an issue with resource management. The exception
   *     encapsulates the original cause to provide clear debugging insights.
   */
  private void addBatchIntoMemory(int offset, int length) throws RuntimeException {
    validateState(MergeOnReadDeleteWriterState.CAN_CONSUME);
    // get the Sys Col FilePathVector to determine incoming record count
    ValueVector sysColFilePathVector =
        getVectorFromSchemaPath(deleteFileVectorContainer, ColumnUtils.FILE_PATH_COLUMN_NAME);
    deleteFileVectorContainer.setRecordCount(sysColFilePathVector.getValueCount());

    boolean multiPartitionedBatchMode =
        !(offset == 0 && length == deleteFileVectorContainer.getRecordCount());

    VectorContainer consumableBatch = getConsumableBatch(offset, length, multiPartitionedBatchMode);

    try (SelectionVector2 deleteFileSV2 = new SelectionVector2(context.getAllocator())) {
      // filteredContainer is the incoming consumable container with Sv2 attached.
      // sv2 filters out nulls in the file-path column
      // because nulls in file-path column indicate data rows.
      // We only want positional-delete rows.
      try (VectorContainerWithSV filteredContainer =
          prepareIncomingSVContainerForPositionalDeletes(
              null, 0, length, deleteFileSV2, consumableBatch)) {

        // indicates that the selection-vector is empty or has filtered out all records.
        // Nothing to consume.
        if (filteredContainer.getSelectionVector2().getCount() < 1) {
          return;
        }

        // set sorter's 'incoming'
        injectIncomingSVContainerIntoSorter(filteredContainer);

        // finally, we consume the positional-delete content into the sorter
        vectorSorter.consumeData();
        checkAndHandleSorterLimit(filteredContainer);
      }
    } catch (Exception e) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.ERROR;
      consumableBatch.close();
      throw new RuntimeException("Unable to add batch into memory.", e);
    } finally {
      // can only close if deleteFileVectorContainer is not the consumableBatch.
      // deleteFileVectorContainer is continually pumping from the operator.
      if (multiPartitionedBatchMode) {
        consumableBatch.close();
      }
    }
  }

  /**
   * Gets the sub-batch of rows from deleteWriter's 'incoming'.
   *
   * <p>Partitions may generate sub-batches.
   *
   * @param offset the starting boundary of the partition
   * @param length the number of records from the batch that belong to partition
   * @param multiPartitionedBatchMode indicates if the incoming batch is partitioned
   * @return the sub-batch to be consumed by the VectorSorter
   */
  private VectorContainer getConsumableBatch(
      int offset, int length, boolean multiPartitionedBatchMode) {
    VectorContainer consumableBatch =
        multiPartitionedBatchMode
            ? buildPartitionBasedBatch(offset, length)
            : deleteFileVectorContainer;
    consumableBatch.setRecordCount(length);
    return consumableBatch;
  }

  /**
   * Builds a partition-based batch for consumption. All content entering the VectorSorter must be
   * of the same partition.
   *
   * @param offset the starting boundary of the partition
   * @param length the number of records from the batch that belong to partition
   * @return the partitioned-based consumable batch for the VectorSorter
   */
  private VectorContainer buildPartitionBasedBatch(int offset, int length) {
    VectorContainer partitionBatch = new VectorContainer(context.getAllocator());
    partitionBatch.addSchema(deleteFileVectorContainer.getSchema());
    partitionBatch.buildSchema();

    List<TransferPair> transfers =
        Streams.stream(deleteFileVectorContainer)
            .map(
                vw ->
                    vw.getValueVector()
                        .makeTransferPair(
                            getVectorFromSchemaPath(partitionBatch, vw.getField().getName())))
            .collect(Collectors.toList());

    // We must perform a vector-copy specific to the current partition or else the entire batch
    // will be transferred to the sorter, regardless of partition and/or rows included in the sv2.
    // This would mean lost content for the remaining partitions within the batch.
    // A vector copy solves this issue by safely keeping the original data for the lifecycle of
    // the consume-data operation.
    for (int i = 0; i < length; i++) {
      int finalI = i;
      transfers.forEach(transfer -> transfer.copyValueSafe(finalI + offset, finalI));
      partitionBatch.setAllCount(i + 1);
    }
    return partitionBatch;
  }

  /**
   * Prepares batch for consumption by the VectorSorter
   *
   * @param offset the starting index
   * @param length the number of records in the incoming batch
   * @param positionalDeleteSV2 the selection vector for the delete file
   * @param consumableBatch the incoming batch
   * @return a flag indicating if the incoming batch is empty after excluding nulls.
   */
  private VectorContainerWithSV prepareIncomingSVContainerForPositionalDeletes(
      VectorContainerWithSV presetFilteredContainer,
      int offset,
      int length,
      SelectionVector2 positionalDeleteSV2,
      VectorContainer consumableBatch) {

    // always-true predicate for VectorContainerWithSV filtering on delete Files.
    // The important filtering for delete-file content is done in the selectionVector.
    VectorContainerWithSV filteredContainer =
        prepareFilteredContainer(
            presetFilteredContainer, positionalDeleteSV2, consumableBatch, t -> true);

    // prepare the SV2 for the delete file writer.
    // Keep only rows where sys col 'file_path' is non-null.
    deleteFileRecordWriter.prepareSV2(
        (VarCharVector) getVectorFromSchemaPath(consumableBatch, ColumnUtils.FILE_PATH_COLUMN_NAME),
        positionalDeleteSV2,
        offset,
        length,
        Objects::nonNull,
        referencedDataFiles::add);

    filteredContainer.setRecordCount(positionalDeleteSV2.getCount());

    return filteredContainer;
  }

  /**
   * builds a filtered container, i.e. a container with a selection vector attached.
   *
   * @param filteredContainer the filtered container we intend to prepare
   * @param selectionVector2 the selection vector for the filtered container
   * @param batch the incoming batch
   * @param predicate the predicate to filter the batch
   * @return the filtered container
   */
  private VectorContainerWithSV prepareFilteredContainer(
      VectorContainerWithSV filteredContainer,
      SelectionVector2 selectionVector2,
      VectorContainer batch,
      Predicate<ValueVector> predicate) {

    if (filteredContainer == null) {
      filteredContainer = buildVectorContainerWithSV2(selectionVector2);

      // projects the columns needed from incoming batch into the filtered container
      Iterator<ValueVector> filteredIterator = rowFilterIterator(batch, predicate);
      filteredContainer.addCollection(() -> filteredIterator);
      filteredContainer.buildSchema(SelectionVectorMode.TWO_BYTE);
    }
    return filteredContainer;
  }

  /**
   * Checks handles the sorter accordingly if the sorter's limits has been reached.
   *
   * <p>There are two limits we look out for:
   *
   * <ul>
   *   <li><b>Memory Limit:</b> The sorter's memory is full.
   *   <li><b>Record Limit:</b> The sorter's record count has reached the maximum.
   * </ul>
   *
   * If the memory limit was reached, the sorter rejected the incoming batch. In this case, We must
   * consume batch immediately into a new sorter.
   *
   * @param filteredContainer filteredContainer the incoming consumable container with Sv2 attached.
   *     sv2 filters out nulls in the file-path column because nulls in file-path column indicate
   *     data rows. We only want positional-delete rows.
   * @throws Exception
   */
  private void checkAndHandleSorterLimit(VectorContainerWithSV filteredContainer) throws Exception {
    boolean recordLimitReached =
        vectorSorter.getRecordCountInMemory() >= getMaxDeleteFileRecordCount();

    boolean memoryLimitReached = vectorSorter.getState() == State.CAN_PRODUCE;

    if (recordLimitReached || memoryLimitReached) {

      if (memoryLimitReached) {
        logger.debug(
            String.format(
                "A Vector Sorter has exceeded its memory limit. Total rows: %s. "
                    + "A new Vector Sorter will be generated "
                    + "to consume the remaining data from the batch.",
                vectorSorter.getRecordCountInMemory()));
      } else {
        logger.debug(
            String.format(
                "A Vector Sorter has exceeded its record limit. Total rows: %s. "
                    + "The Vector Sorter will consume the remaining records from the batch, "
                    + "then, transition to a producible state.",
                vectorSorter.getRecordCountInMemory()));
      }

      vectorSorter.noMoreToConsume();
      writableVectorSorters.add(
          new WritableVectorSorter(
              vectorSorter,
              deleteFileRecordWriter.getPartition(),
              new HashSet<>(referencedDataFiles)));
      referencedDataFiles.clear();

      // build new vector sorter to handle remaining incoming data from operator
      vectorSorter = new VectorSorter(context, ORDERINGS);
      vectorSorter.setup(filteredContainer, false);
      listener.setHasPendingOutput(true);

      if (memoryLimitReached) {
        vectorSorter.consumeData();
      }

      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.CAN_PRODUCE;
    }
  }

  /**
   * prepare the VectorSorter for consumption.
   *
   * @param filteredContainer the incoming consumable container with Sv2 attached. sv2 filters out
   *     nulls in the file-path column because nulls in file-path column indicate data rows. We only
   *     want positional-delete rows.
   */
  private void injectIncomingSVContainerIntoSorter(VectorContainerWithSV filteredContainer) {
    if (vectorSorter.getState() == State.NEEDS_SETUP) {
      vectorSorter.setup(filteredContainer, false);
    } else {
      vectorSorter.setIncoming(filteredContainer);
    }
    listener.setHasPendingOutput(true);
  }

  /**
   * column-based filter on the incoming batch based on the predicate.
   *
   * @param batch the incoming batch
   * @param predicate the predicate to filter the batch
   * @return an iterator of the filtered columns
   */
  private Iterator<ValueVector> rowFilterIterator(
      VectorContainer batch, Predicate<ValueVector> predicate) {
    return Streams.stream(batch)
        .filter(v -> predicate.test(v.getValueVector()))
        .map(v -> (ValueVector) v.getValueVector())
        .iterator();
  }

  /**
   * Processes all positional-delete content ready to be written. Close data-file writer if
   * completed-input or output-limit reached.
   */
  @Override
  public boolean processPendingOutput(boolean completedInput, boolean reachedOutputLimit)
      throws Exception {
    try {
      if (deleteFileRecordWriter == null) {
        return false;
      }
      return handlePositionalDeletes(completedInput, reachedOutputLimit);
    } catch (Exception e) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.ERROR;
      cleanupResources();
      throw new RuntimeException("Merge-On-Read-Writer failed to process an output", e);
    }
  }

  /**
   * Handles all pending VectorSorters ready to write positional deletes.
   *
   * <p>This method locks the operator into a CAN_PRODUCE state until all pending data is written.
   * The pending content is stored in a queue awaiting output.
   *
   * <p><strong>Important Note:</strong> This method writes positional-delete data in the
   * CAN_PRODUCE state, rather than the WriterOperator's traditional CAN_CONSUME state. Due to this,
   * the parquetWriter's current partition <i>likely</i> won't match the vectorSorter's content.
   *
   * <p>To write content with the correct partitioning, the default partition is temporarily
   * replaced with the WritableVectorSorter's cached partition spec. This ensures that data is
   * written to the correct partition.
   *
   * <p>It is crucial to track the delete-writer's latest-set partition ({@link
   * MergeOnReadRecordWriter#defaultPartition}) as provided by the WriterOperator to maintain
   * consistency once we have outputed all content from a sorter.
   *
   * @param completedInput A flag indicating if all incoming data from the operator has been
   *     consumed.
   * @return Flag indicating if any pending output must be written before moving the operator
   *     forward.
   * @throws Exception if an error occurs while attempting to write positional deletes.
   */
  private boolean handlePositionalDeletes(boolean completedInput, boolean reachedOutputLimit)
      throws Exception {
    try {
      // We must close dataFile parquet writer when either condition is met to ensure we pass
      // the output entry to the manifest + committer
      if (completedInput || reachedOutputLimit) {
        AutoCloseables.close(dataFileVectorContainer, dataFileRecordWriter);
      }

      // keep track of default partition for when we temporarily swap the partition.
      if (defaultPartition == null) {
        defaultPartition = deleteFileRecordWriter.getPartition();
      }

      // Handles Final Vector Sorter that never reached record Limit, if exists
      if (completedInput && writableVectorSorters.isEmpty() && currWritableSorter == null) {
        return handleFinalSorterIfExists();
      }

      // get the next writableSorter if the current one is null
      if (currWritableSorter == null && !writableVectorSorters.isEmpty()) {
        currWritableSorter = writableVectorSorters.poll();
        mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.CAN_PRODUCE;
      }

      switch (mergeOnReadDeleteWriterState) {
        case CAN_PRODUCE:
          produceOutputForDeleteFile();
          return true;
        case DONE:
          finalizeDeleteWriter(completedInput);
          return false;
        default:
          // Handle cleanup when all processing is complete and no operations are pending.
          if (completedInput) {
            cleanupResources();
            mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.NOT_SET_UP;
          }
          return false;
      }
    } catch (Exception e) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.ERROR;
      cleanupResources();
      throw new IOException("Unable to output positional deletes during Merge On Read DML.", e);
    }
  }

  /**
   * Finalizes the delete writer. This method is called when the delete writer is done processing
   * all the incoming data.
   *
   * @param completedInput A flag indicating if all incoming data from operator has been consumed.
   * @throws IOException If an I/O error occurs.
   */
  private void finalizeDeleteWriter(boolean completedInput) throws IOException {
    deleteFileRecordWriter.setPartition(defaultPartition);
    defaultPartition = null;
    if (completedInput) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.NOT_SET_UP;
      cleanupResources();
    } else {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.CAN_CONSUME;
    }
  }

  /**
   * Produces and writes the output for the delete file.
   *
   * <p>Content within the sorter is outputted on a per-batch basis. Batches are outputted and
   * written until all content from the sorter has been offloaded. parquet file is closed
   * immediately after the last batch is written in order uphold sorted order.
   *
   * @throws Exception
   */
  private void produceOutputForDeleteFile() throws Exception {
    int sortedOutputRecords = currWritableSorter.getVectorSorter().outputData();
    if (sortedOutputRecords != 0) {
      writeToDeleteFile(currWritableSorter);
    } else {
      // record writer already handles flushing when record limit is reached.
      sendReferencesToDeleteWriter(currWritableSorter.getReferencedDataFiles());
      deleteFileRecordWriter.flushAndPrepareForNextWritePath();
      deleteFileRecordWriter.setPartition(defaultPartition);
      currWritableSorter.close();
      currWritableSorter = null;

      // checks if there are more pending sorters to be written
      if (!writableVectorSorters.isEmpty()) {
        // moves to next writableSorter
        mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.CAN_PRODUCE;
        return;
      }

      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.DONE;
    }
  }

  /**
   * Sets the active sorter to a writable state if the sorter is the final one expected. Otherwise,
   * notify the operator that there is no more sorters pending to produce positional-delete content.
   * <br>
   *
   * <p>A sorter is final and ready to write when all the following conditions are met:
   *
   * <ul>
   *   <li>An active VectorSorter exists and is accepting consumption of delete-content
   *   <li>There are no pending sorters expecting to write
   *   <li>The operator has no more data to consume
   * </ul>
   *
   * Otherwise, notify the operator that there is no more positional-deletes to produce.
   *
   * <p>The final sorter was not placed within the queue because it was still active. We only
   * force-place it in the queue once data consumption is complete, indicating final vectorSorter is
   * ready to be sorted and written.
   *
   * @return A flag indicating if there is any pending output, which notifies the writer operator if
   *     we can proceed past the CAN_PRODUCE state.
   * @throws Exception if noMoreToConsume fails
   */
  private boolean handleFinalSorterIfExists() throws Exception {
    if (vectorSorter != null && vectorSorter.getState() != State.NEEDS_SETUP) {
      // captures the final sorter. This sorter was not placed within the queue because it
      // could still be active. We only place it in the queue once data consumption is
      // complete
      logger.debug(
          "Final Vector Sorter has lingering data. Transitioning the Vector Sorter to a producible state.");
      vectorSorter.noMoreToConsume();
      writableVectorSorters.add(
          new WritableVectorSorter(
              vectorSorter, defaultPartition, new HashSet<>(referencedDataFiles)));
      referencedDataFiles.clear();
      vectorSorter = null;
      return true;
    } else if (vectorSorter != null && vectorSorter.getState() == State.NEEDS_SETUP) {
      logger.debug("Final Vector Sorter is empty. Closing.");
      vectorSorter.close();
      vectorSorter = null;
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.DONE;
    }
    logger.debug("No producible Vector Sorters found.");
    return false;
  }

  /**
   * Writes the sorted output to the delete file.
   *
   * <p>Because positional delete-content is written in the CAN_PRODUCE state, there is a strong
   * likelihood that the <b>delete</b>-{@link ParquetRecordWriter}'s {@link WritePartition} points
   * the current state of the Consuming batch. This CAN_CONSUME 'batch data' partition is likely
   * different from the partition we wish to write to...
   *
   * <p>...Thus, In order to ensure we write to the proper partition, we will temporarily inject the
   * cached partition captured at the time of the current WritableVectorSorter's creation.
   *
   * @param writableVectorSorter The writable vector sorter.
   */
  private void writeToDeleteFile(WritableVectorSorter writableVectorSorter) {
    validateState(MergeOnReadDeleteWriterState.CAN_PRODUCE);
    try {
      deleteFileRecordWriter.setPartition(writableVectorSorter.getPartitionSpec());
      VectorAccessible sortedOutputDeleteVector =
          writableVectorSorter.getVectorSorter().getOutputVector();

      EventBasedRecordWriter eventBasedRecordWriter =
          new EventBasedRecordWriter(sortedOutputDeleteVector, deleteFileRecordWriter);

      // offset is always 0 because we are guaranteed to write every record released from the
      // sorter.
      // All record filtering was done prior to add adding the batch into the sorter.
      // ... Thus, we do not need an SV RecordWriter.
      eventBasedRecordWriter.write(0, sortedOutputDeleteVector.getRecordCount());
    } catch (Exception e) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.ERROR;
      throw new RuntimeException(
          "Failed to output content from the Vector Sorter to the Parquet Writer", e);
    }
  }

  /**
   * Write data-rows to the parquet file.
   *
   * <p>SV2 Container is used to exclude and positional-delete rows. We exclude by finding all
   * row-based indexes where the system columns are null.
   *
   * @param offset starting row-index of batch
   * @param length number of records to parse within batch
   * @return number of records written
   * @throws IOException
   */
  private int writeBatchContainingSplitRows(int offset, int length) throws IOException {
    try (SelectionVector2 filePathSV2 = getSV2FromContainer(filteredContainerForRowSplitDataFile)) {

      try {
        dataFileVectorContainer.setRecordCount(filePathVector.getValueCount());
        Predicate<ValueVector> excludeFilePath =
            v -> !v.getField().getName().equalsIgnoreCase(ColumnUtils.FILE_PATH_COLUMN_NAME);

        // column-based Filter. Remove the "file path" column from the incoming VectorContainer.
        // This is done to ensure that the "file path" column is not written to the output file.
        // we only need the data columns to be written.
        filteredContainerForRowSplitDataFile =
            prepareFilteredContainer(
                filteredContainerForRowSplitDataFile,
                filePathSV2,
                dataFileVectorContainer,
                excludeFilePath);

        // Prepare the fileLoadEntrySV2 using the provided offset and length.
        // Although the file path column is filtered out, the SV2 is still prepared using the
        // file path vector. When file-path is null, we can guarantee the row is a data-row.
        dataFileRecordWriter.prepareSV2(
            filePathVector, filePathSV2, offset, length, Objects::isNull, null);

        // Set the record count for the filteredContainer based on the fileLoadEntrySV2 count.
        filteredContainerForRowSplitDataFile.setRecordCount(filePathSV2.getCount());

        // Create or update the event-based record writer to use the filtered container for
        // writing.
        if (this.dataFileEventBasedRecordWriter == null) {
          this.dataFileEventBasedRecordWriter =
              new SVFilteredEventBasedRecordWriter(
                  filteredContainerForRowSplitDataFile, dataFileRecordWriter);
        } else {
          ((SVFilteredEventBasedRecordWriter) dataFileEventBasedRecordWriter)
              .setBatch(filteredContainerForRowSplitDataFile);
        }

        // Write the batch using the event-based record writer.
        return dataFileEventBasedRecordWriter.write(offset, length);
      } finally {
        if (offset + length == dataFileVectorContainer.getRecordCount()) {
          filteredContainerForRowSplitDataFile.close();
          filteredContainerForRowSplitDataFile = null;
        }
      }
    }
  }

  /**
   * Builds a new VectorContainerWithSV object. If the provided selection vector 2 is null, a new
   * selection vector 2 is created.
   *
   * @param sv2 The selection vector 2 to use.
   * @return The new VectorContainerWithSV object.
   */
  private VectorContainerWithSV buildVectorContainerWithSV2(SelectionVector2 sv2) {
    return new VectorContainerWithSV(
        context.getAllocator(), sv2 != null ? sv2 : new SelectionVector2(context.getAllocator()));
  }

  /**
   * Gets the selection vector 2 from the provided VectorContainerWithSV object. If the provided
   * VectorContainerWithSV object is null, a new selection vector 2 is created.
   *
   * @param filteredContainer The VectorContainerWithSV object to get the sv2 from.
   * @return The sv2
   */
  private SelectionVector2 getSV2FromContainer(VectorContainerWithSV filteredContainer) {
    return filteredContainer != null
        ? filteredContainer.getSelectionVector2()
        : new SelectionVector2(context.getAllocator());
  }

  /** Closes the resources held by the data and delete writer. */
  private void cleanupResources() throws IOException {
    logger.debug("Cleaning up all allocators and resources");
    try {
      AutoCloseables.close(
          filePathVector,
          posVector,
          vectorSorter,
          currWritableSorter,
          filteredContainerForRowSplitDataFile,
          filteredContainerForPartitionedTables,
          deleteFileVectorContainer,
          dataFileVectorContainer,
          deleteFileRecordWriter,
          dataFileRecordWriter);
      for (WritableVectorSorter writableVectorSorter : writableVectorSorters) {
        AutoCloseables.close(writableVectorSorter);
      }
    } catch (Exception e) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.ERROR;
      throw new IOException("Unable to close resources during Merge On Read DML.", e);
    }
  }

  /**
   * Aborts the record writer and cleans up resources.
   *
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public void abort() throws IOException {
    logger.debug("Aborting current resources");
    try {
      switch (writerMode) {
        case DELETE_FILES_ONLY:
          deleteFileRecordWriter.abort();
          break;
        case DATA_AND_DELETE_FILES:
          dataFileRecordWriter.abort();
          deleteFileRecordWriter.abort();
          break;
        case DATA_FILES_ONLY:
          dataFileRecordWriter.abort();
          break;
        default:
          throw new UnexpectedException("Merge On Read DML Writer reached an invalid state.");
      }
    } catch (Exception e) {
      mergeOnReadDeleteWriterState = MergeOnReadDeleteWriterState.ERROR;
      throw new IOException("Unable to abort Merge On Read DML.", e);
    } finally {
      cleanupResources();
    }
  }

  /** Closes the all resources held by the involved allocators. */
  @Override
  public void close() throws Exception {
    logger.debug("closing Merge-On-Read Writer.");
    cleanupResources();
  }

  /**
   * Calculates the maximum number of records that can be written to a delete file. The maximum
   * number of records is determined by the block size (memory) of the delete file. We bound this
   * number a max to ensure we don't run out of memory.
   *
   * <p>Because the delete-file schema is static, we can always expect 'file_path' and 'pos'
   * columns. A target block size of 8MB (8 Million Bytes) equates to roughly 2 Million records. The
   * 1:4 ratio is a safe rough-estimate for the number of records per block size.
   *
   * @return The maximum number of records that can be written to a delete file.
   */
  private Long getMaxDeleteFileRecordCount() {
    return context
            .getOptions()
            .getOption(ExecConstants.PARQUET_POSITIONAL_DELETEFILE_BLOCK_SIZE)
            .getNumVal()
        / DELETE_FILE_BLOCK_SIZE_TO_ROW_COUNT_RATIO;
  }

  /**
   * Sets the write batch mode.
   *
   * @param writeBatchMode The write batch mode.
   */
  public void setWriteBatchMode(WriteBatchMode writeBatchMode) {
    this.writeBatchMode = writeBatchMode;
  }

  /**
   * Resets the write batch mode to NOT_SPECIFIED. This should be called after the write batch
   * operation has been completed.
   */
  public void resetWriteBatchMode() {
    this.writeBatchMode = WriteBatchMode.NOT_SPECIFIED;
  }

  /**
   * This class represents a sortable unit of data that can be written to a partition. It implements
   * AutoCloseable to ensure that resources are properly released when no longer needed.
   */
  private static class WritableVectorSorter implements AutoCloseable {

    // VectorSorter is used to sort the data in memory before it is written to a partition.
    private final VectorSorter vectorSorter;

    // WritePartition represents the specific partition to which the data will be written.
    private final WritePartition partitionSpec;

    // referenced DataFiles associated with Positional Delete File
    private final Set<ByteArrayWrapper> referencedDataFiles;

    /**
     * Constructs a new WritableVectorSorter with the given VectorSorter and WritePartition.
     *
     * @param vectorSorter The VectorSorter used to sort the data.
     * @param partitionSpec The WritePartition representing the partition to which the data will be
     *     written.
     * @param referencedDataFiles The referenced Datafiles in the Positional Delete File
     */
    public WritableVectorSorter(
        VectorSorter vectorSorter,
        WritePartition partitionSpec,
        Set<ByteArrayWrapper> referencedDataFiles) {
      this.vectorSorter = vectorSorter;
      this.partitionSpec = partitionSpec;
      this.referencedDataFiles = referencedDataFiles;
    }

    /**
     * @return The VectorSorter used to sort the data.
     */
    public VectorSorter getVectorSorter() {
      return vectorSorter;
    }

    /**
     * @return The WritePartition representing the partition to which the data will be written.
     */
    public WritePartition getPartitionSpec() {
      return partitionSpec;
    }

    /**
     * @return the set of referenced Datafiles
     */
    public Set<ByteArrayWrapper> getReferencedDataFiles() {
      return referencedDataFiles;
    }

    /**
     * Closes this WritableVectorSorter, releasing any resources it holds. In this case, it closes
     * the VectorSorter.
     *
     * @throws Exception If an error occurs while closing the VectorSorter.
     */
    @Override
    public void close() throws Exception {
      vectorSorter.close();
    }
  }
}
