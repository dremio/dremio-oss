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
package com.dremio.exec.store.iceberg.manifestwriter;

import static com.dremio.exec.planner.sql.parser.DmlUtils.isInsertOperation;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.acceleration.UpdateIdWrapper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPOP;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.parquet.ParquetRecordWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;

public class ManifestFileRecordWriter implements RecordWriter {

  @JsonDeserialize(builder = ImmutableWritingContext.Builder.class)
  @Value.Immutable
  public interface WritingContext {

    LazyManifestWriter getManifestWriter();

    long getAddedRowCount();

    long getLength();

    long getDataFilesSize();

    UpdateIdWrapper getUpdateIdWrapper();

    byte[] getSchema();

    Set<IcebergPartitionData> getPartitionDataInCurrentManifest();

    static ImmutableWritingContext.Builder builder() {
      return new ImmutableWritingContext.Builder();
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(ManifestFileRecordWriter.class);
  private final OperatorContext context;
  private final OperatorStats operatorStats;
  private final WriterOptions writerOptions;
  private AtomicReference<OutputEntryListener> listener;

  /* Metrics */
  private int numFilesWritten = 0;
  private long minFileSize = Long.MAX_VALUE;
  private long maxFileSize = Long.MIN_VALUE;
  private long avgFileSize = 0;
  private long minRecordCountInFile = Long.MAX_VALUE;
  private long maxRecordCountInFile = Long.MIN_VALUE;

  /* Input */
  private VarBinaryVector metadataVector;
  private BigIntVector fileSizeVector;
  private BigIntVector totalInsertedRecords;
  private IntVector operationTypeVector;
  private VarCharVector pathVector;
  private BigIntVector rejectedRecordsVector;
  private final ManifestWritesHelper manifestWritesHelper;
  private boolean readMetadataVector = true;

  private UpdateIdWrapper updateIdWrapper = new UpdateIdWrapper();
  private long dataFilesSize = 0;
  private long addedRowCount = 0;

  private final boolean singleWriter;

  public ManifestFileRecordWriter(OperatorContext context, IcebergManifestWriterPOP writer) {
    this.context = context;
    writerOptions = writer.getOptions();
    operatorStats = context.getStats();
    manifestWritesHelper = getManifestWritesHelper(writer, context);
    readMetadataVector = !writer.getOptions().getTableFormatOptions().getIcebergSpecificOptions().getIcebergTableProps().isMetadataRefresh();
    this.singleWriter = writer.isSingleWriter();
  }

  @VisibleForTesting
  ManifestWritesHelper getManifestWritesHelper(IcebergManifestWriterPOP writer, OperatorContext context) {
    return ManifestWritesHelper.getInstance(writer, context);
  }

  @Override
  public void setup(VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener) throws IOException {
    if (readMetadataVector) {
      metadataVector = (VarBinaryVector) getVectorFromSchemaPath(incoming, RecordWriter.METADATA_COLUMN);
      fileSizeVector = (BigIntVector) getVectorFromSchemaPath(incoming, RecordWriter.FILESIZE_COLUMN);
    }
    if (isInsertOperation(writerOptions)) {
      totalInsertedRecords = (BigIntVector) getVectorFromSchemaPath(incoming, RecordWriter.RECORDS_COLUMN);
      pathVector = (VarCharVector) getVectorFromSchemaPath(incoming, RecordWriter.PATH_COLUMN);
      if (metadataVector == null) {
        metadataVector = (VarBinaryVector) getVectorFromSchemaPath(incoming, RecordWriter.METADATA_COLUMN);
      }
      if (fileSizeVector == null) {
        fileSizeVector = (BigIntVector) getVectorFromSchemaPath(incoming, RecordWriter.FILESIZE_COLUMN);
      }
      rejectedRecordsVector = (BigIntVector) getVectorFromSchemaPath(incoming, RecordWriter.REJECTED_RECORDS_COLUMN);
    }
    operationTypeVector = (IntVector) getVectorFromSchemaPath(incoming, RecordWriter.OPERATION_TYPE_COLUMN);

    this.listener = new AtomicReference<>();
    this.listener.set(listener);
    manifestWritesHelper.setIncoming(incoming);
    manifestWritesHelper.startNewWriter();
  }

  @Override
  public void startPartition(WritePartition partition) throws Exception {
  }

  @Override
  public int writeBatch(int offset, int length) throws IOException {
    for (int i = offset; i < length; i++) {
      if (manifestWritesHelper.hasReachedMaxLen()) {
        writeGeneratedManifestFileToDisk();
        manifestWritesHelper.startNewWriter();
        updateIdWrapper = new UpdateIdWrapper();
        dataFilesSize = 0;
        addedRowCount = 0;
      }

      if (readMetadataVector) {
        if (!metadataVector.isNull(i) && !(isInsertOperation(writerOptions) && isErrorRecord(i))) {
          updateIdWrapper.update(UpdateIdWrapper.deserialize(metadataVector.get(i)));
        }
        if (!fileSizeVector.isNull(i) && !(isInsertOperation(writerOptions) && isErrorRecord(i))) {
          dataFilesSize += fileSizeVector.get(i);
        }
      }

      if (isInsertOperation(writerOptions) && !totalInsertedRecords.isNull(i) && !isErrorRecord(i)) {
        addedRowCount += totalInsertedRecords.get(i);
      }

      manifestWritesHelper.processIncomingRow(i);
      processDeletedDataFiles();
      if (isInsertOperation(writerOptions) && isErrorRecord(i)) {
        processErrorRecords(i);
      }

      if (isOrphanFileRecord(i)) {
        processOrphanDataFiles();
      }

    }
    return length - offset + 1;
  }

  /**
   * Checks the record at an arbitrary index if it's connected to a copy into error.
   * @param row record index
   * @return true, if the operation type of the record equals {@link OperationType#COPY_INTO_ERROR}
   */
  private boolean isErrorRecord(int row) {
    return OperationType.COPY_INTO_ERROR.value == operationTypeVector.get(row);
  }

  /**
   * If the processed record is a copy into error record, just pass it through.
   * @param row record index
   */
  private void processErrorRecords(int row) {
    listener.get().recordsWritten(
      totalInsertedRecords.get(row),
      fileSizeVector.get(row),
      new String(pathVector.get(row)),
      metadataVector.get(row),
      null,
      null,
      null,
      null,
      operationTypeVector.get(row),
      null,
      rejectedRecordsVector.get(row)
    );
  }

  private boolean isOrphanFileRecord(int row) {
    return OperationType.ORPHAN_DATAFILE.value == operationTypeVector.get(row);
  }

  private void processOrphanDataFiles() {
    manifestWritesHelper.processOrphanFiles((orphanFile) -> {
        listener.get().recordsWritten(orphanFile.recordCount(), orphanFile.fileSizeInBytes(),
          orphanFile.path().toString(), null, orphanFile.specId(),
          null, manifestWritesHelper.getWrittenSchema(),
          null, OperationType.ORPHAN_DATAFILE.value, null, 0L);
      });
  }

  private void processDeletedDataFiles() {
    manifestWritesHelper.processDeletedFiles((deleteDataFile, metaInfoBytes) -> {
      IcebergPartitionData partitionData = null;
      if (!readMetadataVector) { // metadata refresh
        IcebergTableProps tableProps = writerOptions.getTableFormatOptions().getIcebergSpecificOptions().getIcebergTableProps();
        List<String> partitionColumns = tableProps.getPartitionColumnNames();
        BatchSchema batchSchema = tableProps.getFullSchema();
        PartitionSpec icebergPartitionSpec = IcebergUtils.getIcebergPartitionSpec(batchSchema, partitionColumns,
          SchemaConverter.getBuilder().build().toIcebergSchema(batchSchema));
        partitionData = IcebergPartitionData.fromStructLike(icebergPartitionSpec, deleteDataFile.partition());
      }

      listener.get().recordsWritten(deleteDataFile.recordCount(), deleteDataFile.fileSizeInBytes(),
              deleteDataFile.path().toString(), null, deleteDataFile.specId(),
              metaInfoBytes, manifestWritesHelper.getWrittenSchema(),
        Collections.singleton(partitionData), OperationType.DELETE_DATAFILE.value, null, 0L);
    });
  }

  @Override
  public void abort() throws IOException {
      if(manifestWritesHelper != null) {
        manifestWritesHelper.abort();
      }
  }

  @Override
  public void close() throws Exception {
    writeGeneratedManifestFileToDisk();

    manifestWritesHelper.getFutureResults();
  }

  private void writeGeneratedManifestFileToDisk() throws IOException {
    manifestWritesHelper.prepareWrite();

    if (!manifestWritesHelper.canWrite()) {
      return;
    }

    WritingContext writingContext = WritingContext.builder()
      .setManifestWriter(manifestWritesHelper.getManifestWriter())
      .setDataFilesSize(dataFilesSize)
      .setPartitionDataInCurrentManifest(manifestWritesHelper.partitionDataInCurrentManifest())
      .setUpdateIdWrapper(updateIdWrapper)
      .setAddedRowCount(addedRowCount)
      .setLength(manifestWritesHelper.length())
      .setSchema(manifestWritesHelper.getWrittenSchema())
      .build();
    if (singleWriter) {
      manifestWritesHelper.write(writingContext, this::processGeneratedManifestFile);
    } else {
      final ManifestFile generatedManifestFile = manifestWritesHelper.write();
      processGeneratedManifestFile(generatedManifestFile, writingContext);
    }
  }

  private void processGeneratedManifestFile(ManifestFile manifestFile,
                                            WritingContext writingContext) {
    //In case of Insert or CTAS recordCount should be total added rows.
    long recordCount = isInsertOperation(writerOptions) ? writingContext.getAddedRowCount() : manifestFile.addedFilesCount();
    byte[] manifestMetaInfo = null;
    try {
      manifestMetaInfo = IcebergSerDe.serializeToByteArray(
        new IcebergMetadataInformation(IcebergSerDe.serializeManifestFile(manifestFile)));
    } catch (IOException ex) {
      logger.error("Error while serializing manifest file {}", manifestFile, ex);
      return;
    }
    listener.get().recordsWritten(recordCount, writingContext.getLength() + writingContext.getDataFilesSize(), manifestFile.path(),
      writingContext.getUpdateIdWrapper().getUpdateId() != null ? writingContext.getUpdateIdWrapper().serialize() : null,
      manifestFile.partitionSpecId(), manifestMetaInfo, writingContext.getSchema(),
      writingContext.getPartitionDataInCurrentManifest(), OperationType.ADD_MANIFESTFILE.value, null, 0L);
    updateStats(manifestFile.length(), manifestFile.addedFilesCount());
  }

  @VisibleForTesting
  protected synchronized void updateStats(long memSize, long recordCount) {
    minFileSize = min(minFileSize, memSize);
    maxFileSize = max(maxFileSize, memSize);
    avgFileSize = (avgFileSize * numFilesWritten + memSize) / (numFilesWritten + 1);
    minRecordCountInFile = min(minRecordCountInFile, recordCount);
    maxRecordCountInFile = max(maxRecordCountInFile, recordCount);
    numFilesWritten++;

    operatorStats.setLongStat(ParquetRecordWriter.Metric.NUM_FILES_WRITTEN, numFilesWritten);
    operatorStats.setLongStat(ParquetRecordWriter.Metric.MIN_FILE_SIZE, minFileSize);
    operatorStats.setLongStat(ParquetRecordWriter.Metric.MAX_FILE_SIZE, maxFileSize);
    operatorStats.setLongStat(ParquetRecordWriter.Metric.AVG_FILE_SIZE, avgFileSize);
    operatorStats.setLongStat(ParquetRecordWriter.Metric.MIN_RECORD_COUNT_IN_FILE, minRecordCountInFile);
    operatorStats.setLongStat(ParquetRecordWriter.Metric.MAX_RECORD_COUNT_IN_FILE, maxRecordCountInFile);
  }
}
