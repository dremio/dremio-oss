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

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;

import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.acceleration.UpdateIdWrapper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.iceberg.IcebergFormatConfig;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.parquet.ParquetRecordWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.google.common.annotations.VisibleForTesting;

public class ManifestFileRecordWriter implements RecordWriter {

  private final OperatorStats operatorStats;
  private final WriterOptions writerOptions;
  private OutputEntryListener listener;

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
  private final ManifestWritesHelper manifestWritesHelper;
  private boolean readMetadataVector = true;

  private UpdateIdWrapper updateIdWrapper = new UpdateIdWrapper();
  private long dataFilesSize = 0;

  public ManifestFileRecordWriter(OperatorContext context, EasyWriter writer, IcebergFormatConfig formatConfig) {
    writerOptions = writer.getOptions();
    operatorStats = context.getStats();
    manifestWritesHelper = getManifestWritesHelper(writer, formatConfig, (int) context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
    readMetadataVector = !writer.getOptions().getIcebergTableProps().isMetadataRefresh();
  }

  @VisibleForTesting
  ManifestWritesHelper getManifestWritesHelper(EasyWriter writer, IcebergFormatConfig formatConfig, int columnLimit) {
    return ManifestWritesHelper.getInstance(writer, formatConfig, columnLimit);
  }

  @Override
  public void setup(VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener) throws IOException {
    if (readMetadataVector) {
      metadataVector = (VarBinaryVector) getVectorFromSchemaPath(incoming, RecordWriter.METADATA_COLUMN);
      fileSizeVector = (BigIntVector) getVectorFromSchemaPath(incoming, RecordWriter.FILESIZE_COLUMN);
    }

    this.listener = listener;
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
      }

      if (readMetadataVector) {
        if (!metadataVector.isNull(i)) {
          updateIdWrapper.update(UpdateIdWrapper.deserialize(metadataVector.get(i)));
        }
        if (!fileSizeVector.isNull(i)) {
          dataFilesSize += fileSizeVector.get(i);
        }
      }

      manifestWritesHelper.processIncomingRow(i);
      processDeletedDataFiles();
    }
    return length - offset + 1;
  }

  private void processDeletedDataFiles() {
    manifestWritesHelper.processDeletedFiles((deleteDataFile, metaInfoBytes) -> {
      IcebergPartitionData partitionData = null;
      if (!readMetadataVector) { // metadata refresh
        List<String> partitionColumns = writerOptions.getIcebergTableProps().getPartitionColumnNames();
        BatchSchema batchSchema = writerOptions.getIcebergTableProps().getFullSchema();
        PartitionSpec icebergPartitionSpec = IcebergUtils.getIcebergPartitionSpec(batchSchema, partitionColumns,
          new SchemaConverter().toIcebergSchema(batchSchema));
        partitionData = IcebergPartitionData.fromStructLike(icebergPartitionSpec, deleteDataFile.partition());
      }

      listener.recordsWritten(deleteDataFile.recordCount(), deleteDataFile.fileSizeInBytes(),
              deleteDataFile.path().toString(), null, deleteDataFile.specId(),
              metaInfoBytes, manifestWritesHelper.getWrittenSchema(),
        Collections.singleton(partitionData));
    });
  }

  @Override
  public void abort() throws IOException {

  }

  @Override
  public void close() throws Exception {
    writeGeneratedManifestFileToDisk();
  }

  private void writeGeneratedManifestFileToDisk() throws IOException {
    final Optional<ManifestFile> generatedManifestFile = manifestWritesHelper.write();
    if (generatedManifestFile.isPresent()) {
      final ManifestFile manifestFile = generatedManifestFile.get();
      int addedFilesCount = manifestFile.addedFilesCount();
      byte[] manifestMetaInfo = IcebergSerDe.serializeToByteArray(
              new IcebergMetadataInformation(IcebergSerDe.serializeManifestFile(manifestFile),
                      IcebergMetadataInformation.IcebergMetadataFileType.MANIFEST_FILE));
      listener.recordsWritten(addedFilesCount, manifestWritesHelper.length() + dataFilesSize, manifestFile.path(),
              updateIdWrapper.getUpdateId() != null ? updateIdWrapper.serialize() : null,
              manifestFile.partitionSpecId(), manifestMetaInfo, manifestWritesHelper.getWrittenSchema(), manifestWritesHelper.partitionDataInCurrentManifest());
      updateStats(manifestFile.length(), addedFilesCount);
    }
  }

  @VisibleForTesting
  protected void updateStats(long memSize, long recordCount) {
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
