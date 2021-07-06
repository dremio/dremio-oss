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
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.IOException;
import java.util.Optional;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.ManifestFile;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.iceberg.manifestwriter.ManifestWritesHelper;
import com.dremio.exec.store.parquet.ParquetRecordWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.google.common.annotations.VisibleForTesting;

public class ManifestFileRecordWriter implements RecordWriter {

  private final OperatorStats operatorStats;
  private byte[] currentMetadata = null;
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
  private ManifestWritesHelper manifestWritesHelper;
  private boolean readMetadataVector = true;

  public ManifestFileRecordWriter(OperatorContext context, EasyWriter writer, IcebergFormatConfig formatConfig) {
    operatorStats = context.getStats();
    manifestWritesHelper = getManifestWritesHelper(writer, formatConfig);
    readMetadataVector = !writer.getOptions().getIcebergTableProps().isMetadataRefresh();
  }

  @VisibleForTesting
  ManifestWritesHelper getManifestWritesHelper(EasyWriter writer, IcebergFormatConfig formatConfig) {
    return ManifestWritesHelper.getInstance(writer, formatConfig);
  }

  @Override
  public void setup(VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener) throws IOException {
    if (readMetadataVector) {
      metadataVector = (VarBinaryVector) getVectorFromSchemaPath(incoming, RecordWriter.METADATA_COLUMN);
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
      if (readMetadataVector) {
        currentMetadata = metadataVector.get(i);
      }
      if (manifestWritesHelper.hasReachedMaxLen()) {
        writeGeneratedManifestFileToDisk();
        manifestWritesHelper.startNewWriter();
      }

      manifestWritesHelper.processIncomingRow(i);
      processDeletedDataFiles();
    }
    return length - offset + 1;
  }

  private void processDeletedDataFiles() {
    manifestWritesHelper.processDeletedFiles((deleteDataFile, metaInfoBytes) -> {
      listener.recordsWritten(deleteDataFile.recordCount(), deleteDataFile.fileSizeInBytes(),
              deleteDataFile.path().toString(), currentMetadata, deleteDataFile.specId(),
              metaInfoBytes, manifestWritesHelper.getWrittenSchema());
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
      listener.recordsWritten(addedFilesCount, manifestWritesHelper.length(), manifestFile.path(), currentMetadata,
              manifestFile.partitionSpecId(), manifestMetaInfo, manifestWritesHelper.getWrittenSchema());
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
