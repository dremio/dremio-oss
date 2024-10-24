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
package com.dremio.exec.store.parquet.copyinto;

import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.ExtendedProperty;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.Builder;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.CopyIntoFileState;
import com.dremio.exec.physical.config.copyinto.CopyIntoHistoryExtendedProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties;
import com.dremio.exec.physical.config.copyinto.IngestionProperties;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.tablefunctions.copyerrors.ValidationErrorRowWriter;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.io.file.Path;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.namespace.file.proto.FileType;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for Parquet file import error handling during COPY INTO SKIP_FILE command. */
public class ParquetCopyIntoSkipUtils {

  private static final Logger logger = LoggerFactory.getLogger(ParquetCopyIntoSkipUtils.class);

  static void writeCopyIntoSkipError(
      OutputMutator outgoingMutator,
      SimpleQueryContext copyIntoSkipQueryContext,
      CopyIntoQueryProperties copyIntoQueryProperties,
      IngestionProperties ingestionProperties,
      CopyIntoTransformationProperties transformationProperties,
      String filePath,
      long fileSize,
      long rejectedRecordCount,
      String errorMessage,
      long processingStartTime) {

    logger.info("COPY INTO operation is skipping {} due to: {}", filePath, errorMessage);

    // TODO: populate ExtendedFormatOptions for Parquet if implemented
    Builder builder =
        new Builder(
                copyIntoSkipQueryContext.getQueryId(),
                copyIntoSkipQueryContext.getUserName(),
                copyIntoSkipQueryContext.getTableNamespace(),
                copyIntoQueryProperties.getStorageLocation(),
                filePath,
                new ExtendedFormatOptions(),
                FileType.PARQUET.name(),
                CopyIntoFileState.SKIPPED)
            .setRecordsLoadedCount(0)
            .setRecordsRejectedCount(rejectedRecordCount)
            .setBranch(copyIntoQueryProperties.getBranch())
            .setProcessingStartTime(processingStartTime)
            .setFileSize(fileSize)
            .setFirstErrorMessage(errorMessage);

    if (ingestionProperties != null) {
      builder
          .setPipeId(ingestionProperties.getPipeId())
          .setPipeName(ingestionProperties.getPipeName())
          .setFileNotificationTimestamp(ingestionProperties.getNotificationTimestamp())
          .setIngestionSourceType(ingestionProperties.getIngestionSourceType())
          .setRequestId(ingestionProperties.getRequestId());
    }

    if (transformationProperties != null) {
      builder.setTransformationProperties(
          ExtendedProperty.Util.serialize(transformationProperties));
    }

    String infoJson = FileLoadInfo.Util.getJson(builder.build());

    // write error info to first position about why this file is being SKIPPED
    ValueVector errorVector = outgoingMutator.getVector(ColumnUtils.COPY_HISTORY_COLUMN_NAME);
    writeToVector(errorVector, 0, infoJson);
    outgoingMutator.getContainer().setAllCount(1);
  }

  /**
   * Creates a vector writer for copy_errors() that writes 1 row only into the supplied output
   * mutator
   *
   * @param outputMutator vectors to write to
   * @param filePathForError file where we saw the error in
   * @param originalJobId original COPY INTO job ID
   * @return writer instance
   */
  public static ValidationErrorRowWriter oneRowvalidationErrorRowWriter(
      OutputMutator outputMutator, String filePathForError, String originalJobId) {
    BatchSchema validationResultSchema = outputMutator.getContainer().getSchema();
    ValueVector[] validationResult = new ValueVector[validationResultSchema.getTotalFieldCount()];
    int fieldIx = 0;
    for (Field f : validationResultSchema) {
      validationResult[fieldIx++] = outputMutator.getVector(f.getName());
    }
    return ValidationErrorRowWriter.newVectorWriter(
        validationResult, filePathForError, originalJobId, () -> 0);
  }

  /**
   * A dummy RecordReaderCreator to provide means of writing error details relating to Parquet file
   * initialization (i.e. footer reading) during the COPY INTO operation.
   */
  static class CopyIntoSkipErrorRecordReaderCreator extends SplitReaderCreator {

    private final SimpleQueryContext copyIntoSkipQueryContext;
    private final CopyIntoQueryProperties copyIntoQueryProperties;
    private final String filePath;
    private final long fileSize;
    private final long rejectedRecordCount;
    private final String errorMessage;
    private final boolean isValidationMode;
    private ValidationErrorRowWriter validationErrorWriter;
    private final CopyIntoHistoryExtendedProperties copyIntoHistoryExtendedProperties;
    private final IngestionProperties ingestionProperties;
    private final long processingStartTime;
    private final CopyIntoTransformationProperties transformationProperties;

    CopyIntoSkipErrorRecordReaderCreator(
        SimpleQueryContext copyIntoSkipQueryContext,
        CopyIntoQueryProperties copyIntoQueryProperties,
        IngestionProperties ingestionProperties,
        CopyIntoTransformationProperties transformationProperties,
        String filePath,
        long fileSize,
        long rejectedRecordCount,
        String errorMessage,
        CopyIntoHistoryExtendedProperties copyIntoHistoryExtendedProperties,
        boolean isValidationMode,
        long processingStartTime) {
      this.copyIntoQueryProperties = copyIntoQueryProperties;
      this.copyIntoSkipQueryContext = copyIntoSkipQueryContext;
      this.copyIntoHistoryExtendedProperties = copyIntoHistoryExtendedProperties;
      this.filePath = filePath;
      this.fileSize = fileSize;
      this.rejectedRecordCount = rejectedRecordCount;
      this.errorMessage = errorMessage;
      this.isValidationMode = isValidationMode;
      this.ingestionProperties = ingestionProperties;
      this.processingStartTime = processingStartTime;
      this.transformationProperties = transformationProperties;
    }

    @Override
    public RecordReader createRecordReader(MutableParquetMetadata footer) {
      return new RecordReader() {
        private OutputMutator outputMutator;
        private boolean errorReturned = false;

        @Override
        public void setup(OutputMutator output) throws ExecutionSetupException {
          this.outputMutator = output;
          if (isValidationMode) {
            validationErrorWriter =
                oneRowvalidationErrorRowWriter(
                    outputMutator, filePath, copyIntoHistoryExtendedProperties.getOriginalJobId());
          }
        }

        @Override
        public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {}

        @Override
        public int next() {
          if (errorReturned) {
            return 0;
          } else {
            errorReturned = true;
            if (isValidationMode) {
              validationErrorWriter.write(null, null, null, errorMessage);
            } else {
              writeCopyIntoSkipError(
                  outputMutator,
                  copyIntoSkipQueryContext,
                  copyIntoQueryProperties,
                  ingestionProperties,
                  transformationProperties,
                  filePath,
                  fileSize,
                  rejectedRecordCount,
                  errorMessage,
                  processingStartTime);
            }
            return 1;
          }
        }

        @Override
        public void close() throws Exception {}
      };
    }

    @Override
    public InputStreamProvider getInputStreamProvider() {
      return null;
    }

    @Override
    public MutableParquetMetadata getFooter() {
      return null;
    }

    @Override
    public void createInputStreamProvider(
        InputStreamProvider lastInputStreamProvider, MutableParquetMetadata lastFooter) {}

    @Override
    public void close() throws Exception {}

    @Override
    public Path getPath() {
      return Path.of(filePath);
    }

    @Override
    public void addRowGroupsToRead(Set<Integer> rowGroupList) {}
  }
}
