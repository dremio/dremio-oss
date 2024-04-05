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

import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.physical.config.copyinto.CopyIntoHistoryExtendedProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.exec.store.dfs.SplitReaderCreator;
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
      String filePath,
      String errorMessage) {

    logger.info("COPY INTO operation is skipping {} due to: {}", filePath, errorMessage);

    // TODO: populate ExtendedFormatOptions for Parquet if implemented
    String infoJson =
        FileLoadInfo.Util.getJson(
            new CopyIntoFileLoadInfo.Builder(
                    copyIntoSkipQueryContext.getQueryId(),
                    copyIntoSkipQueryContext.getUserName(),
                    copyIntoSkipQueryContext.getTableNamespace(),
                    copyIntoQueryProperties.getStorageLocation(),
                    filePath,
                    new ExtendedFormatOptions(),
                    FileType.PARQUET.name(),
                    CopyIntoFileLoadInfo.CopyIntoFileState.SKIPPED)
                .setRecordsLoadedCount(0)
                .setRecordsRejectedCount(1)
                .build());

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
    private final String errorMessage;
    private final boolean isValidationMode;
    private ValidationErrorRowWriter validationErrorWriter;
    private CopyIntoHistoryExtendedProperties copyIntoHistoryExtendedProperties;

    CopyIntoSkipErrorRecordReaderCreator(
        SimpleQueryContext copyIntoSkipQueryContext,
        CopyIntoQueryProperties copyIntoQueryProperties,
        String filePath,
        String errorMessage,
        CopyIntoHistoryExtendedProperties copyIntoHistoryExtendedProperties,
        boolean isValidationMode) {
      this.copyIntoQueryProperties = copyIntoQueryProperties;
      this.copyIntoSkipQueryContext = copyIntoSkipQueryContext;
      this.copyIntoHistoryExtendedProperties = copyIntoHistoryExtendedProperties;
      this.filePath = filePath;
      this.errorMessage = errorMessage;
      this.isValidationMode = isValidationMode;
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
                  filePath,
                  errorMessage);
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
