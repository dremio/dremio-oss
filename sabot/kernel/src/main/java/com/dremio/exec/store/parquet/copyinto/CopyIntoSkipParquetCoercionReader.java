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

import static com.dremio.exec.planner.ExceptionUtils.collapseExceptionMessages;
import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties.PropertyKey;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.Builder;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.CopyIntoFileState;
import com.dremio.exec.physical.config.copyinto.CopyIntoHistoryExtendedProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties;
import com.dremio.exec.physical.config.copyinto.IngestionProperties;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.TypeCoercion;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.exec.store.parquet.ParquetFilters;
import com.dremio.exec.tablefunctions.copyerrors.ValidationErrorRowWriter;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.util.VectorUtil;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.project.ProjectErrorUtils;
import com.dremio.sabot.op.project.ProjectErrorUtils.ProjectionError;
import com.dremio.sabot.op.scan.MutatorSchemaChangeCallBack;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.scan.ScanOperator.Metric;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import io.protostuff.ByteString;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;

/**
 * COPY INTO dry run implementation of Parquet reader - used for the 1st (dry run) scan of a double
 * scan during COPY INTO 'skip_file' - used for copy_errors() table function to report detailed
 * errors
 */
public class CopyIntoSkipParquetCoercionReader extends CopyIntoTransformationParquetCoercionReader {

  private final CopyIntoQueryProperties copyIntoQueryProperties;
  private final SimpleQueryContext copyIntoQueryContext;
  private final CopyIntoHistoryExtendedProperties copyIntoHistoryExtendedProperties;
  private final CopyIntoSkipParquetSplitReaderCreatorIterator parquetSplitReaderCreatorIterator;
  private boolean failureSeen = false;
  private boolean isCopyHistoryEventRecorded = false;
  private boolean isCopyIntoSkip = false;
  private boolean isValidationMode = false;
  private ValidationErrorRowWriter validationErrorWriter;
  private final int rowgroup;
  private final long rowIndexOffset;
  // vector container for running the projection (i.e. coercion) output into, to be discarded later
  private VectorContainer validationContainer = null;
  private boolean writeSuccessEvent = false;
  private Exception setupError = null;
  private final long processingStartTime;
  private final long fileSize;
  private final IngestionProperties ingestionProperties;
  private VarCharVector errorVector = null;

  protected CopyIntoSkipParquetCoercionReader(
      OperatorContext context,
      List<SchemaPath> columns,
      RecordReader inner,
      BatchSchema originalSchema,
      TypeCoercion typeCoercion,
      ParquetFilters filters,
      ByteString extendedProperties,
      CopyIntoSkipParquetSplitReaderCreatorIterator parquetSplitReaderCreatorIterator,
      int rowgroup,
      long rowIndexOffset,
      boolean writeSuccessEvent,
      long fileSize,
      IngestionProperties ingestionProperties,
      CopyIntoTransformationProperties transformationProperties,
      BatchSchema targetSchema) {
    super(
        context,
        columns,
        inner,
        originalSchema,
        typeCoercion,
        filters,
        transformationProperties,
        targetSchema);
    this.parquetSplitReaderCreatorIterator = parquetSplitReaderCreatorIterator;
    this.rowgroup = rowgroup;
    this.rowIndexOffset = rowIndexOffset;
    this.processingStartTime = parquetSplitReaderCreatorIterator.getProcessingStartTime();
    Optional<CopyIntoExtendedProperties> copyIntoExtendedPropertiesOptional =
        CopyIntoExtendedProperties.Util.getProperties(extendedProperties);
    if (copyIntoExtendedPropertiesOptional.isEmpty()) {
      throw new RuntimeException(
          "CopyIntoSkipParquetCoercionReader requires CopyIntoExtendedProperties");
    }
    CopyIntoExtendedProperties copyIntoExtendedProperties =
        copyIntoExtendedPropertiesOptional.get();
    this.copyIntoQueryProperties =
        copyIntoExtendedProperties.getProperty(
            PropertyKey.COPY_INTO_QUERY_PROPERTIES, CopyIntoQueryProperties.class);
    if (this.copyIntoQueryProperties != null) {
      this.isCopyIntoSkip =
          CopyIntoQueryProperties.OnErrorOption.SKIP_FILE.equals(
              this.copyIntoQueryProperties.getOnErrorOption());
      this.writeSuccessEvent =
          writeSuccessEvent
              && copyIntoQueryProperties.shouldRecord(
                  CopyIntoFileLoadInfo.CopyIntoFileState.FULLY_LOADED);
    }

    this.copyIntoQueryContext =
        copyIntoExtendedProperties.getProperty(PropertyKey.QUERY_CONTEXT, SimpleQueryContext.class);

    this.copyIntoHistoryExtendedProperties =
        copyIntoExtendedProperties.getProperty(
            PropertyKey.COPY_INTO_HISTORY_PROPERTIES, CopyIntoHistoryExtendedProperties.class);

    if (copyIntoHistoryExtendedProperties != null) {
      this.isValidationMode = true;
    }

    this.fileSize = fileSize;
    this.ingestionProperties = ingestionProperties;
  }

  public static CopyIntoSkipParquetCoercionReader newInstance(
      OperatorContext context,
      List<SchemaPath> columns,
      RecordReader inner,
      BatchSchema originalSchema,
      TypeCoercion typeCoercion,
      ParquetFilters filters,
      ByteString extendedProperties,
      CopyIntoSkipParquetSplitReaderCreatorIterator parquetSplitReaderCreatorIterator,
      int rowgroup,
      long rowIndexOffset,
      boolean writeSuccessEvent,
      long fileSize,
      IngestionProperties ingestionProperties,
      CopyIntoTransformationProperties transformationProperties,
      BatchSchema targetSchema) {
    return new CopyIntoSkipParquetCoercionReader(
        context,
        columns,
        inner,
        originalSchema,
        typeCoercion,
        filters,
        extendedProperties,
        parquetSplitReaderCreatorIterator,
        rowgroup,
        rowIndexOffset,
        writeSuccessEvent,
        fileSize,
        ingestionProperties,
        transformationProperties,
        targetSchema);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      super.setup(output);
    } catch (Exception e) {
      setupError = e;
      outgoingMutator = output;
    }
    if (isValidationMode) {
      // here 'outgoing' is the copy_errors() result holder
      this.validationErrorWriter =
          ParquetCopyIntoSkipUtils.oneRowvalidationErrorRowWriter(
              output, inner.getFilePath(), copyIntoHistoryExtendedProperties.getOriginalJobId());
    }
  }

  @Override
  protected void setupProjector(OutputMutator outgoing, VectorContainer projectorOutput) {
    if (isValidationMode) {
      // dummy constructs to direct the projection output into during a copy_errors() run
      validationContainer = VectorContainer.create(context.getAllocator(), originalSchema);

      // in validation mode we would like to know the record position, where an error was seen
      // this is only possible with Java expression evaluation
      projectorOptions.setCodeGenOption(SupportedEngines.CodeGenOption.Java.name());
      projectorOptions.setTrackRecordLevelErrors(true);

      this.errorVector =
          (VarCharVector)
              VectorUtil.getVectorFromSchemaPath(
                  validationContainer, ColumnUtils.COPY_HISTORY_COLUMN_NAME);

      // here 'outgoing' is the copy_errors() result holder
      OutputMutator validationMutator =
          new ScanOperator.ScanMutator(
              validationContainer, context, (MutatorSchemaChangeCallBack) outgoing.getCallBack());
      super.setupProjector(validationMutator, validationContainer);
    } else {
      super.setupProjector(outgoing, projectorOutput);
    }
  }

  @Override
  public int next() {
    if (failureSeen || isCopyHistoryEventRecorded) {
      return 0;
    }
    long startDryRunNs = System.nanoTime();

    int batchRecordCount = 0;
    long totalRecordCount = 0;

    try {
      do {
        if (setupError != null) {
          throw setupError;
        }
        batchRecordCount = super.next();
        totalRecordCount += batchRecordCount;

        if (isValidationMode) {
          // In validation mode exceptions are caught and error details are saved into the error
          // vector instead
          // Thus we need to manually check the error vector and throw if errors were found
          throwOnFirstError();
        }

        // need to process all batches in one go, as we need to return 0 rowcount in a dry run
        allocate(mutator.getFieldVectorMap());
      } while (batchRecordCount != 0);
      return writeCopyIntoSuccessEvent(totalRecordCount);
    } catch (Exception e) {
      // reports to creatorIterator to stop providing readers for this particular file
      parquetSplitReaderCreatorIterator.markFailedFile(Path.of(inner.getFilePath()));
      failureSeen = true;

      if (isCopyIntoSkip) {
        // writes 1 error to the COPY INTO ERROR column
        ParquetCopyIntoSkipUtils.writeCopyIntoSkipError(
            outgoingMutator,
            copyIntoQueryContext,
            copyIntoQueryProperties,
            ingestionProperties,
            copyIntoTransformationProperties,
            inner.getFilePath(),
            fileSize,
            1L,
            e.getMessage(),
            processingStartTime);
      } else {
        // write a more detailed error description row for copy_errors() table function output
        Preconditions.checkState(isValidationMode);
        String errorDescription =
            setupError != null
                ? String.format(
                    "Error encountered during reader setup in rowgroup %d: %s",
                    rowgroup, collapseExceptionMessages(e))
                : String.format("%s in rowgroup %d", e.getMessage(), rowgroup);
        // record number is calculated as: get rowgroup offset of the rowgroup we're reading, and
        // add the number of records we have seen so far in the rowgroup; then since idxInBatch
        // denotes the error
        // position in the current _batch_ only, we need to rewind 1 batch worth of records before
        // adding it
        Long recordNum = null;
        String fieldName = null;
        if (e instanceof ProjectErrorUtils.ProjectionError) {
          ProjectErrorUtils.ProjectionError projectionError = (ProjectErrorUtils.ProjectionError) e;
          recordNum =
              rowIndexOffset
                  + totalRecordCount
                  - batchRecordCount
                  + projectionError.getIdxInBatch()
                  + 1;
          if (projectionError.getFieldNames() != null) {
            fieldName = Joiner.on(',').join(projectionError.getFieldNames());
          }
        }
        validationErrorWriter.write(fieldName, recordNum, recordNum, errorDescription);
      }
      context.getStats().addLongStat(Metric.NUM_READERS_SKIPPED, 1);
      return 1;
    } finally {
      context
          .getStats()
          .addLongStat(Metric.DRY_RUN_READ_TIME_NS, System.nanoTime() - startDryRunNs);
    }
  }

  private void throwOnFirstError() throws Exception {
    Iterator<ProjectionError> it =
        ProjectErrorUtils.parseErrors(
            errorVector, fieldId -> originalSchema.getColumn(fieldId).getName());
    if (it.hasNext()) {
      throw it.next();
    }
  }

  private int writeCopyIntoSuccessEvent(long totalRecordsCount) {
    if (!this.isCopyHistoryEventRecorded && this.writeSuccessEvent) {
      Builder builder =
          new Builder(
                  this.copyIntoQueryContext.getQueryId(),
                  this.copyIntoQueryContext.getUserName(),
                  this.copyIntoQueryContext.getTableNamespace(),
                  this.copyIntoQueryProperties.getStorageLocation(),
                  this.inner.getFilePath(),
                  new ExtendedFormatOptions(),
                  FileType.PARQUET.name(),
                  CopyIntoFileState.FULLY_LOADED)
              .setRecordsLoadedCount(totalRecordsCount)
              .setBranch(copyIntoQueryProperties.getBranch())
              .setFileSize(fileSize)
              .setProcessingStartTime(processingStartTime);
      if (ingestionProperties != null) {
        builder.setPipeName(ingestionProperties.getPipeName());
        builder.setPipeId(ingestionProperties.getPipeId());
        builder.setRequestId(ingestionProperties.getRequestId());
        builder.setIngestionSourceType(ingestionProperties.getIngestionSourceType());
        builder.setFileNotificationTimestamp(ingestionProperties.getNotificationTimestamp());
      }

      String infoJson = FileLoadInfo.Util.getJson(builder.build());

      // clear all vectors and write history info at the first position
      outgoingMutator.getVectors().forEach(ValueVector::clear);
      ValueVector historyColName =
          this.outgoingMutator.getVector(ColumnUtils.COPY_HISTORY_COLUMN_NAME);
      writeToVector(historyColName, 0, infoJson);
      this.outgoingMutator.getContainer().setAllCount(1);
      this.isCopyHistoryEventRecorded = true;
      return 1;
    }
    return 0;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(validationContainer);
    super.close();
  }
}
