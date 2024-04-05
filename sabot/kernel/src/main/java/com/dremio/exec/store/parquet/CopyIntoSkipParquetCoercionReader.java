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

import static com.dremio.exec.planner.ExceptionUtils.collapseExceptionMessages;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoHistoryExtendedProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.TypeCoercion;
import com.dremio.exec.tablefunctions.copyerrors.ValidationErrorRowWriter;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.MutatorSchemaChangeCallBack;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.scan.ScanOperator.Metric;
import com.google.common.base.Preconditions;
import io.protostuff.ByteString;
import java.util.List;
import java.util.Optional;

/**
 * COPY INTO dry run implementation of Parquet reader - used for the 1st (dry run) scan of a double
 * scan during COPY INTO 'skip_file' - used for copy_errors() table function to report detailed
 * errors
 */
public class CopyIntoSkipParquetCoercionReader extends ParquetCoercionReader {

  private final CopyIntoQueryProperties copyIntoQueryProperties;
  private final SimpleQueryContext copyIntoQueryContext;
  private final CopyIntoHistoryExtendedProperties copyIntoHistoryExtendedProperties;
  private final CopyIntoSkipParquetSplitReaderCreatorIterator parquetSplitReaderCreatorIterator;
  private boolean failureSeen = false;
  private boolean isCopyIntoSkip = false;
  private boolean isValidationMode = false;
  private ValidationErrorRowWriter validationErrorWriter;
  private final int rowgroup;
  // vector container for running the projection (i.e. coercion) output into, to be discarded later
  private VectorContainer validationContainer = null;
  private Exception setupError = null;

  protected CopyIntoSkipParquetCoercionReader(
      OperatorContext context,
      List<SchemaPath> columns,
      RecordReader inner,
      BatchSchema originalSchema,
      TypeCoercion typeCoercion,
      ParquetFilters filters,
      ByteString extendedProperties,
      CopyIntoSkipParquetSplitReaderCreatorIterator parquetSplitReaderCreatorIterator,
      int rowgroup) {
    super(context, columns, inner, originalSchema, typeCoercion, filters);
    this.parquetSplitReaderCreatorIterator = parquetSplitReaderCreatorIterator;
    this.rowgroup = rowgroup;
    Optional<CopyIntoExtendedProperties> copyIntoExtendedPropertiesOptional =
        CopyIntoExtendedProperties.Util.getProperties(extendedProperties);
    if (!copyIntoExtendedPropertiesOptional.isPresent()) {
      throw new RuntimeException(
          "CopyIntoSkipParquetCoercionReader requires CopyIntoExtendedProperties");
    }
    CopyIntoExtendedProperties copyIntoExtendedProperties =
        copyIntoExtendedPropertiesOptional.get();
    this.copyIntoQueryProperties =
        copyIntoExtendedProperties.getProperty(
            CopyIntoExtendedProperties.PropertyKey.COPY_INTO_QUERY_PROPERTIES,
            CopyIntoQueryProperties.class);
    if (this.copyIntoQueryProperties != null) {
      this.isCopyIntoSkip =
          CopyIntoQueryProperties.OnErrorOption.SKIP_FILE.equals(
              this.copyIntoQueryProperties.getOnErrorOption());
    }

    this.copyIntoQueryContext =
        copyIntoExtendedProperties.getProperty(
            CopyIntoExtendedProperties.PropertyKey.QUERY_CONTEXT, SimpleQueryContext.class);

    this.copyIntoHistoryExtendedProperties =
        copyIntoExtendedProperties.getProperty(
            CopyIntoExtendedProperties.PropertyKey.COPY_INTO_HISTORY_PROPERTIES,
            CopyIntoHistoryExtendedProperties.class);

    if (copyIntoHistoryExtendedProperties != null) {
      this.isValidationMode = true;
    }
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
      int rowgroup) {
    return new CopyIntoSkipParquetCoercionReader(
        context,
        columns,
        inner,
        originalSchema,
        typeCoercion,
        filters,
        extendedProperties,
        parquetSplitReaderCreatorIterator,
        rowgroup);
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
    if (failureSeen) {
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
        // need to process all batches in one go, as we need to return 0 rowcount in a dry run
        allocate(mutator.getFieldVectorMap());
      } while (batchRecordCount != 0);
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
            inner.getFilePath(),
            e.getMessage());
      } else {
        // write a more detailed error description row for copy_errors() table function output
        Preconditions.checkState(isValidationMode);
        String errorDescription =
            setupError != null
                ? String.format(
                    "Error encountered during reader setup in rowgroup %d: %s",
                    rowgroup, collapseExceptionMessages(e))
                : String.format(
                    "%s in rowgroup %d between the rows %d and %d",
                    e.getMessage(),
                    rowgroup,
                    totalRecordCount,
                    totalRecordCount + getNumRowsPerBatch());
        validationErrorWriter.write(null, null, null, errorDescription);
      }
      context.getStats().addLongStat(Metric.NUM_READERS_SKIPPED, 1);
      return 1;
    } finally {
      context
          .getStats()
          .addLongStat(Metric.DRY_RUN_READ_TIME_NS, System.nanoTime() - startDryRunNs);
    }

    // if we made it here, there's no error to report
    return 0;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(validationContainer);
    super.close();
  }
}
