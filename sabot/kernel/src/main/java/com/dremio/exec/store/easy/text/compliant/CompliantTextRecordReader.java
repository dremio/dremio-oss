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
package com.dremio.exec.store.easy.text.compliant;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties.PropertyKey;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.Builder;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.CopyIntoFileState;
import com.dremio.exec.physical.config.copyinto.CopyIntoHistoryExtendedProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties.OnErrorOption;
import com.dremio.exec.physical.config.copyinto.IngestionProperties;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.exec.store.dfs.copyinto.CopyIntoExceptionUtils;
import com.dremio.exec.store.dfs.easy.ExtendedEasyReaderProperties;
import com.dremio.exec.store.easy.text.compliant.TextReader.RecordReaderStatus;
import com.dremio.exec.tablefunctions.copyerrors.ValidationErrorRowWriter;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator.Metric;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.Maps;
import com.univocity.parsers.common.TextParsingException;
import io.protostuff.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapred.FileSplit;

// New text reader, complies with the RFC 4180 standard for text/csv files
public class CompliantTextRecordReader extends AbstractRecordReader {

  private enum RecordBatchReadingStatus {
    PRE_VALIDATION,
    SKIPPING_FILE,
    NORMAL;
  }

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CompliantTextRecordReader.class);

  private static final int READ_BUFFER = 1024 * 1024;
  private static final int WHITE_SPACE_BUFFER = 64 * 1024;

  // settings to be used while parsing
  private final TextParsingSettings settings;
  // Chunk of the file to be read by this reader
  private FileSplit split;
  // text reader implementation
  private TextReader reader;
  private TextReader preValidatorReader;
  private final CompressionCodecFactory codecFactory;
  private final FileSystem dfs;

  private boolean schemaImposedMode;
  private ExtendedFormatOptions extendedFormatOptions;
  private CopyIntoQueryProperties copyIntoQueryProperties;
  private SimpleQueryContext queryContext;
  private IngestionProperties ingestionProperties;

  private int linesToSkip;

  // copy_errors
  private boolean isValidationMode = false;
  private BatchSchema validatedTableSchema = null;
  private String originalJobId = null;
  private ValueVector[] validationResult;
  private int validationErrorCountInBatch;
  private String setupError = null;
  private boolean isSetupErrorWritten = false;
  private ValidationErrorRowWriter validationErrorRowWriter = null;
  private final String filePathForError;
  private boolean onErrorHandlingRequired = false;
  private RecordBatchReadingStatus recordBatchReadingStatus = RecordBatchReadingStatus.NORMAL;
  private final List<AutoCloseable> closeables = new ArrayList<>();
  private boolean isFileLoadEventRecorded = false;
  private long processingStartTime;

  public CompliantTextRecordReader(
      FileSplit split,
      CompressionCodecFactory codecFactory,
      FileSystem dfs,
      OperatorContext context,
      TextParsingSettings settings,
      List<SchemaPath> columns) {
    super(context, columns);
    this.split = split;
    this.settings = settings;
    this.codecFactory = codecFactory;
    this.dfs = dfs;
    this.linesToSkip = settings.getSkipLines();

    // We don't care about the URI schema but the actual path only in the errors
    this.filePathForError = split.getPath().toUri().getPath();
  }

  public CompliantTextRecordReader(
      FileSplit split,
      CompressionCodecFactory codecFactory,
      FileSystem dfs,
      OperatorContext context,
      TextParsingSettings settings,
      List<SchemaPath> columns,
      ExtendedEasyReaderProperties properties,
      ByteString extendedProperties) {
    this(split, codecFactory, dfs, context, settings, columns);
    if (properties != null) {
      this.schemaImposedMode = properties.isSchemaImposed();
      this.extendedFormatOptions = properties.getExtendedFormatOptions();
      Optional<CopyIntoExtendedProperties> copyIntoExtendedPropertiesOptional =
          CopyIntoExtendedProperties.Util.getProperties(extendedProperties);
      if (copyIntoExtendedPropertiesOptional.isPresent()) {
        CopyIntoExtendedProperties copyIntoExtendedProperties =
            copyIntoExtendedPropertiesOptional.get();
        this.copyIntoQueryProperties =
            copyIntoExtendedProperties.getProperty(
                CopyIntoExtendedProperties.PropertyKey.COPY_INTO_QUERY_PROPERTIES,
                CopyIntoQueryProperties.class);
        this.queryContext =
            copyIntoExtendedProperties.getProperty(
                CopyIntoExtendedProperties.PropertyKey.QUERY_CONTEXT, SimpleQueryContext.class);

        CopyIntoHistoryExtendedProperties copyIntoHistoryExtendedProperties =
            copyIntoExtendedProperties.getProperty(
                PropertyKey.COPY_INTO_HISTORY_PROPERTIES, CopyIntoHistoryExtendedProperties.class);
        this.ingestionProperties =
            copyIntoExtendedProperties.getProperty(
                CopyIntoExtendedProperties.PropertyKey.INGESTION_PROPERTIES,
                IngestionProperties.class);

        if (copyIntoHistoryExtendedProperties != null) {
          this.isValidationMode = true;
          this.validatedTableSchema = copyIntoHistoryExtendedProperties.getValidatedTableSchema();
          this.originalJobId = copyIntoHistoryExtendedProperties.getOriginalJobId();
        }
      }
    }

    OnErrorOption onErrorOption =
        copyIntoQueryProperties == null ? null : copyIntoQueryProperties.getOnErrorOption();
    this.onErrorHandlingRequired =
        this.schemaImposedMode
            && this.copyIntoQueryProperties != null
            && (onErrorOption == OnErrorOption.CONTINUE
                || onErrorOption == OnErrorOption.SKIP_FILE);
  }

  // checks to see if we are querying all columns(star) or individual columns
  @Override
  public boolean isStarQuery() {
    if (settings.isUseRepeatedVarChar()) {
      return super.isStarQuery()
          || getColumns().stream().anyMatch(path -> path.equals(RepeatedVarCharOutput.COLUMNS));
    }
    return super.isStarQuery();
  }

  /**
   * Performs the initial setup required for the record reader. Initializes the input stream,
   * handling of the output record batch and the actual reader to be used.
   *
   * @param outputMutator Used to create the schema in the output record batch
   * @throws ExecutionSetupException
   */
  @Override
  public void setup(OutputMutator outputMutator) throws ExecutionSetupException {
    processingStartTime = System.currentTimeMillis();
    // setup Output, Input, and Reader
    TextInput input = null;
    final TextOutput output;
    final int sizeLimit =
        Math.toIntExact(this.context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
    try {
      try {
        // setup Input using InputStream
        input = createInput();
        // Setup a separate input for pre-validation
        TextInput inputForPreValidation =
            !isValidationMode
                    && copyIntoQueryProperties != null
                    && copyIntoQueryProperties.getOnErrorOption() == OnErrorOption.SKIP_FILE
                ? createInput()
                : null;

        if (isSkipQuery()) {
          if (settings.isHeaderExtractionEnabled()) {
            extractHeader();
          }
          // When no columns are projected try to make the parser do less work by turning off
          // options that have extra cost
          settings.setIgnoreLeadingWhitespaces(false);
          settings.setIgnoreTrailingWhitespaces(false);
          output = new TextCountOutput();
        } else {
          // setup Output using OutputMutator
          if (schemaImposedMode) {
            setupForValidationMode(outputMutator);
            // if header extraction is disabled, use positional column mode
            if (settings.isHeaderExtractionEnabled()) {
              String[] fieldNames = extractHeader();
              try {
                output =
                    new SchemaImposedOutput(
                        outputMutator,
                        fieldNames,
                        sizeLimit,
                        extendedFormatOptions,
                        filePathForError,
                        isValidationMode,
                        validatedTableSchema,
                        validationErrorRowWriter,
                        onErrorHandlingRequired);
              } catch (SchemaMismatchException e) {
                throw UserException.dataReadError()
                    .message(CopyIntoExceptionUtils.redactMessage(e.getMessage()))
                    .buildSilently();
              }
            } else {
              output =
                  new SchemaImposedOutput(
                      outputMutator,
                      sizeLimit,
                      extendedFormatOptions,
                      isValidationMode,
                      validatedTableSchema,
                      validationErrorRowWriter);
            }
          } else if (settings.isHeaderExtractionEnabled()) {
            // extract header and use that to setup a set of VarCharVectors
            String[] fieldNames = extractHeader();
            output =
                new FieldVarCharOutput(
                    outputMutator, fieldNames, getColumns(), isStarQuery(), sizeLimit);
            output.init();
          } else if (settings.isAutoGenerateColumnNames()) {
            String[] fieldNames = generateColumnNames();
            output =
                new FieldVarCharOutput(
                    outputMutator, fieldNames, getColumns(), isStarQuery(), sizeLimit);
            output.init();
          } else {
            // simply use RepeatedVarCharVector
            output =
                new RepeatedVarCharOutput(outputMutator, getColumns(), isStarQuery(), sizeLimit);
            output.init();
          }
        }

        if (inputForPreValidation == null) {
          reader =
              createReader(
                  input, output, copyIntoQueryProperties, ingestionProperties, processingStartTime);
        } else {
          preValidatorReader =
              createReader(
                  inputForPreValidation,
                  output,
                  copyIntoQueryProperties,
                  ingestionProperties,
                  processingStartTime);
          preValidatorReader.start();
          // In case of pre-validation the normal reader is similar to the "abort" option
          reader =
              createReader(
                  input, output, copyIntoQueryProperties, ingestionProperties, processingStartTime);
          recordBatchReadingStatus = RecordBatchReadingStatus.PRE_VALIDATION;
        }
        reader.start();
      } catch (Throwable e) {
        if (isValidationMode || onErrorHandlingRequired) {
          this.setupError = CopyIntoExceptionUtils.redactException(e).getMessage();
          if (reader == null || !(reader.getOutput() instanceof SchemaImposedOutput)) {
            SchemaImposedOutput imposedOutput =
                new SchemaImposedOutput(
                    outputMutator,
                    sizeLimit,
                    extendedFormatOptions,
                    isValidationMode,
                    validatedTableSchema,
                    validationErrorRowWriter);
            reader =
                createReader(
                    input,
                    imposedOutput,
                    copyIntoQueryProperties,
                    ingestionProperties,
                    processingStartTime);
          }
        } else {
          throw e;
        }
      }
    } catch (IOException e) {
      Throwable t = e.getCause();
      if (t instanceof StreamFinishedPseudoException) {
        return;
      }
      String bestEffortMessage = bestEffortMessageForUnknownException(t);
      if (bestEffortMessage != null) {
        throw new ExecutionSetupException(bestEffortMessage);
      }
      throw new ExecutionSetupException(
          String.format("Failure while setting up text reader for file %s", split.getPath()), e);
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(
          String.format("Failure while setting up text reader for file %s", split.getPath()), e);
    } catch (IllegalArgumentException e) {
      throw UserException.dataReadError(e)
          .addContext("File Path", split.getPath().toString())
          .build(logger);
    }
  }

  private TextInput createInput() throws IOException {
    ArrowBuf readBuffer = closeLater(this.context.getAllocator().buffer(READ_BUFFER));
    FSInputStream stream =
        FileSystemUtils.openPossiblyCompressedStream(
            codecFactory, dfs, Path.of(split.getPath().toUri()));
    return new TextInput(
        settings, stream, readBuffer, split.getStart(), split.getStart() + split.getLength());
  }

  private TextReader createReader(
      TextInput input,
      TextOutput output,
      CopyIntoQueryProperties copyIntoQueryProperties,
      IngestionProperties ingestionProperties,
      long processingStartTime) {
    ArrowBuf whitespaceBuffer = closeLater(this.context.getAllocator().buffer(WHITE_SPACE_BUFFER));
    return closeLater(
        new TextReader(
            settings,
            input,
            output,
            whitespaceBuffer,
            filePathForError,
            split.getLength(),
            schemaImposedMode,
            copyIntoQueryProperties,
            ingestionProperties,
            queryContext,
            isValidationMode,
            processingStartTime));
  }

  private void setupForValidationMode(OutputMutator outputMutator) {
    if (isValidationMode) {
      BatchSchema validationResultSchema = outputMutator.getContainer().getSchema();
      this.validationResult = new ValueVector[validationResultSchema.getTotalFieldCount()];
      int fieldIx = 0;
      for (Field f : validationResultSchema) {
        validationResult[fieldIx++] = outputMutator.getVector(f.getName());
      }
      this.validationErrorRowWriter =
          ValidationErrorRowWriter.newVectorWriter(
              validationResult,
              filePathForError,
              originalJobId,
              () -> validationErrorCountInBatch++);
    }
  }

  private String[] readFirstLineForColumnNames()
      throws ExecutionSetupException, SchemaChangeException, IOException {
    // setup Output using OutputMutator
    // we should use a separate output mutator to avoid reshaping query output with header data
    try (HeaderOutputMutator hOutputMutator = new HeaderOutputMutator();
        ArrowBuf readBufferInReader = this.context.getAllocator().buffer(READ_BUFFER);
        ArrowBuf whitespaceBufferInReader =
            this.context.getAllocator().buffer(WHITE_SPACE_BUFFER)) {
      final int sizeLimit =
          Math.toIntExact(
              this.context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
      final RepeatedVarCharOutput hOutput =
          new RepeatedVarCharOutput(hOutputMutator, getColumns(), true, sizeLimit);
      hOutput.init();
      this.allocate(hOutputMutator.fieldVectorMap);

      // setup Input using InputStream
      // we should read file header irrespective of split given to this reader
      FSInputStream hStream =
          FileSystemUtils.openPossiblyCompressedStream(
              codecFactory, dfs, Path.of(split.getPath().toUri()));
      TextInput hInput =
          new TextInput(
              settings, hStream, readBufferInReader, 0, Math.min(READ_BUFFER, split.getLength()));
      // setup Reader using Input and Output
      this.reader =
          new TextReader(settings, hInput, hOutput, whitespaceBufferInReader, isValidationMode);

      String[] fieldNames;
      try {
        reader.start();

        // extract first non-empty row
        do {
          TextReader.RecordReaderStatus status = reader.parseNext();
          if (TextReader.RecordReaderStatus.SUCCESS != status) {
            // end of file most likely
            throw new IOException(StreamFinishedPseudoException.INSTANCE);
          }

          // grab the field names from output
          fieldNames = hOutput.getTextOutput();
        } while (fieldNames == null);

        if (settings.isTrimHeader()) {
          for (int i = 0; i < fieldNames.length; i++) {
            fieldNames[i] = fieldNames[i].trim();
          }
        }
        return fieldNames;
      } finally {
        hOutput.close();
        // cleanup and set to skip the first line next time we read input
        reader.close();
      }
    }
  }

  /**
   * This method is responsible to implement logic for extracting header from text file Currently it
   * is assumed to be first line if headerExtractionEnabled is set to true TODO: enhance to support
   * more common header patterns
   *
   * @return field name strings
   */
  private String[] extractHeader()
      throws SchemaChangeException, IOException, ExecutionSetupException {
    assert (settings.isHeaderExtractionEnabled());

    // don't skip header in case skipFirstLine is set true
    settings.setSkipFirstLine(false);
    final String[] fieldNames = readFirstLineForColumnNames();
    settings.setSkipFirstLine(true);

    if (schemaImposedMode) {
      // if the continue option is set decorate the original schema with an error column
      if (onErrorHandlingRequired) {
        String[] fieldNamesWithError = new String[fieldNames.length + 1];
        System.arraycopy(fieldNames, 0, fieldNamesWithError, 0, fieldNames.length);
        fieldNamesWithError[fieldNames.length] = ColumnUtils.COPY_HISTORY_COLUMN_NAME;
        return fieldNamesWithError;
      }
      // return original field names without modification
      return fieldNames;
    }
    return validateColumnNames(fieldNames);
  }

  public static String[] validateColumnNames(String[] fieldNames) {
    final Map<String, Integer> uniqueFieldNames = Maps.newHashMap();
    if (fieldNames != null) {
      for (int i = 0; i < fieldNames.length; ++i) {
        if (fieldNames[i].isEmpty()) {
          fieldNames[i] = TextColumnNameGenerator.columnNameForIndex(i);
        }
        // If we have seen this column name before, add a suffix
        final Integer count = uniqueFieldNames.get(fieldNames[i]);
        if (count != null) {
          uniqueFieldNames.put(fieldNames[i], count + 1);
          fieldNames[i] = fieldNames[i] + count;
        } else {
          uniqueFieldNames.put(fieldNames[i], 0);
        }
      }
      return fieldNames;
    }
    return null;
  }

  /**
   * Generate fields names per column in text file. Read first line and count columns and return
   * fields names like excel sheet. A, B, C and so on.
   *
   * @return field name strings, null if no records found in text file.
   */
  private String[] generateColumnNames()
      throws SchemaChangeException, IOException, ExecutionSetupException {
    assert (settings.isAutoGenerateColumnNames());

    final boolean shouldSkipFirstLine = settings.isSkipFirstLine();
    settings.setSkipFirstLine(false);
    final String[] columns = readFirstLineForColumnNames();
    settings.setSkipFirstLine(shouldSkipFirstLine);
    if (columns != null && columns.length > 0) {
      String[] fieldNames = new String[columns.length];
      for (int i = 0; i < columns.length; ++i) {
        fieldNames[i] = TextColumnNameGenerator.columnNameForIndex(i);
      }
      return fieldNames;
    } else {
      return null;
    }
  }

  /**
   * Generates the next record batch
   *
   * @return number of records in the batch
   */
  @Override
  public int next() {
    // If there was an error during we need to write it as first and only validation error record
    if (!StringUtils.isEmpty(setupError)) {
      if (isSetupErrorWritten) {
        return 0;
      }
      writeSetupError();
      reader.finishBatch();
      isSetupErrorWritten = true;
      return 1;
    }

    try {
      // The first call of next for SKIP_FILE
      if (recordBatchReadingStatus == RecordBatchReadingStatus.PRE_VALIDATION) {
        long startDryRunNs = System.nanoTime();
        long count = 0;
        preValidatorReader.resetForNextBatch();
        RecordReaderStatus status;
        while ((status = preValidatorReader.parseNext()) == RecordReaderStatus.SUCCESS) {
          ++count;
          if (count >= numRowsPerBatch) {
            // We need to reset the reader per record batch to not to overflow the vectors or get
            // OoO for additional allocations
            preValidatorReader.finishBatch();
            preValidatorReader.resetForNextBatch();
            count = 0;
          }
        }
        context
            .getStats()
            .addLongStat(Metric.DRY_RUN_READ_TIME_NS, System.nanoTime() - startDryRunNs);
        if (status != RecordReaderStatus.END) {
          // The errorInfo is written at the parseNext call after the error
          preValidatorReader.parseNext();

          recordBatchReadingStatus = RecordBatchReadingStatus.SKIPPING_FILE;
          context.getStats().addLongStat(Metric.NUM_READERS_SKIPPED, 1);
          // Skipping the file: one errorInfo is written to the first row
          return 1;
        } else {
          recordBatchReadingStatus = RecordBatchReadingStatus.NORMAL;
        }
      }

      // The second call of next for SKIP_FILE in case of error
      if (recordBatchReadingStatus == RecordBatchReadingStatus.SKIPPING_FILE) {
        return 0;
      }

      reader.resetForNextBatch();
      long recordCount = 0;
      int validationErrorCount = 0;

      // We return a maximum of configured batch size worth of records. In normal mode these are
      // records from the source
      // file. In copy_errors validation mode these are validation error records, so we may traverse
      // in the source file
      // as long as we saw a batch worth of validation errors (or EOF), thus in this case (source)
      // recordCount can go
      // beyond batch size.
      loop:
      while (isValidationMode
          ? validationErrorCount < numRowsPerBatch
          : recordCount < numRowsPerBatch) {
        TextReader.RecordReaderStatus status = reader.parseNext();
        switch (status) {
          case SUCCESS:
            recordCount++;
            break;
          case VALIDATION_ERROR:
            validationErrorCount++;
            break;
          case ERROR:
            recordCount++;
            break loop;
          case END:
            if (!isFileLoadEventRecorded) {
              recordCount += reader.writeSuccessfulParseEvent();
              isFileLoadEventRecorded = true;
            }
            break loop;
          case SKIP:
            continue;
          default:
            throw UserException.parseError()
                .message("Unrecognized read state %s while parsing text file", status.name())
                .buildSilently();
        }
      }
      reader.finishBatch();
      validationErrorCountInBatch = 0;
      if (isValidationMode) {
        return validationErrorCount;
      } else {
        return (int) recordCount;
      }
    } catch (IOException | TextParsingException e) {
      throw UserException.dataReadError(e)
          .addContext(
              "Failure while reading file %s. Happened at or shortly before byte position %d.",
              split.getPath(), reader.getPos())
          .build(logger);
    }
  }

  /**
   * Cleanup state once we are finished processing all the records. This would internally close the
   * input stream we are reading from.
   */
  @Override
  public void close() throws Exception {
    try {
      AutoCloseables.close(closeables);
    } finally {
      reader = null;
    }
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    int estimatedRecordCount;
    if ((reader != null) && (reader.getInput() != null) && (vectorMap.size() > 0)) {
      final OptionManager options = context.getOptions();
      final int listSizeEstimate = (int) options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
      final int varFieldSizeEstimate =
          (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
      final int estimatedRecordSize =
          BatchSchema.estimateRecordSize(vectorMap, listSizeEstimate, varFieldSizeEstimate);
      if (estimatedRecordSize > 0) {
        if (schemaImposedMode && reader.getInput().length < 0 && estimatedRecordSize == 1) {
          estimatedRecordCount = 0;
        } else {
          estimatedRecordCount =
              (int) Math.min(reader.getInput().length / estimatedRecordSize, numRowsPerBatch);
        }
      } else {
        estimatedRecordCount = (int) numRowsPerBatch;
      }
    } else {
      estimatedRecordCount = (int) numRowsPerBatch;
    }
    for (final ValueVector v : vectorMap.values()) {
      v.setInitialCapacity(estimatedRecordCount);
      v.allocateNew();
    }
  }

  /**
   * Writes setup error information.
   *
   * <p>This method checks if error handling is required and if the schema-imposed mode is enabled
   * and the reader's output is an instance of SchemaImposedOutput. If so, it retrieves the index of
   * the file history column and writes setup error information to the output. If error handling is
   * not required, the setup error information is written using the validation error row writer.
   */
  private void writeSetupError() {
    if (onErrorHandlingRequired) {
      if (schemaImposedMode && reader.getOutput() instanceof SchemaImposedOutput) {
        SchemaImposedOutput schemaImposedOutput = (SchemaImposedOutput) reader.getOutput();
        OptionalInt fileHistoryColIndex = schemaImposedOutput.getFileHistoryColIndex();
        if (fileHistoryColIndex.isPresent()) {
          schemaImposedOutput.startField(fileHistoryColIndex.getAsInt());
          Builder builder =
              new Builder(
                      queryContext.getQueryId(),
                      queryContext.getUserName(),
                      queryContext.getTableNamespace(),
                      copyIntoQueryProperties.getStorageLocation(),
                      filePathForError,
                      extendedFormatOptions,
                      FileType.CSV.name(),
                      CopyIntoFileState.SKIPPED)
                  .setRecordsLoadedCount(0)
                  .setRecordsRejectedCount(0)
                  .setRecordDelimiter(new String(settings.getNewLineDelimiter()))
                  .setFieldDelimiter(new String(settings.getDelimiter()))
                  .setQuoteChar(new String(settings.getQuote()))
                  .setEscapeChar(new String(settings.getQuoteEscape()))
                  .setBranch(copyIntoQueryProperties.getBranch())
                  .setProcessingStartTime(processingStartTime)
                  .setFileSize(split.getLength())
                  .setFirstErrorMessage(setupError);

          if (ingestionProperties != null) {
            builder
                .setPipeId(ingestionProperties.getPipeId())
                .setPipeName(ingestionProperties.getPipeName())
                .setFileNotificationTimestamp(ingestionProperties.getNotificationTimestamp())
                .setIngestionSourceType(ingestionProperties.getIngestionSourceType())
                .setRequestId(ingestionProperties.getRequestId());
          }

          String infoJson = FileLoadInfo.Util.getJson(builder.build());

          for (byte b : infoJson.getBytes()) {
            schemaImposedOutput.append(b);
          }
          schemaImposedOutput.endHistoryEventField();
          schemaImposedOutput.finishRecord();
        }
      }
    } else {
      validationErrorRowWriter.write(null, 0L, 1L, setupError);
    }
  }

  private <T extends AutoCloseable> T closeLater(T closeable) {
    closeables.add(closeable);
    return closeable;
  }

  /**
   * TextRecordReader during its first phase read to extract header should pass its own
   * OutputMutator to avoid reshaping query output. This class provides OutputMutator for header
   * extraction.
   */
  private class HeaderOutputMutator implements OutputMutator, AutoCloseable {
    private final Map<String, ValueVector> fieldVectorMap = Maps.newHashMap();

    @Override
    public <T extends ValueVector> T addField(Field field, Class<T> clazz)
        throws SchemaChangeException {
      ValueVector v = fieldVectorMap.get(field.getName().toLowerCase());
      if (v == null || v.getClass() != clazz) {
        // Field does not exist add it to the map
        v = TypeHelper.getNewVector(field, context.getAllocator());
        if (!clazz.isAssignableFrom(v.getClass())) {
          throw new SchemaChangeException(
              String.format(
                  "Class %s was provided, expected %s.",
                  clazz.getSimpleName(), v.getClass().getSimpleName()));
        }
        v.allocateNew();
        fieldVectorMap.put(field.getName().toLowerCase(), v);
      }
      return clazz.cast(v);
    }

    @Override
    public ValueVector getVector(String name) {
      return fieldVectorMap.get((name != null) ? name.toLowerCase() : name);
    }

    @Override
    public Collection<ValueVector> getVectors() {
      return fieldVectorMap.values();
    }

    @Override
    public void allocate(int recordCount) {
      // do nothing for now
    }

    @Override
    public ArrowBuf getManagedBuffer() {
      return context.getManagedBuffer();
    }

    @Override
    public CallBack getCallBack() {
      return null;
    }

    @Override
    public boolean getAndResetSchemaChanged() {
      return false;
    }

    @Override
    public boolean getSchemaChanged() {
      return false;
    }

    /**
     * Since this OutputMutator is passed by TextRecordReader to get the header out the mutator
     * might not get cleaned up elsewhere. TextRecordReader will call this method to clear any
     * allocations
     */
    @Override
    public void close() {
      for (final ValueVector v : fieldVectorMap.values()) {
        v.clear();
      }
      fieldVectorMap.clear();
    }
  }

  @Override
  public boolean supportsSkipAllQuery() {
    return true;
  }
}
