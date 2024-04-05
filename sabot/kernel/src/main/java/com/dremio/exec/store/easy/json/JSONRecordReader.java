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
package com.dremio.exec.store.easy.json;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.Builder;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo.CopyIntoFileState;
import com.dremio.exec.physical.config.copyinto.CopyIntoHistoryExtendedProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties.OnErrorOption;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.dfs.FileLoadInfo.Util;
import com.dremio.exec.store.dfs.easy.ExtendedEasyReaderProperties;
import com.dremio.exec.store.easy.json.JsonProcessor.ReadState;
import com.dremio.exec.store.easy.json.reader.CountingJsonReader;
import com.dremio.exec.tablefunctions.copyerrors.ValidationErrorRowWriter;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.vector.complex.fn.JsonReader;
import com.dremio.exec.vector.complex.fn.JsonReaderIOException;
import com.dremio.exec.vector.complex.fn.TransformationException;
import com.dremio.exec.vector.complex.fn.WorkingBuffer;
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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;

public class JSONRecordReader extends AbstractRecordReader {

  private enum RecordBatchReadingStatus {
    PRE_VALIDATION,
    SKIPPING_FILE,
    NORMAL;
  }

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(JSONRecordReader.class);

  private final OperatorContext context;
  private boolean enableAllTextMode;
  private boolean readNumbersAsDouble;
  private boolean schemaImposedMode;
  private ExtendedFormatOptions extendedFormatOptions;
  private CopyIntoQueryProperties copyIntoQueryProperties;
  private SimpleQueryContext queryContext;

  // Data we're consuming
  private final Path fsPath;
  private final JsonNode embeddedContent;

  private final CompressionCodecFactory codecFactory;
  private final FileSystem fileSystem;

  private VectorContainerWriter writer;
  private JsonProcessor jsonReader;
  private JsonProcessor preValidatorJsonReader;
  private int recordCount;
  private long runningRecordCount = 0;

  private FSInputStream stream;

  // copy_errors
  private boolean isValidationMode = false;
  private BatchSchema validatedTableSchema = null;
  private String originalJobId = null;
  private ValueVector[] validationResult;
  private ValidationErrorRowWriter validationErrorRowWriter = null;
  private final String filePathForError;
  private RecordBatchReadingStatus recordBatchReadingStatus = RecordBatchReadingStatus.NORMAL;
  private boolean isFileLoadEventRecorded = false;
  private String setupError = null;
  private boolean isSetupErrorWritten = false;
  private boolean onErrorHandlingRequired = false;

  /**
   * Create a JSON Record Reader that uses a file based input stream.
   *
   * @param context
   * @param inputPath
   * @param codecFactory
   * @param fileSystem
   * @param columns pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public JSONRecordReader(
      final OperatorContext context,
      final String inputPath,
      final CompressionCodecFactory codecFactory,
      final FileSystem fileSystem,
      final List<SchemaPath> columns)
      throws OutOfMemoryException {
    this(context, inputPath, null, codecFactory, fileSystem, columns);
  }

  @Override
  public String getFilePath() {
    return fsPath != null ? fsPath.toString() : "";
  }

  /**
   * Create a new JSON Record Reader that uses a in memory materialized JSON stream.
   *
   * @param context
   * @param embeddedContent
   * @param codecFactory
   * @param fileSystem
   * @param columns pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public JSONRecordReader(
      final OperatorContext context,
      final JsonNode embeddedContent,
      final CompressionCodecFactory codecFactory,
      final FileSystem fileSystem,
      final List<SchemaPath> columns)
      throws OutOfMemoryException {
    this(context, null, embeddedContent, codecFactory, fileSystem, columns);
  }

  private JSONRecordReader(
      final OperatorContext operatorContext,
      final String inputPath,
      final JsonNode embeddedContent,
      final CompressionCodecFactory codecFactory,
      final FileSystem fileSystem,
      final List<SchemaPath> columns) {
    super(operatorContext, columns);

    Preconditions.checkArgument(
        (inputPath == null && embeddedContent != null)
            || (inputPath != null && embeddedContent == null),
        "One of inputPath or embeddedContent must be set but not both.");

    if (inputPath != null) {
      this.fsPath = Path.of(inputPath);
      this.embeddedContent = null;
    } else {
      this.embeddedContent = embeddedContent;
      this.fsPath = null;
    }

    this.codecFactory = codecFactory;
    this.fileSystem = fileSystem;
    this.context = operatorContext;

    // only enable all text mode if we aren't using embedded content mode.
    final OptionManager options = operatorContext.getOptions();
    this.enableAllTextMode =
        embeddedContent == null
            && options.getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR);
    this.readNumbersAsDouble =
        embeddedContent == null
            && options.getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR);

    // We don't care about the URI schema but the actual path only in the errors
    this.filePathForError = fsPath == null ? "" : fsPath.toURI().getPath();
  }

  public JSONRecordReader(
      final OperatorContext operatorContext,
      final String inputPath,
      final CompressionCodecFactory codecFactory,
      final FileSystem fileSystem,
      final List<SchemaPath> columns,
      final ExtendedEasyReaderProperties properties,
      final ByteString extendedProperties) {
    this(operatorContext, inputPath, null, codecFactory, fileSystem, columns);
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
        this.onErrorHandlingRequired =
            schemaImposedMode
                && copyIntoQueryProperties != null
                && (copyIntoQueryProperties.getOnErrorOption() == OnErrorOption.SKIP_FILE
                    || copyIntoQueryProperties.getOnErrorOption() == OnErrorOption.CONTINUE);
        this.queryContext =
            copyIntoExtendedProperties.getProperty(
                CopyIntoExtendedProperties.PropertyKey.QUERY_CONTEXT, SimpleQueryContext.class);
        CopyIntoHistoryExtendedProperties copyIntoHistoryExtendedProperties =
            copyIntoExtendedProperties.getProperty(
                CopyIntoExtendedProperties.PropertyKey.COPY_INTO_HISTORY_PROPERTIES,
                CopyIntoHistoryExtendedProperties.class);

        if (copyIntoHistoryExtendedProperties != null) {
          this.isValidationMode = true;
          this.validatedTableSchema = copyIntoHistoryExtendedProperties.getValidatedTableSchema();
          this.originalJobId = copyIntoHistoryExtendedProperties.getOriginalJobId();
        }
      }
    }
  }

  public void resetSpecialSchemaOptions() {
    this.enableAllTextMode = false;
    this.readNumbersAsDouble = false;
  }

  @Override
  public String toString() {
    return super.toString()
        + "[hadoopPath = "
        + fsPath
        + ", recordCount = "
        + recordCount
        + ", runningRecordCount = "
        + runningRecordCount
        + ", ...]";
  }

  @Override
  public void setup(final OutputMutator output) throws ExecutionSetupException {
    try {
      this.writer = new VectorContainerWriter(output);
      this.writer.setInitialCapacity(context.getTargetBatchSize());
      if (fsPath != null) {
        this.stream =
            FileSystemUtils.openPossiblyCompressedStream(codecFactory, fileSystem, fsPath);
      }
      if (isSkipQuery()) {
        this.jsonReader = new CountingJsonReader();
        setupParser(jsonReader);
      } else {
        final int sizeLimit =
            Math.toIntExact(
                this.context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
        final int maxLeafLimit =
            Math.toIntExact(
                this.context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
        setupForValidationMode(output);
        BatchSchema batchSchema =
            isValidationMode
                ? validatedTableSchema
                : (output.getContainer() != null && output.getContainer().hasSchema()
                    ? output.getContainer().getSchema()
                    : null);

        if (copyIntoQueryProperties != null
            && copyIntoQueryProperties.getOnErrorOption() == OnErrorOption.SKIP_FILE) {
          this.jsonReader =
              new JsonReader(
                  context.getManagedBuffer(),
                  ImmutableList.copyOf(getColumns()),
                  sizeLimit,
                  maxLeafLimit,
                  enableAllTextMode,
                  true,
                  readNumbersAsDouble,
                  schemaImposedMode,
                  extendedFormatOptions,
                  null,
                  queryContext,
                  context,
                  batchSchema,
                  context
                      .getOptions()
                      .getOption(PlannerSettings.ENFORCE_VALID_JSON_DATE_FORMAT_ENABLED),
                  filePathForError,
                  isValidationMode,
                  validationErrorRowWriter);
          this.preValidatorJsonReader =
              new JsonReader(
                  context.getManagedBuffer(),
                  ImmutableList.copyOf(getColumns()),
                  sizeLimit,
                  maxLeafLimit,
                  enableAllTextMode,
                  true,
                  readNumbersAsDouble,
                  schemaImposedMode,
                  extendedFormatOptions,
                  copyIntoQueryProperties,
                  queryContext,
                  context,
                  batchSchema,
                  context
                      .getOptions()
                      .getOption(PlannerSettings.ENFORCE_VALID_JSON_DATE_FORMAT_ENABLED),
                  filePathForError,
                  true,
                  validationErrorRowWriter);
          setupParser(preValidatorJsonReader);
          recordBatchReadingStatus = RecordBatchReadingStatus.PRE_VALIDATION;
        } else {
          this.jsonReader =
              new JsonReader(
                  context.getManagedBuffer(),
                  ImmutableList.copyOf(getColumns()),
                  sizeLimit,
                  maxLeafLimit,
                  enableAllTextMode,
                  true,
                  readNumbersAsDouble,
                  schemaImposedMode,
                  extendedFormatOptions,
                  copyIntoQueryProperties,
                  queryContext,
                  context,
                  batchSchema,
                  context
                      .getOptions()
                      .getOption(PlannerSettings.ENFORCE_VALID_JSON_DATE_FORMAT_ENABLED),
                  filePathForError,
                  isValidationMode,
                  validationErrorRowWriter);
          setupParser(jsonReader);
        }
      }
    } catch (final Exception e) {
      if (onErrorHandlingRequired) {
        setupError = e.getMessage();
        logger.debug(
            String.format(
                "Encountered error while setting up JsonRecordReader. JsonRecordReader is running in '%s' mode.",
                copyIntoQueryProperties.getOnErrorOption()),
            e);
      } else {
        String bestEffortMessage = bestEffortMessageForUnknownException(e.getCause());
        if (bestEffortMessage != null) {
          throw new ExecutionSetupException(bestEffortMessage);
        }
        handleAndRaise("Failure reading JSON file", e);
      }
    }
  }

  private void setupParser(JsonProcessor jsonProcessor) throws IOException {
    if (fsPath != null) {
      jsonProcessor.setSource(stream);
    } else {
      jsonProcessor.setSource(embeddedContent);
    }
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
              validationResult, filePathForError, originalJobId, () -> 0);
    }
  }

  protected void handleAndRaise(String suffix, Throwable e) throws UserException {
    if (e instanceof JsonReaderIOException) {
      e = e.getCause();
    }
    String message = e.getMessage();
    int columnNr = -1;
    int lineNr = -1;
    if (e instanceof JsonParseException) {
      final JsonParseException ex = (JsonParseException) e;
      message = ex.getOriginalMessage();
      columnNr = ex.getLocation().getColumnNr();
      lineNr = ex.getLocation().getLineNr();
    }

    StringBuilder errorMsgBuilder = new StringBuilder();
    errorMsgBuilder.append(String.format("%s - %s", suffix, message));
    UserException.Builder exceptionBuilder = UserException.dataReadError(e);
    if (columnNr > 0) {
      exceptionBuilder.pushContext("Column ", columnNr);
    }

    if (fsPath != null) {
      exceptionBuilder
          .pushContext("Record ", currentRecordNumberInFile())
          .pushContext("File ", filePathForError);
      if (e instanceof TransformationException) {
        lineNr = ((TransformationException) e).getLineNumber();
      }
      errorMsgBuilder.append(String.format(" File: %s", filePathForError));
      errorMsgBuilder.append(String.format(" Line: %s,", lineNr));
      errorMsgBuilder.append(String.format(" Record: %s", currentRecordNumberInFile()));
    }
    exceptionBuilder.message(errorMsgBuilder.toString());
    throw exceptionBuilder.build(logger);
  }

  private long currentRecordNumberInFile() {
    return runningRecordCount + recordCount + 1;
  }

  private ReadState readWrite(JsonProcessor jsonProcessor, boolean trackRecordCount)
      throws Exception {
    jsonProcessor.resetDataSizeCounter();
    writer.allocate();
    writer.reset();

    ReadState readState = null;
    while (recordCount < numRowsPerBatch) {
      writer.setPosition(recordCount);
      readState = jsonProcessor.write(writer);

      if (readState == ReadState.WRITE_SUCCEED) {
        if (!trackRecordCount) {
          continue;
        }
        recordCount++;
      } else if (readState == ReadState.VALIDATION_ERROR) {
        // for JSON we bail out at the first validation error
        recordCount = 1;
        break;
      } else if (!isFileLoadEventRecorded && readState == ReadState.END_OF_STREAM) {
        recordCount += jsonProcessor.writeSuccessfulParseEvent(writer);
        isFileLoadEventRecorded = true;
        break;
      } else {
        break;
      }

      // If we already reached the target batch size, end the batch.
      if (jsonProcessor.getDataSizeCounter() > numBytesPerBatch) {
        break;
      }
    }

    writer.setValueCount(recordCount);

    updateRunningCount();

    return readState;
  }

  /**
   * Writes setup error information to the configured writer. This method populates the writer with
   * details about the setup error, such as query information, user, table namespace, storage
   * location, file path, and error state. The information is written in JSON format to a specific
   * column in the writer's schema.
   *
   * @throws IOException If an I/O error occurs during the writing process.
   */
  private void writeSetupError() throws IOException {
    // Allocate space in the writer for the setup error data.
    writer.allocate();

    // Reset the writer position and set it to the current record count.
    writer.reset();
    writer.setPosition(recordCount);

    // Access the root struct writer for setting up the error information.
    StructWriter structWriter = writer.rootAsStruct();

    // Create a JSON representation of the setup error information.
    String infoJson =
        Util.getJson(
            new Builder(
                    queryContext.getQueryId(),
                    queryContext.getUserName(),
                    queryContext.getTableNamespace(),
                    copyIntoQueryProperties.getStorageLocation(),
                    filePathForError,
                    extendedFormatOptions,
                    FileType.JSON.name(),
                    CopyIntoFileState.SKIPPED)
                .setRecordsLoadedCount(0)
                .setRecordsRejectedCount(0L)
                .build());

    // Prepare a VarChar holder in the working buffer for the JSON data.
    WorkingBuffer workingBuffer = new WorkingBuffer(context.getManagedBuffer());
    final int size = workingBuffer.prepareVarCharHolder(infoJson);

    // Write the VarChar data to the specified column in the writer's schema.
    structWriter
        .varChar(ColumnUtils.COPY_HISTORY_COLUMN_NAME)
        .writeVarChar(0, size, workingBuffer.getBuf());

    // Update the value count in the writer.
    writer.setValueCount(++recordCount);
  }

  @Override
  public int next() {
    try {
      recordCount = 0;

      if (!StringUtils.isEmpty(setupError)) {
        if (isSetupErrorWritten) {
          return recordCount;
        }
        writeSetupError();
        isSetupErrorWritten = true;
        return recordCount;
      }

      // ON_ERROR='SKIP_FILE' and the first call of next()
      if (recordBatchReadingStatus == RecordBatchReadingStatus.PRE_VALIDATION) {
        long startDryRunNs = System.nanoTime();
        if (readWrite(preValidatorJsonReader, false) == ReadState.VALIDATION_ERROR) {
          recordBatchReadingStatus = RecordBatchReadingStatus.SKIPPING_FILE;
          context.getStats().addLongStat(Metric.NUM_READERS_SKIPPED, 1);
        } else {
          recordBatchReadingStatus = RecordBatchReadingStatus.NORMAL;
        }

        if (fsPath != null) {
          // The stream is already closed by the parser, so we need to open another one
          this.stream =
              FileSystemUtils.openPossiblyCompressedStream(codecFactory, fileSystem, fsPath);
          setupParser(jsonReader);
        }
        context
            .getStats()
            .addLongStat(Metric.DRY_RUN_READ_TIME_NS, System.nanoTime() - startDryRunNs);
      }
      if (recordBatchReadingStatus == RecordBatchReadingStatus.SKIPPING_FILE) {
        return recordCount;
      }
      readWrite(jsonReader, !isValidationMode);
    } catch (Exception e) {
      handleAndRaise("Error parsing JSON", e);
    }
    return recordCount;
  }

  private void updateRunningCount() {
    runningRecordCount += recordCount;
  }

  @Override
  public void close() throws Exception {
    if (stream != null) {
      stream.close();
    }
  }

  @Override
  public boolean supportsSkipAllQuery() {
    return true;
  }
}
