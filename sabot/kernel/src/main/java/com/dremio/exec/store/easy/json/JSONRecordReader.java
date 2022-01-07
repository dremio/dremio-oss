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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.easy.json.JsonProcessor.ReadState;
import com.dremio.exec.store.easy.json.reader.CountingJsonReader;
import com.dremio.exec.vector.complex.fn.JsonReader;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class JSONRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONRecordReader.class);

  private final OperatorContext context;
  private boolean enableAllTextMode;
  private boolean readNumbersAsDouble;

  // Data we're consuming
  private final Path fsPath;
  private final JsonNode embeddedContent;

  private final CompressionCodecFactory codecFactory;
  private final FileSystem fileSystem;

  private VectorContainerWriter writer;
  private JsonProcessor jsonReader;
  private int recordCount;
  private long runningRecordCount = 0;

  private InputStream stream;

  /**
   * Create a JSON Record Reader that uses a file based input stream.
   * @param context
   * @param inputPath
   * @param codecFactory
   * @param fileSystem
   * @param columns  pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public JSONRecordReader(
      final OperatorContext context,
      final String inputPath,
      final CompressionCodecFactory codecFactory,
      final FileSystem fileSystem,
      final List<SchemaPath> columns) throws OutOfMemoryException {
    this(context, inputPath, null, codecFactory, fileSystem, columns);
  }

  @Override
  public String getFilePath() {
    return fsPath.toString();
  }

  /**
   * Create a new JSON Record Reader that uses a in memory materialized JSON stream.
   * @param context
   * @param embeddedContent
   * @param codecFactory
   * @param fileSystem
   * @param columns  pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public JSONRecordReader(final OperatorContext context, final JsonNode embeddedContent,
      final CompressionCodecFactory codecFactory, final FileSystem fileSystem, final List<SchemaPath> columns)
      throws OutOfMemoryException {
    this(context, null, embeddedContent, codecFactory, fileSystem, columns);
  }

  private JSONRecordReader(final OperatorContext operatorContext,
                           final String inputPath,
                           final JsonNode embeddedContent,
                           final CompressionCodecFactory codecFactory,
                           final FileSystem fileSystem,
                           final List<SchemaPath> columns) {
    super(operatorContext, columns);

    Preconditions.checkArgument(
        (inputPath == null && embeddedContent != null) ||
        (inputPath != null && embeddedContent == null),
        "One of inputPath or embeddedContent must be set but not both."
        );

    if(inputPath != null) {
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
    this.enableAllTextMode = embeddedContent == null && options.getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR);
    this.readNumbersAsDouble = embeddedContent == null && options.getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR);
  }

  public void resetSpecialSchemaOptions() {
    this.enableAllTextMode = false;
    this.readNumbersAsDouble = false;
  }

  @Override
  public String toString() {
    return super.toString()
        + "[hadoopPath = " + fsPath
        + ", recordCount = " + recordCount
        + ", runningRecordCount = " + runningRecordCount + ", ...]";
  }

  @Override
  public void setup(final OutputMutator output) throws ExecutionSetupException {
    try{
      if (fsPath != null) {
        this.stream = FileSystemUtils.openPossiblyCompressedStream(codecFactory, fileSystem, fsPath);
      }

      this.writer = new VectorContainerWriter(output);
      this.writer.setInitialCapacity(context.getTargetBatchSize());
      if (isSkipQuery()) {
        this.jsonReader = new CountingJsonReader();
      } else {
        final int sizeLimit = Math.toIntExact(this.context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
        final int maxLeafLimit = Math.toIntExact(this.context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
        this.jsonReader = new JsonReader(
          context.getManagedBuffer(), ImmutableList.copyOf(getColumns()), sizeLimit, maxLeafLimit, enableAllTextMode, true, readNumbersAsDouble);
      }
      setupParser();
    } catch(final Exception e) {
      String bestEffortMessage = bestEffortMessageForUnknownException(e.getCause());
      if (bestEffortMessage != null) {
        throw new ExecutionSetupException(bestEffortMessage);
      }
      handleAndRaise("Failure reading JSON file", e);
    }
  }

  private void setupParser() throws IOException {
    if(fsPath != null){
      jsonReader.setSource(stream);
    }else{
      jsonReader.setSource(embeddedContent);
    }
  }

  protected void handleAndRaise(String suffix, Exception e) throws UserException {

    String message = e.getMessage();
    int columnNr = -1;

    if (e instanceof JsonParseException) {
      final JsonParseException ex = (JsonParseException) e;
      message = ex.getOriginalMessage();
      columnNr = ex.getLocation().getColumnNr();
    }

    UserException.Builder exceptionBuilder = UserException.dataReadError(e)
            .message("%s - %s", suffix, message);
    if (columnNr > 0) {
      exceptionBuilder.pushContext("Column ", columnNr);
    }

    if (fsPath != null) {
      exceptionBuilder.pushContext("Record ", currentRecordNumberInFile())
          .pushContext("File ", fsPath.toURI().getPath());
    }

    throw exceptionBuilder.build(logger);
  }

  private long currentRecordNumberInFile() {
    return runningRecordCount + recordCount + 1;
  }

  @Override
  public int next() {
    jsonReader.resetDataSizeCounter();
    writer.allocate();
    writer.reset();

    recordCount = 0;
    ReadState write = null;
//    Stopwatch p = new Stopwatch().start();
    try{
      outside: while(recordCount < numRowsPerBatch) {
        writer.setPosition(recordCount);
        write = jsonReader.write(writer);

        if(write == ReadState.WRITE_SUCCEED) {
//          logger.debug("Wrote record.");
          recordCount++;
        }else{
//          logger.debug("Exiting.");
          break outside;
        }

        // If we already reached the target batch size, end the batch.
        if (jsonReader.getDataSizeCounter() > numBytesPerBatch) {
          break outside;
        }
      }

      writer.setValueCount(recordCount);
//      p.stop();
//      System.out.println(String.format("Wrote %d records in %dms.", recordCount, p.elapsed(TimeUnit.MILLISECONDS)));

      updateRunningCount();
      return recordCount;

    } catch (final Exception e) {
      handleAndRaise("Error parsing JSON", e);
    }
    // this is never reached
    return 0;
  }

  private void updateRunningCount() {
    runningRecordCount += recordCount;
  }

  @Override
  public void close() throws Exception {
    if(stream != null) {
      stream.close();
    }
  }

  @Override
  public boolean supportsSkipAllQuery(){
    return true;
  }

}
