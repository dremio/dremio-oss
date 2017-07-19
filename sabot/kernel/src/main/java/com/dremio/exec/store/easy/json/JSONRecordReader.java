/*
 * Copyright (C) 2017 Dremio Corporation
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
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.easy.json.JsonProcessor.ReadState;
import com.dremio.exec.store.easy.json.reader.CountingJsonReader;
import com.dremio.exec.vector.complex.fn.JsonReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class JSONRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONRecordReader.class);

  private VectorContainerWriter writer;

  // Data we're consuming
  private Path hadoopPath;
  private JsonNode embeddedContent;
  private InputStream stream;
  private final FileSystemWrapper fileSystem;
  private JsonProcessor jsonReader;
  private int recordCount;
  private long runningRecordCount = 0;
  private final OperatorContext context;
  private final boolean enableAllTextMode;
  private final boolean readNumbersAsDouble;
  private final boolean unionEnabled;

  /**
   * Create a JSON Record Reader that uses a file based input stream.
   * @param fragmentContext
   * @param inputPath
   * @param fileSystem
   * @param columns  pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public JSONRecordReader(final OperatorContext context, final String inputPath, final FileSystemWrapper fileSystem,
      final List<SchemaPath> columns) throws OutOfMemoryException {
    this(context, inputPath, null, fileSystem, columns);
  }

  /**
   * Create a new JSON Record Reader that uses a in memory materialized JSON stream.
   * @param fragmentContext
   * @param embeddedContent
   * @param fileSystem
   * @param columns  pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public JSONRecordReader(final OperatorContext context, final JsonNode embeddedContent,
      final FileSystemWrapper fileSystem, final List<SchemaPath> columns) throws OutOfMemoryException {
    this(context, null, embeddedContent, fileSystem, columns);
  }

  private JSONRecordReader(final OperatorContext operatorContext,
                           final String inputPath,
                           final JsonNode embeddedContent,
                           final FileSystemWrapper fileSystem,
                           final List<SchemaPath> columns) {
    super(operatorContext, columns);

    Preconditions.checkArgument(
        (inputPath == null && embeddedContent != null) ||
        (inputPath != null && embeddedContent == null),
        "One of inputPath or embeddedContent must be set but not both."
        );

    if(inputPath != null) {
      this.hadoopPath = new Path(inputPath);
    } else {
      this.embeddedContent = embeddedContent;
    }

    this.fileSystem = fileSystem;
    this.context = operatorContext;

    // only enable all text mode if we aren't using embedded content mode.
    this.enableAllTextMode = embeddedContent == null && operatorContext.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR);
    this.readNumbersAsDouble = embeddedContent == null && operatorContext.getOptions().getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR);
    this.unionEnabled = embeddedContent == null && operatorContext.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE);
  }

  @Override
  public String toString() {
    return super.toString()
        + "[hadoopPath = " + hadoopPath
        + ", recordCount = " + recordCount
        + ", runningRecordCount = " + runningRecordCount + ", ...]";
  }

  @Override
  public void setup(final OutputMutator output) throws ExecutionSetupException {
    try{
      if (hadoopPath != null) {
        this.stream = fileSystem.openPossiblyCompressedStream(hadoopPath);
      }

      this.writer = new VectorContainerWriter(output);
      if (isSkipQuery()) {
        this.jsonReader = new CountingJsonReader();
      } else {
        this.jsonReader = new JsonReader(context.getManagedBuffer(), ImmutableList.copyOf(getColumns()), enableAllTextMode, true, readNumbersAsDouble);
      }
      setupParser();
    }catch(final Exception e){
      handleAndRaise("Failure reading JSON file", e);
    }
  }

  private void setupParser() throws IOException {
    if(hadoopPath != null){
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

    if (hadoopPath != null) {
      exceptionBuilder.pushContext("Record ", currentRecordNumberInFile())
          .pushContext("File ", hadoopPath.toUri().getPath());
    }

    throw exceptionBuilder.build(logger);
  }

  private long currentRecordNumberInFile() {
    return runningRecordCount + recordCount + 1;
  }

  @Override
  public int next() {
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

      }

      jsonReader.ensureAtLeastOneField(writer);

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

  public boolean supportsSkipAllQuery(){
    return true;
  }

}
