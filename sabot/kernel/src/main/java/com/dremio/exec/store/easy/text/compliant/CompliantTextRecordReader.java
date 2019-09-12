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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.poi.hssf.util.CellReference;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.FileSystemUtils;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.univocity.parsers.common.TextParsingException;

import io.netty.buffer.ArrowBuf;


// New text reader, complies with the RFC 4180 standard for text/csv files
public class CompliantTextRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompliantTextRecordReader.class);

  private static final int READ_BUFFER = 1024 * 1024;
  private static final int WHITE_SPACE_BUFFER = 64 * 1024;

  // settings to be used while parsing
  private final TextParsingSettings settings;
  // Chunk of the file to be read by this reader
  private final FileSplit split;
  // text reader implementation
  private TextReader reader;
  // input buffer
  private ArrowBuf readBuffer;
  // working buffer to handle whitespaces
  private ArrowBuf whitespaceBuffer;
  private final CompressionCodecFactory codecFactory;
  private final FileSystem dfs;

  public CompliantTextRecordReader(FileSplit split, CompressionCodecFactory codecFactory, FileSystem dfs,
      OperatorContext context, TextParsingSettings settings, List<SchemaPath> columns) {
    super(context, columns);
    this.split = split;
    this.settings = settings;
    this.codecFactory = codecFactory;
    this.dfs = dfs;
  }

  // checks to see if we are querying all columns(star) or individual columns
  @Override
  public boolean isStarQuery() {
    if (settings.isUseRepeatedVarChar()) {
      return super.isStarQuery() || Iterables.tryFind(getColumns(), path -> path.equals(RepeatedVarCharOutput.COLUMNS)).isPresent();
    }
    return super.isStarQuery();
  }

  /**
   * Performs the initial setup required for the record reader.
   * Initializes the input stream, handling of the output record batch
   * and the actual reader to be used.
   *
   * @param outputMutator Used to create the schema in the output record batch
   * @throws ExecutionSetupException
   */
  @Override
  public void setup(OutputMutator outputMutator) throws ExecutionSetupException {
    // setup Output, Input, and Reader
    try {
      final TextOutput output;

      if (isSkipQuery()) {
        if (settings.isHeaderExtractionEnabled()) {
          extractHeader();
        }
        // When no columns are projected try to make the parser do less work by turning off options that have extra cost
        settings.setIgnoreLeadingWhitespaces(false);
        settings.setIgnoreTrailingWhitespaces(false);
        output = new TextCountOutput();
      } else {
        final int sizeLimit = Math.toIntExact(this.context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
        // setup Output using OutputMutator
        if (settings.isHeaderExtractionEnabled()) {
          //extract header and use that to setup a set of VarCharVectors
          String[] fieldNames = extractHeader();
          output = new FieldVarCharOutput(outputMutator, fieldNames, getColumns(), isStarQuery(), sizeLimit);
        } else if (settings.isAutoGenerateColumnNames()) {
          String[] fieldNames = generateColumnNames();
          output = new FieldVarCharOutput(outputMutator, fieldNames, getColumns(), isStarQuery(), sizeLimit);
        } else {
          //simply use RepeatedVarCharVector
          output = new RepeatedVarCharOutput(outputMutator, getColumns(), isStarQuery(), sizeLimit);
        }
      }

      readBuffer = this.context.getAllocator().buffer(READ_BUFFER);
      whitespaceBuffer = this.context.getAllocator().buffer(WHITE_SPACE_BUFFER);

      // setup Input using InputStream
      FSInputStream stream = FileSystemUtils.openPossiblyCompressedStream(codecFactory, dfs, Path.of(split.getPath().toUri()));
      TextInput input = new TextInput(settings, stream, readBuffer, split.getStart(), split.getStart() + split.getLength());

      // setup Reader using Input and Output
      reader = new TextReader(settings, input, output, whitespaceBuffer);
      reader.start();
    } catch (IOException e) {
      if (e.getCause() instanceof StreamFinishedPseudoException) {
        return;
      }
      throw new ExecutionSetupException(String.format("Failure while setting up text reader for file %s", split.getPath()), e);
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(String.format("Failure while setting up text reader for file %s", split.getPath()), e);
    } catch (IllegalArgumentException e) {
      throw UserException.dataReadError(e).addContext("File Path", split.getPath().toString()).build(logger);
    }
  }

  private String[] readFirstLineForColumnNames() throws ExecutionSetupException, SchemaChangeException, IOException {
    // setup Output using OutputMutator
    // we should use a separate output mutator to avoid reshaping query output with header data
    try (HeaderOutputMutator hOutputMutator = new HeaderOutputMutator();
         ArrowBuf readBufferInReader = this.context.getAllocator().buffer(READ_BUFFER);
         ArrowBuf whitespaceBufferInReader = this.context.getAllocator().buffer(WHITE_SPACE_BUFFER)) {
      final int sizeLimit = Math.toIntExact(this.context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
      final RepeatedVarCharOutput hOutput = new RepeatedVarCharOutput(hOutputMutator, getColumns(), true, sizeLimit);
      this.allocate(hOutputMutator.fieldVectorMap);

      // setup Input using InputStream
      // we should read file header irrespective of split given given to this reader
      FSInputStream hStream = FileSystemUtils.openPossiblyCompressedStream(codecFactory, dfs, Path.of(split.getPath().toUri()));
      TextInput hInput = new TextInput(settings, hStream, readBufferInReader, 0, Math.min(READ_BUFFER, split.getLength()));
      // setup Reader using Input and Output
      this.reader = new TextReader(settings, hInput, hOutput, whitespaceBufferInReader);
      reader.start();

      String[] fieldNames;
      try {
        // extract first non-empty row
        do {
          boolean isAnyData = reader.parseNext();
          if (!isAnyData) {
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
        // cleanup and set to skip the first line next time we read input
        reader.close();
      }
    }
  }

  /**
   * This method is responsible to implement logic for extracting header from text file
   * Currently it is assumed to be first line if headerExtractionEnabled is set to true
   * TODO: enhance to support more common header patterns
   *
   * @return field name strings
   */
  private String[] extractHeader() throws SchemaChangeException, IOException, ExecutionSetupException {
    assert (settings.isHeaderExtractionEnabled());

    // don't skip header in case skipFirstLine is set true
    settings.setSkipFirstLine(false);
    final String[] fieldNames = readFirstLineForColumnNames();
    settings.setSkipFirstLine(true);
    return validateColumnNames(fieldNames);
  }

  public static String[] validateColumnNames(String[] fieldNames) {
    final Map<String, Integer> uniqueFieldNames = Maps.newHashMap();
    if (fieldNames != null) {
      for (int i = 0; i < fieldNames.length; ++i) {
        if (fieldNames[i].isEmpty()) {
          fieldNames[i] = CellReference.convertNumToColString(i);
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
   * Generate fields names per column in text file.
   * Read first line and count columns and return fields names like excel sheet.
   * A, B, C and so on.
   *
   * @return field name strings, null if no records found in text file.
   */
  private String[] generateColumnNames() throws SchemaChangeException, IOException, ExecutionSetupException {
    assert (settings.isAutoGenerateColumnNames());

    final boolean shouldSkipFirstLine = settings.isSkipFirstLine();
    settings.setSkipFirstLine(false);
    final String[] columns = readFirstLineForColumnNames();
    settings.setSkipFirstLine(shouldSkipFirstLine);
    if (columns != null && columns.length > 0) {
      String[] fieldNames = new String[columns.length];
      for (int i = 0; i < columns.length; ++i) {
        fieldNames[i] = CellReference.convertNumToColString(i);
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
    reader.resetForNextBatch();
    int cnt = 0;

    try {
      while (cnt < numRowsPerBatch && reader.parseNext()) {
        cnt++;
      }
      reader.finishBatch();
      return cnt;
    } catch (IOException | TextParsingException e) {
      throw UserException.dataReadError(e)
        .addContext("Failure while reading file %s. Happened at or shortly before byte position %d.",
          split.getPath(), reader.getPos())
        .build(logger);
    }
  }

  /**
   * Cleanup state once we are finished processing all the records.
   * This would internally close the input stream we are reading from.
   */
  @Override
  public void close() throws Exception {
    try {
      AutoCloseables.close(reader, readBuffer, whitespaceBuffer);
    } finally {
      reader = null;
      readBuffer = null;
      whitespaceBuffer = null;
    }
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    int estimatedRecordCount;
    if ((reader != null) && (reader.getInput() != null) && (vectorMap.size() > 0)) {
      final OptionManager options = context.getOptions();
      final int listSizeEstimate = (int) options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
      final int varFieldSizeEstimate = (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
      final int estimatedRecordSize = BatchSchema.estimateRecordSize(vectorMap, listSizeEstimate, varFieldSizeEstimate);
      if (estimatedRecordSize > 0) {
        estimatedRecordCount = (int) Math.min(reader.getInput().length / estimatedRecordSize, numRowsPerBatch);
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
   * TextRecordReader during its first phase read to extract header should pass its own
   * OutputMutator to avoid reshaping query output.
   * This class provides OutputMutator for header extraction.
   */
  private class HeaderOutputMutator implements OutputMutator, AutoCloseable {
    private final Map<String, ValueVector> fieldVectorMap = Maps.newHashMap();

    @Override
    public <T extends ValueVector> T addField(Field field, Class<T> clazz) throws SchemaChangeException {
      ValueVector v = fieldVectorMap.get(field.getName().toLowerCase());
      if (v == null || v.getClass() != clazz) {
        // Field does not exist add it to the map
        v = TypeHelper.getNewVector(field, context.getAllocator());
        if (!clazz.isAssignableFrom(v.getClass())) {
          throw new SchemaChangeException(String.format(
            "Class %s was provided, expected %s.", clazz.getSimpleName(), v.getClass().getSimpleName()));
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
      //do nothing for now
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
    public boolean isSchemaChanged() {
      return false;
    }

    /**
     * Since this OutputMutator is passed by TextRecordReader to get the header out
     * the mutator might not get cleaned up elsewhere. TextRecordReader will call
     * this method to clear any allocations
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
