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
package com.dremio.exec.store.easy.sequencefile;

import static com.dremio.common.util.MajorTypeHelper.getArrowTypeForMajorType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Stopwatch;


public class SequenceFileRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SequenceFileRecordReader.class);

  private static final MajorType KEY_TYPE = Types.optional(MinorType.VARBINARY);
  private static final MajorType VALUE_TYPE = Types.optional(MinorType.VARBINARY);

  private static final String keySchema = "binary_key";
  private static final String valueSchema = "binary_value";

  private VarBinaryVector keyVector;
  private VarBinaryVector valueVector;
  private final FileSplit split;
  private org.apache.hadoop.mapred.RecordReader<BytesWritable, BytesWritable> reader;
  private final BytesWritable key = new BytesWritable();
  private final BytesWritable value = new BytesWritable();
  private final FileSystemWrapper dfs;
  private final int maxCellSize;

  public SequenceFileRecordReader(final OperatorContext context,
                                  final FileSplit split,
                                  final FileSystemWrapper dfs) {
    super(context, getStaticColumns());
    this.dfs = dfs;
    this.split = split;
    this.maxCellSize = Math.toIntExact(context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
  }

  private static List<SchemaPath> getStaticColumns() {
    final List<SchemaPath> columns = new ArrayList<>();
    columns.add(SchemaPath.getSimplePath(keySchema));
    columns.add(SchemaPath.getSimplePath(valueSchema));
    return columns;
  }

  @Override
  protected boolean isSkipQuery() {
    return false;
  }

  private org.apache.hadoop.mapred.RecordReader<BytesWritable, BytesWritable> getRecordReader(
    final InputFormat<BytesWritable, BytesWritable> inputFormat,
    final JobConf jobConf) throws ExecutionSetupException {
    try {
      return inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
    } catch (IOException e) {
      throw new ExecutionSetupException(
          String.format("Error in creating sequencefile reader for file: %s, start: %d, length: %d",
              split.getPath(), split.getStart(), split.getLength()), e);
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    final SequenceFileAsBinaryInputFormat inputFormat = new SequenceFileAsBinaryInputFormat();
    final JobConf jobConf = new JobConf(dfs.getConf());
    jobConf.setInputFormat(inputFormat.getClass());
    reader = getRecordReader(inputFormat, jobConf);
    final Field keyField = new Field(keySchema, true, getArrowTypeForMajorType(KEY_TYPE), null);
    final Field valueField = new Field(valueSchema, true, getArrowTypeForMajorType(VALUE_TYPE), null);
    try {
      keyVector = output.addField(keyField, VarBinaryVector.class);
      valueVector = output.addField(valueField, VarBinaryVector.class);
    } catch (SchemaChangeException sce) {
      throw new ExecutionSetupException("Error in setting up sequencefile reader.", sce);
    }
  }

  @Override
  public int next() {
    final Stopwatch watch = Stopwatch.createStarted();
    if (keyVector != null) {
      keyVector.clear();
      keyVector.allocateNew();
    }
    if (valueVector != null) {
      valueVector.clear();
      valueVector.allocateNew();
    }
    int recordCount = 0;
    int batchSize = 0;
    try {
      while (recordCount < numRowsPerBatch && batchSize < numBytesPerBatch && reader.next(key, value)) {
        FieldSizeLimitExceptionHelper.checkReadSizeLimit(key.getLength(), maxCellSize, 0, logger);
        keyVector.setSafe(recordCount, key.getBytes(), 0, key.getLength());
        FieldSizeLimitExceptionHelper.checkReadSizeLimit(value.getLength(), maxCellSize, 1, logger);
        valueVector.setSafe(recordCount, value.getBytes(), 0, value.getLength());
        batchSize += (key.getLength() + value.getLength());
        ++recordCount;
      }
      keyVector.setValueCount(recordCount);
      valueVector.setValueCount(recordCount);
      logger.debug("Read {} records in {} ms", recordCount, watch.elapsed(TimeUnit.MILLISECONDS));
      return recordCount;
    } catch (IOException ioe) {
      close();
      throw UserException.dataReadError(ioe).addContext("File Path", split.getPath().toString()).build(logger);
    }
  }

  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    } catch (IOException e) {
      logger.warn("Exception closing reader: {}", e);
    }
  }
}
