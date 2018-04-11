/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.text;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import com.dremio.sabot.exec.context.OperatorStats;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.commons.io.ByteOrderMark;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
import com.dremio.exec.store.StringOutputRecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;

public class TextRecordWriter extends StringOutputRecordWriter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TextRecordWriter.class);

  private final Configuration conf;
  private final OperatorStats stats;
  private final String location;
  private final String prefix;
  private final String fieldDelimiter;
  private final String lineDelimiter;
  private final String extension;

  private WritePartition partition;
  private List<String> columnNames;
  private int index;
  private PrintStream stream = null;
  private FileSystemWrapper fs = null;

  private Path path;
  private long count;

  // Record write status
  private boolean fRecordStarted = false; // true once the startRecord() is called until endRecord() is called
  private StringBuilder currentRecord; // contains the current record separated by field delimiter

  public TextRecordWriter(OperatorContext context, EasyWriter config, TextFormatConfig textConfig) {
    final FragmentHandle handle = context.getFragmentHandle();
    this.conf = new Configuration(config.getFsConf());
    this.stats = context.getStats();
    this.location = config.getLocation();
    this.prefix = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    this.fieldDelimiter = textConfig.getFieldDelimiterAsString();
    this.lineDelimiter = textConfig.getLineDelimiter();
    this.extension = textConfig.outputExtension;
    this.currentRecord = new StringBuilder();
    this.index = 0;
  }

  @Override
  public void setup(List<String> columnNames) throws IOException {
    this.columnNames = columnNames;
    this.fs = FileSystemWrapper.get(conf, stats);
  }

  public static final String NEWLINE = "\n";
  public static final char QUOTE = '"';
  public static final String QUOTE_WITH_ESCAPE = "\"\""; // csv files escape quotes with double quotes
  /*
   * Enclose 'value' in quotes while escaping any quotes that are already in it
   */
  private String quote(String value) {
    StringBuilder sb = new StringBuilder();
    sb.append(QUOTE);
    sb.append(CharMatcher.is(QUOTE).replaceFrom(value, QUOTE_WITH_ESCAPE));
    sb.append(QUOTE);
    return sb.toString();
  }

  @Override
  public void addField(int fieldId, String value) throws IOException {
    if (value != null) {
      // Note: even if newline (\n) is not a line delimiter, external tools (google docs, etc.) treat it as one.
      // Thus, we should escape it
      if (value.contains(fieldDelimiter) || value.contains(lineDelimiter) || value.contains(NEWLINE)) {
        currentRecord.append(quote(value));
      } else {
        currentRecord.append(value);
      }
    }
    currentRecord.append(fieldDelimiter);
  }

  @Override
  public void startPartition(WritePartition partition) throws Exception {

    if(this.partition != null){
      close();
    }

    this.partition = partition;
    // open a new file for writing data with new schema
    try {
      this.path = fs.canonicalizePath(partition.qualified(location, prefix + "_" + index + "." + extension));
      DataOutputStream fos = fs.create(path);
      stream = new PrintStream(fos);
      stream.write(ByteOrderMark.UTF_8.getBytes(), 0, ByteOrderMark.UTF_8.length());
      logger.debug("Created file: {}", path);
    } catch (IOException e) {
      throw UserException.dataWriteError(e)
        .message("Failure while attempting to write file %s.", path)
        .build(logger);
    }
    index++;

    stream.print(Joiner.on(fieldDelimiter).join(columnNames));
    stream.print(lineDelimiter);

  }

  @Override
  public void startRecord() throws IOException {
    if (fRecordStarted) {
      throw new IOException("Previous record is not written completely");
    }

    fRecordStarted = true;
  }

  @Override
  public void endRecord() throws IOException {
    if (!fRecordStarted) {
      throw new IOException("No record is in writing");
    }

    count++;

    // remove the extra delimiter at the end
    currentRecord.deleteCharAt(currentRecord.length()-fieldDelimiter.length());

    stream.print(currentRecord.toString());
    stream.print(lineDelimiter);

    // reset current record status
    currentRecord.delete(0, currentRecord.length());
    fRecordStarted = false;
  }

  @Override
  public FieldConverter getNewMapConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ComplexStringFieldConverter(fieldId, fieldName, reader);
  }

  public class ComplexStringFieldConverter extends FieldConverter {

    public ComplexStringFieldConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeField() throws IOException {
      addField(fieldId, reader.readObject().toString());
    }
  }

  @Override
  public FieldConverter getNewNullConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullTextConverter(fieldId, fieldName, reader);
  }

  public class NullTextConverter extends FieldConverter {

    public NullTextConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeField() throws IOException {
      addField(fieldId, null);
    }
  }

  @Override
  public void close() {

    if (stream != null) {
      listener.recordsWritten(count, path.toString(), null, partition.getBucketNumber());
      stream.close();
      stream = null;
      count = 0;
      index = 0;
      logger.debug("closing file");
    }
  }

  @Override
  public void abort() throws IOException {
    try {
      close();
      fs.delete(new Path(location), true);
    } catch (Exception ex) {
      logger.error("Abort failed. There could be leftover output files", ex);
      throw new IOException(ex);
    }
  }

}
