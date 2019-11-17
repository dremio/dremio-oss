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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.arrow.vector.complex.reader.FieldReader;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.store.EventBasedRecordWriter;
import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
import com.dremio.exec.store.JSONOutputRecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.easy.json.JSONFormatPlugin.JSONFormatConfig;
import com.dremio.exec.vector.complex.fn.BasicJsonOutput;
import com.dremio.exec.vector.complex.fn.ExtendedJsonOutput;
import com.dremio.exec.vector.complex.fn.JsonWriter;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.google.common.collect.Lists;

public class JsonRecordWriter extends JSONOutputRecordWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonRecordWriter.class);
  private static final String LINE_FEED = String.format("%n");

  private final OperatorContext context;
  private final FileSystemPlugin<?> plugin;
  private final String queryUser;

  private String location;
  private String prefix;
  private boolean uglify;
  private String extension;
  private boolean useExtendedOutput;
  private long recordCount;
  private Path fileName;
  private WritePartition partition;

  private FileSystem fs = null;
  private DataOutputStream stream = null;
  private JsonGenerator jsonGenerator = null;

  private long fileSize = 0;

  private final JsonFactory factory = new JsonFactory();

  public JsonRecordWriter(OperatorContext context, EasyWriter writer, JSONFormatConfig formatConfig) {
    super(context);

    final FragmentHandle handle = context.getFragmentHandle();
    final String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());

    this.queryUser = writer.getProps().getUserName();
    this.context = context;
    this.location = writer.getLocation();
    this.prefix = fragmentId;
    this.useExtendedOutput = context.getOptions().getOption(ExecConstants.JSON_EXTENDED_TYPES);
    this.extension = formatConfig.outputExtension;
    this.useExtendedOutput = context.getOptions().getOption(ExecConstants.JSON_EXTENDED_TYPES);
    this.uglify = !formatConfig.prettyPrint || context.getOptions().getOption(ExecConstants.JSON_WRITER_UGLIFY);
    this.plugin = writer.getFormatPlugin().getFsPlugin();
  }

  @Override
  public void setup() throws IOException {
    this.fs = plugin.createFS(queryUser, context);
  }

  @Override
  public void startPartition(WritePartition partition) throws Exception {
    // close previous partition if open.
    if(this.partition != null){
      doClose();
    }
    this.partition = partition;

    try {
      this.fileName = fs.canonicalizePath(partition.qualified(location, prefix + "_0." + extension));
      stream = new DataOutputStream(fs.create(fileName));
      jsonGenerator = factory.createGenerator((OutputStream) stream)
        .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
        .enable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM)
        .useDefaultPrettyPrinter();

      if (uglify) {
        jsonGenerator = jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter(LINE_FEED));
      }
      if(useExtendedOutput){
        gen = new ExtendedJsonOutput(jsonGenerator);
      }else{
        gen = new BasicJsonOutput(jsonGenerator);
      }
      logger.debug("Created file: {}", fileName);
    } catch (IOException ex) {
      throw UserException.dataWriteError(ex)
        .message("Failure writing JSON file %s.", fileName)
        .build(logger);
    }

  }

  @Override
  public FieldConverter getNewMapConverter(int fieldId, String fieldName, FieldReader reader) {
    return new MapJsonConverter(fieldId, fieldName, reader);
  }

  public class MapJsonConverter extends FieldConverter {
    List<FieldConverter> converters = Lists.newArrayList();

    public MapJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      int i = 0;
      for (String name : reader) {
        FieldReader fieldReader = reader.reader(name);
        FieldConverter converter = EventBasedRecordWriter.getConverter(JsonRecordWriter.this, i++, name,
            fieldReader.getMinorType(), fieldReader);
        converters.add(converter);
      }
    }

    @Override
    public void startField() throws IOException {
      gen.writeFieldName(fieldName);
    }

    @Override
    public void writeField() throws IOException {
      gen.writeStartObject();
      for (FieldConverter converter : converters) {
        converter.startField();
        converter.writeField();
      }
      gen.writeEndObject();
    }
  }

  @Override
  public FieldConverter getNewUnionConverter(int fieldId, String fieldName, FieldReader reader) {
    return new UnionJsonConverter(fieldId, fieldName, reader);
  }

  public class UnionJsonConverter extends FieldConverter {

    public UnionJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      gen.writeFieldName(fieldName);
    }

    @Override
    public void writeField() throws IOException {
      JsonWriter writer = new JsonWriter(gen);
      writer.write(reader);
    }
  }

  @Override
  public FieldConverter getNewListConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ListJsonConverter(fieldId, fieldName, reader);
  }

  public class ListJsonConverter extends FieldConverter {
    FieldConverter innerConverter;

    public ListJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      FieldReader fieldReader = reader.reader();
      innerConverter = EventBasedRecordWriter.getConverter(JsonRecordWriter.this, 0, "inner",
          fieldReader.getMinorType(), fieldReader);
    }

    @Override
    public void startField() throws IOException {
      gen.writeFieldName(fieldName);
    }

    @Override
    public void writeField() throws IOException {
      gen.writeStartArray();
      while (reader.next()) {
        innerConverter.writeField();
      }
      gen.writeEndArray();
    }
  }

  @Override
  public FieldConverter getNewNullConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullJsonConverter(fieldId, fieldName, reader);
  }

  public class NullJsonConverter extends FieldConverter {

    public NullJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      gen.writeFieldName(fieldName);
    }

    @Override
    public void writeField() throws IOException {
      /* writing Null to JSON is a NO-OP */
    }
  }

  @Override
  public void startRecord() throws IOException {
    gen.writeStartObject();
  }

  @Override
  public void endRecord() throws IOException {
    gen.writeEndObject();
    recordCount++;
  }

  @Override
  public void abort() throws IOException {
  }

  @Override
  public void close() throws Exception {
    try {
      if (partition == null) {
        // create an empty file.
        startPartition(WritePartition.NONE);
      }
    } finally {
      doClose();
    }
  }

  private void doClose() throws Exception {
    AutoCloseables.close(
      () -> {
          if (gen != null) {
            gen.flush();
            if (stream != null) {
              fileSize = stream.size();
            }
          }
        },
      jsonGenerator,
      stream);

    jsonGenerator = null;
    stream = null;
    if (gen != null) {
      listener.recordsWritten(recordCount, fileSize, fileName.toString(), null, partition.getBucketNumber());
      gen = null;
    }

    recordCount = 0;
    fileSize = 0;
  }
}
