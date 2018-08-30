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
package com.dremio.exec.store.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.complex.impl.StructOrListWriterImpl;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.vector.complex.fn.FieldSelection;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;

import io.netty.buffer.ArrowBuf;

/**
 * A RecordReader implementation for Avro data files.
 *
 * @see RecordReader
 */
public class AvroRecordReader extends AbstractRecordReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroRecordReader.class);

  private final Path hadoop;
  private final long start;
  private final long end;
  private final FieldSelection fieldSelection;
  private ArrowBuf buffer;
  private VectorContainerWriter writer;

  private DataFileReader<GenericContainer> reader = null;
  private final Configuration fsConf;


  public AvroRecordReader(final OperatorContext context,
                          final String inputPath,
                          final long start,
                          final long length,
                          final List<SchemaPath> projectedColumns,
                          final Configuration fsConf
                          ) {
    super(context, projectedColumns);
    hadoop = new Path(inputPath);
    this.start = start;
    this.end = start + length;
    buffer = context.getManagedBuffer();
    this.fieldSelection = FieldSelection.getFieldSelection(projectedColumns);
    this.fsConf = fsConf;
  }

  @Override
  public void setup(final OutputMutator output) throws ExecutionSetupException {
    writer = new VectorContainerWriter(output);

    try {
      reader = new DataFileReader<>(new FsInput(hadoop, fsConf), new GenericDatumReader<GenericContainer>());
      logger.debug("Processing file : {}, start position : {}, end position : {} ", hadoop, start, end);
      reader.sync(this.start);
    } catch (IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    final Stopwatch watch = Stopwatch.createStarted();

    if (reader == null) {
      throw new IllegalStateException("Avro reader is not open.");
    }
    if (!reader.hasNext()) {
      return 0;
    }

    int recordCount = 0;
    writer.allocate();
    writer.reset();

    try {
      for (GenericContainer container = null;
           recordCount < numRowsPerBatch && reader.hasNext() && !reader.pastSync(end);
           recordCount++) {
        writer.setPosition(recordCount);
        container = reader.next(container);
        processRecord(container, container.getSchema());
      }

      writer.setValueCount(recordCount);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    logger.debug("Read {} records in {} ms", recordCount, watch.elapsed(TimeUnit.MILLISECONDS));
    return recordCount;
  }

  private void processRecord(final GenericContainer container, final Schema schema) {

    final Schema.Type type = schema.getType();

    switch (type) {
      case RECORD:
        process(container, schema, null, new StructOrListWriterImpl(writer.rootAsStruct()), fieldSelection);
        break;
      default:
        throw new RuntimeException("Root object must be record type. Found: " + type);
    }
  }

  private void process(final Object value, final Schema schema, final String fieldName, StructOrListWriterImpl writer, FieldSelection fieldSelection) {
    if (value == null) {
      return;
    }
    final Schema.Type type = schema.getType();
    switch (type) {
      case RECORD:
        // list field of StructOrListWriter will be non null when we want to store array of maps/records.
        StructOrListWriterImpl _writer = writer;

        for (final Schema.Field field : schema.getFields()) {
          if (fieldSelection.getChild(field.name()).isNeverValid()) {
            continue;
          }
          boolean isMap = field.schema().getType() == Schema.Type.RECORD ||
              (field.schema().getType() == Schema.Type.UNION &&
              field.schema().getTypes().get(0).getType() == Schema.Type.NULL &&
              field.schema().getTypes().get(1).getType() == Schema.Type.RECORD);
          if (isMap) {
              _writer = (StructOrListWriterImpl) writer.struct(field.name());
              _writer.start();
          }

          process(((GenericRecord) value).get(field.name()), field.schema(), field.name(), _writer, fieldSelection.getChild(field.name()));

          if (isMap) {
            _writer.end();
          }
        }
        break;
      case ARRAY:
        assert fieldName != null;
        final GenericArray<?> array = (GenericArray<?>) value;
        Schema elementSchema = array.getSchema().getElementType();
        Type elementType = elementSchema.getType();
        if (elementType == Schema.Type.RECORD || elementType == Schema.Type.MAP){
          writer = (StructOrListWriterImpl) writer.list(fieldName).listoftstruct(fieldName);
        } else {
          writer = (StructOrListWriterImpl) writer.list(fieldName);
        }
        writer.start();
        for (final Object o : array) {
          process(o, elementSchema, fieldName, writer, fieldSelection.getChild(fieldName));
        }
        writer.end();
        break;
      case UNION:
        // currently supporting only nullable union (optional fields) like ["null", "some-type"].
        if (schema.getTypes().get(0).getType() != Schema.Type.NULL) {
          throw new UnsupportedOperationException("Avro union type must be of the format : [\"null\", \"some-type\"]");
        }
        process(value, schema.getTypes().get(1), fieldName, writer, fieldSelection);
        break;
      case MAP:
        @SuppressWarnings("unchecked")
        final HashMap<Object, Object> map = (HashMap<Object, Object>) value;
        Schema valueSchema = schema.getValueType();
        writer = (StructOrListWriterImpl) writer.struct(fieldName);
        writer.start();
        for (Entry<Object, Object> entry : map.entrySet()) {
          process(entry.getValue(), valueSchema, entry.getKey().toString(), writer, fieldSelection.getChild(entry.getKey().toString()));
        }
        writer.end();
        break;
      case FIXED:
        throw new UnsupportedOperationException("Unimplemented type: " + type.toString());
      case ENUM:  // Enum symbols are strings
      case NULL:  // Treat null type as a primitive
      default:
        assert fieldName != null;

        if (writer.isStructWriter()) {
          if (fieldSelection.isNeverValid()) {
            break;
          }
        }

        processPrimitive(value, schema.getType(), fieldName, writer);
        break;
    }

  }

  private void processPrimitive(final Object value, final Schema.Type type, final String fieldName,
                                final StructOrListWriterImpl writer) {
    if (value == null) {
      return;
    }

    switch (type) {
      case STRING:
        byte[] binary = null;
        final int length;
        if (value instanceof Utf8) {
          binary = ((Utf8) value).getBytes();
          length = ((Utf8) value).getByteLength();
        } else {
          binary = value.toString().getBytes(Charsets.UTF_8);
          length = binary.length;
        }
        ensure(length);
        buffer.setBytes(0, binary);
        writer.varChar(fieldName).writeVarChar(0, length, buffer);
        break;
      case INT:
        writer.integer(fieldName).writeInt((Integer) value);
        break;
      case LONG:
        writer.bigInt(fieldName).writeBigInt((Long) value);
        break;
      case FLOAT:
        writer.float4(fieldName).writeFloat4((Float) value);
        break;
      case DOUBLE:
        writer.float8(fieldName).writeFloat8((Double) value);
        break;
      case BOOLEAN:
        writer.bit(fieldName).writeBit((Boolean) value ? 1 : 0);
        break;
      case BYTES:
        final ByteBuffer buf = (ByteBuffer) value;
        length = buf.remaining();
        ensure(length);
        buffer.setBytes(0, buf);
        writer.binary(fieldName).writeVarBinary(0, length, buffer);
        break;
      case NULL:
        // Nothing to do for null type
        break;
      case ENUM:
        final String symbol = value.toString();
        final byte[] b = symbol.getBytes(StandardCharsets.UTF_8);
        ensure(b.length);
        buffer.setBytes(0, b);
        writer.varChar(fieldName).writeVarChar(0, b.length, buffer);

        break;
      default:
        throw new RuntimeException("Unhandled Avro type: " + type.toString());
    }
  }

  private boolean selected(SchemaPath field) {
    if (isStarQuery()) {
      return true;
    }
    for (final SchemaPath sp : getColumns()) {
      if (sp.contains(field)) {
        return true;
      }
    }
    return false;
  }

  private void ensure(final int length) {
    buffer = buffer.reallocIfNeeded(length);
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        logger.warn("Error closing Avro reader", e);
      } finally {
        reader = null;
      }
    }
  }
}
