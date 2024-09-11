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
package com.dremio.exec.util.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang3.function.TriConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class IcebergParquetRecordWriter<T extends StructLike> extends WriteSupport<T> {

  final Schema schema;
  final MessageType type;
  final TriConsumer<Schema, RecordConsumer, T> recordWriter;
  RecordConsumer recordConsumer;

  public IcebergParquetRecordWriter(
      final Schema schema,
      final Function<Schema, MessageType> schemaConverter,
      final TriConsumer<Schema, RecordConsumer, T> recordWriter) {
    this.schema = schema;
    this.type = schemaConverter.apply(schema);
    this.recordWriter = recordWriter;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    return new WriteContext(type, ImmutableMap.of());
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(T rec) {
    recordWriter.accept(schema, recordConsumer, rec);
  }

  public static MessageType convertPrimitiveSchema(final Schema schema) {
    List<Type> fields = new ArrayList<>(schema.columns().size());
    schema
        .columns()
        .forEach(
            col -> {
              org.apache.parquet.schema.Types.PrimitiveBuilder<PrimitiveType> fieldBuilder;
              org.apache.parquet.schema.Type.Repetition repetition =
                  col.isRequired()
                      ? org.apache.parquet.schema.Type.Repetition.REQUIRED
                      : org.apache.parquet.schema.Type.Repetition.OPTIONAL;
              switch (col.type().typeId()) {
                case BOOLEAN:
                  fieldBuilder =
                      org.apache.parquet.schema.Types.primitive(
                          PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition);
                  break;
                case INTEGER:
                case DATE:
                  fieldBuilder =
                      org.apache.parquet.schema.Types.primitive(
                          PrimitiveType.PrimitiveTypeName.INT32, repetition);
                  break;
                case LONG:
                case TIME:
                case TIMESTAMP:
                  fieldBuilder =
                      org.apache.parquet.schema.Types.primitive(
                          PrimitiveType.PrimitiveTypeName.INT64, repetition);
                  break;
                case FLOAT:
                  fieldBuilder =
                      org.apache.parquet.schema.Types.primitive(
                          PrimitiveType.PrimitiveTypeName.FLOAT, repetition);
                  break;
                case DOUBLE:
                  fieldBuilder =
                      org.apache.parquet.schema.Types.primitive(
                          PrimitiveType.PrimitiveTypeName.DOUBLE, repetition);
                  break;
                case STRING:
                case FIXED:
                case BINARY:
                  fieldBuilder =
                      org.apache.parquet.schema.Types.primitive(
                          PrimitiveType.PrimitiveTypeName.BINARY, repetition);
                  break;
                default:
                  throw new IllegalStateException("This writer only supports primitive types");
              }
              PrimitiveType field = fieldBuilder.id(col.fieldId()).named(col.name());
              fields.add(field);
            });
    return new MessageType("root", fields);
  }

  public static void writePrimitiveRecord(
      final Schema schema, final RecordConsumer consumer, final StructLike rec) {
    Preconditions.checkArgument(schema.columns().size() <= rec.size());
    consumer.startMessage();
    for (int i = 0; i < schema.columns().size(); i++) {
      final Types.NestedField field = schema.columns().get(i);
      final String fieldName = field.name();
      final org.apache.iceberg.types.Type.TypeID fieldType = field.type().typeId();
      consumer.startField(fieldName, i);
      switch (fieldType) {
        case BOOLEAN:
          consumer.addBoolean(rec.get(i, Boolean.class));
          break;
        case INTEGER:
        case DATE:
          consumer.addInteger(rec.get(i, Integer.class));
          break;
        case LONG:
        case TIMESTAMP:
        case TIME:
          consumer.addLong(rec.get(i, Long.class));
          break;
        case FLOAT:
          consumer.addFloat(rec.get(i, Float.class));
          break;
        case DOUBLE:
          consumer.addDouble(rec.get(i, Double.class));
          break;
        case STRING:
          consumer.addBinary(Binary.fromCharSequence(rec.get(i, CharSequence.class)));
          break;
        case FIXED:
        case BINARY:
          consumer.addBinary(Binary.fromConstantByteBuffer(rec.get(i, ByteBuffer.class)));
          break;
        default:
          throw new IllegalStateException("This writer only supports primitive types");
      }
      consumer.endField(fieldName, i);
    }
    consumer.endMessage();
  }
}
