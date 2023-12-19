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

import java.util.function.Function;

import org.apache.commons.lang3.function.TriConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import com.google.common.collect.ImmutableMap;

public class IcebergParquetRecordWriter<T extends StructLike> extends WriteSupport<T> {

  final Schema schema;
  final MessageType type;
  final TriConsumer<Schema, RecordConsumer, T> recordWriter;
  RecordConsumer recordConsumer;

  public IcebergParquetRecordWriter(
    final Schema schema,
    final Function<Schema, MessageType> schemaConverter,
    final TriConsumer<Schema, RecordConsumer, T> recordWriter
  ) {
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
  public void write(T record) {
    recordWriter.accept(schema, recordConsumer, record);
  }
}
