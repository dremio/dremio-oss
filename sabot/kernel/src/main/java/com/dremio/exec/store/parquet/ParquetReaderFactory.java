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
package com.dremio.exec.store.parquet;

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.SimpleIntVector;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.sabot.exec.context.OperatorContext;

public interface ParquetReaderFactory {
  enum ManagedSchemaType {
    HIVE,
    ICEBERG
  }

  boolean isSupported(ColumnChunkMetaData chunk);

  RecordReader newReader(OperatorContext context,
                         ParquetScanProjectedColumns projectedColumns,
                         String path,
                         CompressionCodecFactory codecFactory,
                         List<ParquetFilterCondition> conditions,
                         ParquetFilterCreator filterCreator,
                         ParquetDictionaryConvertor dictionaryConvertor,
                         boolean enableDetailedTracing,
                         MutableParquetMetadata footer,
                         int rowGroupIndex,
                         SimpleIntVector deltas,
                         SchemaDerivationHelper schemaHelper,
                         InputStreamProvider inputStreamProvider,
                         List<RuntimeFilter> runtimeFilters,
                         ArrowBuf validityBuf,
                         BatchSchema tableSchema,
                         boolean ignoreSchemaLearning);

  ParquetFilterCreator newFilterCreator(OperatorContext operatorContext, ManagedSchemaType type, ManagedSchema schema, BufferAllocator allocator);

  ParquetDictionaryConvertor newDictionaryConvertor(ManagedSchemaType type, ManagedSchema schema);

  ParquetReaderFactory NONE = new ParquetReaderFactory(){

    @Override
    public boolean isSupported(ColumnChunkMetaData chunk) {
      return false;
    }

    @Override
    public RecordReader newReader(OperatorContext context,
                                  ParquetScanProjectedColumns projectedColumns,
                                  String path,
                                  CompressionCodecFactory codecFactory,
                                  List<ParquetFilterCondition> conditions,
                                  ParquetFilterCreator filterCreator,
                                  ParquetDictionaryConvertor dictionaryConvertor,
                                  boolean enableDetailedTracing,
                                  MutableParquetMetadata footer,
                                  int rowGroupIndex,
                                  SimpleIntVector deltas,
                                  SchemaDerivationHelper schemaHelper,
                                  InputStreamProvider inputStreamProvider,
                                  List<RuntimeFilter> runtimeFilters,
                                  ArrowBuf validityBuf,
                                  BatchSchema tableSchema,
                                  boolean ignoreSchemaLearning) {

      throw new UnsupportedOperationException();
    }

    @Override
    public ParquetFilterCreator newFilterCreator(OperatorContext operatorContext, ManagedSchemaType type, ManagedSchema managedSchema, BufferAllocator allocator) {
      return ParquetFilterCreator.DEFAULT;
    }

    @Override
    public ParquetDictionaryConvertor newDictionaryConvertor(ManagedSchemaType type, ManagedSchema schema) {
      return ParquetDictionaryConvertor.DEFAULT;
    }
  };
}
