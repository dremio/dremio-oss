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

import org.apache.arrow.vector.SimpleIntVector;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.RecordReader;
import com.dremio.sabot.exec.context.OperatorContext;

public interface ParquetReaderFactory {

  boolean isSupported(ColumnChunkMetaData chunk);

  RecordReader newReader(OperatorContext context,
      List<SchemaPath> columns,
      String path,
      CodecFactory codecFactory,
      List<ParquetFilterCondition> conditions,
      boolean enableDetailedTracing,
      ParquetMetadata footer,
      int rowGroupIndex,
      SimpleIntVector deltas,
      SchemaDerivationHelper schemaHelper,
      InputStreamProvider inputStreamProvider);

  ParquetReaderFactory NONE = new ParquetReaderFactory(){

    @Override
    public boolean isSupported(ColumnChunkMetaData chunk) {
      return false;
    }

    @Override
    public RecordReader newReader(OperatorContext context, List<SchemaPath> columns, String path,
        CodecFactory codecFactory, List<ParquetFilterCondition> conditions, boolean enableDetailedTracing,
        ParquetMetadata footer, int rowGroupIndex, SimpleIntVector deltas, SchemaDerivationHelper schemaHelper,
        InputStreamProvider inputStreamProvider) {
      throw new UnsupportedOperationException();
    }};

}
