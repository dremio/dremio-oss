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

import com.dremio.exec.record.BatchSchema;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;

/**
 * Parquet reader for Iceberg datasets. This will be an inner reader of a coercion reader to support
 * up promotion of column data types.
 */
public class IcebergParquetReader extends TransactionalTableParquetReader {

  public IcebergParquetReader(
      OperatorContext context,
      ParquetReaderFactory readerFactory,
      BatchSchema tableSchema,
      ParquetScanProjectedColumns projectedColumns,
      IcebergParquetFilters filters,
      ParquetProtobuf.ParquetDatasetSplitScanXAttr readEntry,
      FileSystem fs,
      MutableParquetMetadata footer,
      SchemaDerivationHelper schemaHelper,
      boolean vectorize,
      boolean enableDetailedTracing,
      boolean supportsColocatedReads,
      InputStreamProvider inputStreamProvider,
      boolean isConvertedIcebergDataset) {
    super(
        context,
        readerFactory,
        tableSchema,
        projectedColumns,
        filters,
        readEntry,
        fs,
        footer,
        schemaHelper,
        vectorize,
        enableDetailedTracing,
        supportsColocatedReads,
        inputStreamProvider,
        isConvertedIcebergDataset);
  }
}
