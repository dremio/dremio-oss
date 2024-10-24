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
package com.dremio.exec.store.parquet.copyinto;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties;
import com.dremio.exec.physical.config.copyinto.IngestionProperties;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.FileTypeCoercion;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.parquet.ParquetFilters;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.ParquetScanProjectedColumns;
import com.dremio.exec.store.parquet.ParquetSplitReaderCreatorIterator;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.DefaultNameMapping;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import io.protostuff.ByteString;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Depending on dry_run field of the splitXAttr instance corresponding to this reader, will either
 * create a CopyIntoSkipParquetCoercionReader for 1st (dry run) or rely on the super class's reader
 * creator function to setup a coercion reader for the 2nd scan
 */
public class CopyIntoSkipParquetSplitReaderCreator
    extends CopyIntoTransformationParquetSplitReaderCreator {

  private final CopyIntoSkipParquetSplitReaderCreatorIterator
      copyIntoSkipParquetSplitReaderCreatorIterator;
  private final IngestionProperties ingestionProperties;

  CopyIntoSkipParquetSplitReaderCreator(
      boolean autoCorrectCorruptDates,
      OperatorContext context,
      boolean enableDetailedTracing,
      FileSystem fs,
      int numSplitsToPrefetch,
      boolean prefetchReader,
      boolean readInt96AsTimeStamp,
      CompositeReaderConfig readerConfig,
      ParquetReaderFactory readerFactory,
      List<SchemaPath> realFields,
      boolean supportsColocatedReads,
      boolean vectorize,
      SplitAndPartitionInfo splitInfo,
      IngestionProperties ingestionProperties,
      List<List<String>> tablePath,
      ParquetFilters filters,
      List<SchemaPath> columns,
      BatchSchema fullSchema,
      FileConfig formatSettings,
      List<IcebergProtobuf.IcebergSchemaField> icebergSchemaFields,
      List<DefaultNameMapping> icebergDefaultNameMapping,
      Map<String, Set<Integer>> pathToRowGroupsMap,
      ParquetSplitReaderCreatorIterator parquetSplitReaderCreatorIterator,
      ParquetProtobuf.ParquetDatasetSplitScanXAttr splitXAttr,
      boolean ignoreSchemaLearning,
      boolean isConvertedIcebergDataset,
      UserDefinedSchemaSettings userDefinedSchemaSettings,
      ByteString extendedProperties,
      CopyIntoTransformationProperties transformationProperties,
      BatchSchema targetSchema) {
    super(
        autoCorrectCorruptDates,
        context,
        enableDetailedTracing,
        fs,
        numSplitsToPrefetch,
        prefetchReader,
        readInt96AsTimeStamp,
        readerConfig,
        readerFactory,
        realFields,
        supportsColocatedReads,
        false, // override trimRowGroups to false
        vectorize,
        splitInfo,
        tablePath,
        filters,
        columns,
        fullSchema,
        formatSettings,
        icebergSchemaFields,
        icebergDefaultNameMapping,
        pathToRowGroupsMap,
        parquetSplitReaderCreatorIterator,
        splitXAttr,
        ignoreSchemaLearning,
        isConvertedIcebergDataset,
        userDefinedSchemaSettings,
        extendedProperties,
        transformationProperties,
        targetSchema);
    this.copyIntoSkipParquetSplitReaderCreatorIterator =
        (CopyIntoSkipParquetSplitReaderCreatorIterator) parquetSplitReaderCreatorIterator;
    this.ingestionProperties = ingestionProperties;
  }

  @Override
  protected RecordReader createRecordReader(
      ParquetScanProjectedColumns projectedColumns, RecordReader innerReader) {
    RecordReader wrappedRecordReader =
        splitXAttr.getIsDryRun()
            ? CopyIntoSkipParquetCoercionReader.newInstance(
                context,
                projectedColumns.getBatchSchemaProjectedColumns(),
                innerReader,
                fullSchema,
                getFileTypeCoercion(),
                filters,
                extendedProperties,
                copyIntoSkipParquetSplitReaderCreatorIterator,
                splitXAttr.getRowGroupIndex(),
                splitXAttr.getRowIndexOffset(),
                splitXAttr.getWriteSuccessEvent(),
                splitXAttr.getLength(),
                ingestionProperties,
                transformationProperties,
                targetSchema)
            : super.createRecordReader(projectedColumns, innerReader);
    return readerConfig.wrapIfNecessary(context.getAllocator(), wrappedRecordReader, datasetSplit);
  }

  @Override
  protected FileTypeCoercion getFileTypeCoercion() {
    FileTypeCoercion fileTypeCoercion = super.getFileTypeCoercion();
    if (fileTypeCoercion != null) {
      return fileTypeCoercion;
    }
    Map<String, Field> fieldsByName = CaseInsensitiveMap.newHashMap();
    fullSchema.getFields().forEach(field -> fieldsByName.put(field.getName(), field));
    return new FileTypeCoercion(fieldsByName);
  }
}
