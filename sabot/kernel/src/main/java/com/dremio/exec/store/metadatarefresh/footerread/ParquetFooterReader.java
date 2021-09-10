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
package com.dremio.exec.store.metadatarefresh.footerread;

import static com.dremio.exec.ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.arrow.DremioArrowSchema;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.exec.store.parquet.ParquetScanProjectedColumns;
import com.dremio.exec.store.parquet.ParquetTypeHelper;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.exec.store.parquet.SingleStreamProvider;
import com.dremio.exec.store.parquet2.ParquetRowiseReader;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

/**
 * Parquet footer reader used in refresh dataset flow
 */
public class ParquetFooterReader implements FooterReader {

  private static final Logger logger = LoggerFactory.getLogger(ParquetFooterReader.class);

  private final FileSystem fs;
  private final OperatorContext opContext;
  private BatchSchema tableSchema;
  private final long estimatedRecordSize;
  private final double compressionFactor;
  private final boolean readFooter;
  private final int maxLeafCols;

  public ParquetFooterReader(OperatorContext opContext, BatchSchema tableSchema, FileSystem fs,
                             long estimatedRecordSize, double compressionFactor, boolean readFooter) {
    this.opContext = opContext;
    this.tableSchema = tableSchema;
    this.fs = fs;
    this.readFooter = readFooter;
    this.estimatedRecordSize = estimatedRecordSize;
    this.compressionFactor = compressionFactor;
    this.maxLeafCols = (int) opContext.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX);
  }

  @Override
  public Footer getFooter(String path, long fileSize) throws IOException {
    MutableParquetMetadata parquetMetadata = this.readFooter ? readFooter(path, fileSize) : null;
    return new Footer(createBatchSchemaIfNeeded(parquetMetadata, path, fileSize), getRowCount(parquetMetadata, fileSize), FileFormat.PARQUET);
  }

  private long getRowCount(MutableParquetMetadata parquetMetadata, long fileSize) {
    Preconditions.checkState(!readFooter || parquetMetadata != null, "Unexpected state");
    if (parquetMetadata != null) {
      return parquetMetadata.getRowCount();
    }
    return getEstimatedRowCount(fileSize);
  }

  private long getEstimatedRowCount(long fileSize) {
    Preconditions.checkState(!readFooter, "Unexpected state");
    if (estimatedRecordSize == 0 || fileSize == 0) {
      return 0;
    }
    return (long)Math.ceil(fileSize * (compressionFactor / estimatedRecordSize));
  }

  private BatchSchema createBatchSchemaIfNeeded(MutableParquetMetadata parquetMetadata, String path, long fileSize) throws IOException {
    if (tableSchema != null) {
      return tableSchema;
    }
    return tableSchema = batchSchemaFromParquetFooter(parquetMetadata, path, fileSize);
  }


  private List<Field> getFieldsUsingParquetTypeHelper(MutableParquetMetadata footer) throws Exception {
    List<Field> fields = new ArrayList<>();
    final ParquetReaderUtility.DateCorruptionStatus dateStatus = ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;
    final SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
      .readInt96AsTimeStamp(opContext.getOptions().getOption(PARQUET_READER_INT96_AS_TIMESTAMP).getBoolVal())
      .dateCorruptionStatus(dateStatus)
      .build();

    for (Type parquetField : footer.getFileMetaData().getSchema().getFields()) {
      Optional<Field> dremioField = ParquetTypeHelper.toField(parquetField, schemaHelper);
      fields.add(dremioField.orElseThrow(() -> new UnsupportedOperationException(
        String.format("Could not convert the parquetField to dremioField using ParquetTypeHelper - %s", parquetField.toString()))));
    }
    return fields;
  }

  private BatchSchema batchSchemaFromParquetFooter(MutableParquetMetadata footer, String path, long fileSize) throws IOException {
    Schema arrowSchema;
    try {
      arrowSchema = DremioArrowSchema.fromMetaData(footer.getFileMetaData().getKeyValueMetaData());
    } catch (Exception e) {
      arrowSchema = null;
      logger.warn("Invalid Arrow Schema", e);
    }

    List<Field> fields;
    if (arrowSchema == null) {
      try {
        final SchemaConverter converter = new SchemaConverter(opContext.getOptions().getOption(PARQUET_READER_INT96_AS_TIMESTAMP).getBoolVal());
        arrowSchema = converter.fromParquet(footer.getFileMetaData().getSchema()).getArrowSchema();
        // Convert all the arrow fields to dremio fields
        fields = CompleteType.convertToDremioFields(arrowSchema.getFields());
      } catch (Exception e) {
        logger.warn("Cannot convert parquet schema to dremio schema using parquet-arrow schema converter.Trying using ParquetTypeHelper", e);
        try {
          fields = getFieldsUsingParquetTypeHelper(footer);
        } catch (Exception ex) {
          logger.warn("Cannot convert parquet schema to dremio schema using ParquetTypeHelper", ex);
          // Fall back to read the records in the parquet file to generate schema
          return getBatchSchemaFromReader(fs, path, fileSize, footer);
        }
      }
    } else {
      fields = new ArrayList<>(arrowSchema.getFields());
    }

    if (fields.size() > maxLeafCols) {
      logger.error("Parquet contains more columns than the limit. Number of columns {}, Max columns permitted {}", fields.size(), maxLeafCols);
      throw new ColumnCountTooLargeException(maxLeafCols);
    }
    return new BatchSchema(fields).handleUnions(true);
  }

  private BatchSchema getBatchSchemaFromReader(final FileSystem fs, final String path, long fileSize, MutableParquetMetadata mutableParquetMetadata) throws IOException {
    logger.debug("Reading records in the parquet file [{}] to generate schema", path);
    try (
      BufferAllocator sampleAllocator = opContext.getAllocator().newChildAllocator("RecordReadForSchema-alloc", 0, Long.MAX_VALUE);
      SampleMutator mutator = new SampleMutator(sampleAllocator)) {

      final CompressionCodecFactory codec = CodecFactory.createDirectCodecFactory(new Configuration(),
        new ParquetDirectByteBufferAllocator(sampleAllocator), 0);

      if (mutableParquetMetadata.getBlocks().size() == 0) {
        throw new IllegalArgumentException(String.format("parquet file [%s] has no blocks", path));
      }

      final boolean autoCorrectCorruptDates = opContext.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR);

      final ParquetReaderUtility.DateCorruptionStatus dateStatus = ParquetReaderUtility.detectCorruptDates(mutableParquetMetadata, GroupScan.ALL_COLUMNS,
        autoCorrectCorruptDates);

      final SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
        .readInt96AsTimeStamp(opContext.getOptions().getOption(PARQUET_READER_INT96_AS_TIMESTAMP).getBoolVal())
        .dateCorruptionStatus(dateStatus)
        .build();


      final long maxFooterLen = opContext.getOptions().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR);
      try (InputStreamProvider streamProvider = new SingleStreamProvider(fs, Path.of(path), fileSize, maxFooterLen, false, null, null, false);
           RecordReader reader = new AdditionalColumnsRecordReader(opContext, new ParquetRowiseReader(opContext, mutableParquetMetadata, 0,
             path, ParquetScanProjectedColumns.fromSchemaPaths(GroupScan.ALL_COLUMNS),
             fs, schemaHelper, streamProvider, codec, true), Lists.newArrayList(), sampleAllocator)) {

        reader.setup(mutator);

        mutator.allocate(100); // 100 is random; the method will ignore this parameter
        // Read the parquet file to populate inner list types
        reader.next();

        mutator.getContainer().buildSchema(BatchSchema.SelectionVectorMode.NONE);
        return mutator.getContainer().getSchema();
      } catch (Exception e) {
        throw new IOException(e.getMessage());
      }
    }
  }

  private MutableParquetMetadata readFooter(String path, long fileSize) throws IOException {
    logger.debug("Reading footer of file [{}]", path);
    try (SingleStreamProvider singleStreamProvider = new SingleStreamProvider(this.fs, Path.of(path), fileSize,
      maxFooterLen(), false, null, opContext, false)) {
      return singleStreamProvider.getFooter();
    }
  }

  private long maxFooterLen() {
    return opContext.getOptions().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR);
  }

}
