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

import static com.dremio.exec.util.ColumnUtils.ROW_INDEX_COLUMN_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.junit.AfterClass;
import org.junit.Before;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.io.Resources;

public class BaseTestUnifiedParquetReader extends BaseTestOperator {

  protected OperatorContextImpl context;
  protected FileSystem fs;
  protected static final List<AutoCloseable> classCloseables = new ArrayList<>();

  protected static final int DEFAULT_BATCH_SIZE = 100;

  @Before
  public void beforeTest() throws Exception {
    context = testContext.getNewOperatorContext(getTestAllocator(), null, getBatchSize(), null);
    testCloseables.add(context);
    fs = HadoopFileSystem.get(Path.of("/"), new Configuration(), context.getStats());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    AutoCloseables.close(classCloseables);
  }

  protected InputStreamProviderFactory getInputStreamProviderFactory() {
    return InputStreamProviderFactory.DEFAULT;
  }

  protected ParquetReaderFactory getParquetReaderFactory() {
    return ParquetReaderFactory.NONE;
  }

  protected int getBatchSize() {
    return DEFAULT_BATCH_SIZE;
  }

  public static Path getParquetFileFromResource(String resourceName) throws IOException {
    File dest = Files.createTempFile("TestUnifiedParquetReader", ".parquet").toFile();
    Files.copy(Resources.getResource("parquet/" + resourceName).openStream(),
        dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
    classCloseables.add(() -> dest.delete());
    return Path.of(dest.getAbsolutePath());
  }

  protected int readAndValidate(
      Path path,
      ParquetFilters filters,
      List<String> projectedColumns,
      ParquetReaderOptions parquetReaderOptions,
      RecordBatchValidator validator) throws Exception {
    return readAndValidate(path, filters, null, projectedColumns, parquetReaderOptions, validator);
  }

  protected int readAndValidate(
      Path path,
      ParquetFilters filters,
      List<RuntimeFilter> runtimeFilters,
      List<String> projectedColumns,
      ParquetReaderOptions parquetReaderOptions,
      RecordBatchValidator validator) throws Exception {

    ParquetScanProjectedColumns parquetScanProjectedColumns = getProjectedColumns(projectedColumns);
    FileAttributes fileAttributes = fs.getFileAttributes(path);
    InputStreamProvider inputStreamProvider = createInputStreamProvider(
        path,
        fileAttributes,
        0,
        null,
        null,
        parquetScanProjectedColumns);
    MutableParquetMetadata footer = inputStreamProvider.getFooter();
    BatchSchema schema = getSchema(inputStreamProvider.getFooter(), projectedColumns);

    int totalRecords = 0;
    for (int rowGrpIdx = 0; rowGrpIdx < footer.getBlocks().size(); rowGrpIdx++) {
      if (rowGrpIdx > 0) {
        inputStreamProvider = createInputStreamProvider(
            path,
            fileAttributes,
            rowGrpIdx,
            inputStreamProvider,
            footer,
            parquetScanProjectedColumns);
      }

      try (
          UnifiedParquetReader reader = createReader(
              inputStreamProvider,
              fileAttributes,
              rowGrpIdx,
              projectedColumns,
              schema,
              filters,
              runtimeFilters,
              parquetReaderOptions);
          TestOutputMutator outputMutator = new TestOutputMutator(getTestAllocator())) {
        // don't materialize projected columns like row num here to simulate how it works in the production stack
        schema.materializeVectors(getProjectedSchemaPathsWithoutPopulatedCols(projectedColumns), outputMutator);
        reader.setup(outputMutator);
        reader.allocate(outputMutator.getFieldVectorMap());

        int records;
        while ((records = reader.next()) > 0) {
          validator.validate(rowGrpIdx, totalRecords, records, outputMutator);
          totalRecords += records;
        }
      }
    }

    return totalRecords;
  }

  private UnifiedParquetReader createReader(
      InputStreamProvider inputStreamProvider,
      FileAttributes fileAttributes,
      int rowGroupIndex,
      List<String> projectedColumns,
      BatchSchema schema,
      ParquetFilters filters,
      List<RuntimeFilter> runtimeFilters,
      ParquetReaderOptions parquetReaderOptions)
      throws Exception {

    ParquetProtobuf.ParquetDatasetSplitScanXAttr readEntry = ParquetProtobuf.ParquetDatasetSplitScanXAttr.newBuilder()
        .setPath(fileAttributes.getPath().toString())
        .setStart(0)
        .setLength(fileAttributes.size())
        .setFileLength(fileAttributes.size())
        .setLastModificationTime(fileAttributes.lastModifiedTime().toMillis())
        .setRowGroupIndex(rowGroupIndex)
        .build();

    ParquetScanProjectedColumns parquetScanProjectedColumns = getProjectedColumns(projectedColumns);
    SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
        .noSchemaLearning(schema)
        .readInt96AsTimeStamp(parquetReaderOptions.isReadInt96AsTimestampEnabled())
        .dateCorruptionStatus(ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_NO_CORRUPTION)
        .mapDataTypeEnabled(context.getOptions().getOption(ExecConstants.ENABLE_MAP_DATA_TYPE))
        .build();

    ParquetReaderFactory parquetReaderFactory = getParquetReaderFactory();
    UnifiedParquetReader reader = new UnifiedParquetReader(
        context,
        parquetReaderFactory,
        schema,
        parquetScanProjectedColumns,
        null,
        filters,
        parquetReaderFactory.newFilterCreator(context, ParquetReaderFactory.ManagedSchemaType.ICEBERG, null,
            context.getAllocator()),
        ParquetDictionaryConvertor.DEFAULT,
        readEntry,
        fs,
        inputStreamProvider.getFooter(),
        null,
        schemaHelper,
        parquetReaderOptions.isVectorizationEnabled(),
        parquetReaderOptions.isDetailedTracingEnabled(),
        false,
        inputStreamProvider,
        runtimeFilters == null ? new ArrayList<>() : runtimeFilters,
        false);
    reader.setIgnoreSchemaLearning(true);

    return reader;
  }

  private InputStreamProvider createInputStreamProvider(
      Path path,
      FileAttributes fileAttributes,
      int rowGroupIndex,
      InputStreamProvider currentInputStreamProvider,
      MutableParquetMetadata footer,
      ParquetScanProjectedColumns projectedColumns) throws Exception {
    InputStreamProvider inputStreamProvider = getInputStreamProviderFactory().create(
        fs,
        context,
        path,
        fileAttributes.size(),
        fileAttributes.size(),
        projectedColumns,
        footer,
        currentInputStreamProvider,
        f -> rowGroupIndex,
        false,
        null,
        fileAttributes.lastModifiedTime().toMillis(),
        false,
        true, ParquetFilters.NONE,
        getParquetReaderFactory().newFilterCreator(context, ParquetReaderFactory.ManagedSchemaType.ICEBERG, null, context.getAllocator()),
        InputStreamProviderFactory.DEFAULT_NON_PARTITION_COLUMN_RF);

    testCloseables.add(inputStreamProvider);
    return inputStreamProvider;
  }

  private BatchSchema getSchema(MutableParquetMetadata footer, List<String> projectedColumns) {
    final SchemaConverter converter = new SchemaConverter(false);
    Schema schema = converter.fromParquet(footer.getFileMetaData().getSchema()).getArrowSchema();
    List<Field> fields = CompleteType.convertToDremioFields(schema.getFields());
    SchemaBuilder builder = BatchSchema.newBuilder();
    builder.addFields(fields);
    if (projectedColumns.contains(ROW_INDEX_COLUMN_NAME)) {
      builder.addField(Field.nullable(ROW_INDEX_COLUMN_NAME, new ArrowType.Int(64, true)));
    }
    return builder.build();
  }

  private List<SchemaPath> getProjectedSchemaPaths(List<String> colNames) {
    return colNames.stream().map(SchemaPath::getSimplePath).collect(Collectors.toList());
  }

  private List<SchemaPath> getProjectedSchemaPathsWithoutPopulatedCols(List<String> colNames) {
    return colNames.stream()
        .filter(n -> !n.equalsIgnoreCase(ROW_INDEX_COLUMN_NAME))
        .map(SchemaPath::getSimplePath)
        .collect(Collectors.toList());
  }

  private ParquetScanProjectedColumns getProjectedColumns(List<String> colNames) {
    List<SchemaPath> projectedCols = getProjectedSchemaPaths(colNames);
    return ParquetScanProjectedColumns.fromSchemaPaths(projectedCols);
  }

  protected interface RecordBatchValidator {
    void validate(int rowGroupIndex, int outputRowIndex, int records, OutputMutator mutator);
  }
}
