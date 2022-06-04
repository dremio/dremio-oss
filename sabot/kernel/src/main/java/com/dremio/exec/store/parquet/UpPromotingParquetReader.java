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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SampleMutator;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitScanXAttr;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.google.common.base.Preconditions;

/**
 * Parquet reader for datasets. This will be an inner reader of a
 * coercion reader to support up promotion of column data types.
 */
public class UpPromotingParquetReader implements RecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UpPromotingParquetReader.class);
  private final FileSystem fs;
  private final boolean vectorize;
  private final OperatorContext context;
  private final BatchSchema tableSchema;
  private final MutableParquetMetadata footer;
  private final boolean enableDetailedTracing;
  private final boolean supportsColocatedReads;
  private final GlobalDictionaries dictionaries;
  private final ParquetReaderFactory readerFactory;
  private final SchemaDerivationHelper schemaHelper;
  private final ParquetColumnResolver columnResolver;
  private final ParquetDatasetSplitScanXAttr readEntry;
  private final InputStreamProvider inputStreamProvider;
  private final ParquetScanProjectedColumns projectedColumns;
  private final List<ParquetFilterCondition> filterConditions;
  private final Map<String, GlobalDictionaryFieldInfo> globalDictionaryFieldInfoMap;
  private final String filePath;
  private final List<String> tableSchemaPath;
  private final boolean isSchemaLearningDisabledByUser;
  private UnifiedParquetReader currentReader;
  private List<Field> droppedColumns = Collections.emptyList();
  private List<Field> updatedColumns = Collections.emptyList();

  public UpPromotingParquetReader(OperatorContext context, ParquetReaderFactory readerFactory,
                                  BatchSchema tableSchema, ParquetScanProjectedColumns projectedColumns,
                                  Map<String, GlobalDictionaryFieldInfo> globalDictionaryFieldInfoMap,
                                  List<ParquetFilterCondition> filterConditions, ParquetDatasetSplitScanXAttr readEntry,
                                  FileSystem fs, MutableParquetMetadata footer, String filePath, List<String> tableSchemaPath, GlobalDictionaries dictionaries,
                                  SchemaDerivationHelper schemaHelper, boolean vectorize, boolean enableDetailedTracing,
                                  boolean supportsColocatedReads, InputStreamProvider inputStreamProvider, UserDefinedSchemaSettings userDefinedSchemaSettings) {
    this.fs = fs;
    this.footer = footer;
    this.filePath = filePath;
    this.tableSchemaPath = tableSchemaPath;
    this.context = context;
    this.vectorize = vectorize;
    this.readEntry = readEntry;
    this.tableSchema = tableSchema;
    this.dictionaries = dictionaries;
    this.schemaHelper = schemaHelper;
    this.readerFactory = readerFactory;
    this.projectedColumns = projectedColumns;
    this.filterConditions = filterConditions;
    this.inputStreamProvider = inputStreamProvider;
    this.enableDetailedTracing = enableDetailedTracing;
    this.supportsColocatedReads = supportsColocatedReads;
    this.globalDictionaryFieldInfoMap = globalDictionaryFieldInfoMap;
    this.columnResolver = projectedColumns.getColumnResolver(footer.getFileMetaData().getSchema());
    if (userDefinedSchemaSettings != null && userDefinedSchemaSettings.getDroppedColumns() != null) {
      droppedColumns = BatchSchema.deserialize(userDefinedSchemaSettings.getDroppedColumns()).getFields();
    }
    if (userDefinedSchemaSettings != null && userDefinedSchemaSettings.getModifiedColumns() != null) {
      updatedColumns = BatchSchema.deserialize(userDefinedSchemaSettings.getModifiedColumns()).getFields();
    }
    this.isSchemaLearningDisabledByUser = userDefinedSchemaSettings != null && !userDefinedSchemaSettings.getSchemaLearningEnabled();
  }

  public void setupMutator(OutputMutator outputMutator) {
    MutatorSetupManager mutatorSetupManager = new MutatorSetupManager(context, tableSchema, footer, filePath, tableSchemaPath, schemaHelper, columnResolver);
    AdditionalColumnResolver additionalColumnResolver = new AdditionalColumnResolver(tableSchema, columnResolver);

    logger.info("F[setupMutator] Footer size is {} for file {}, current row group index is {}",(footer.getBlocks() == null)? -1:footer.getBlocks().size(), filePath, readEntry.getRowGroupIndex());
    BlockMetaData block = footer.getBlocks().get(readEntry.getRowGroupIndex());
    Collection<SchemaPath> resolvedColumns = additionalColumnResolver.resolveColumns(block.getColumns());
    mutatorSetupManager.setupMutator(outputMutator, resolvedColumns, droppedColumns, updatedColumns, isSchemaLearningDisabledByUser);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    Preconditions.checkArgument(output instanceof SampleMutator, "Unexpected output mutator");

    List<SchemaPath> projectedParquetColumns = columnResolver.getProjectedParquetColumns();
    OutputMutatorHelper.addFooterFieldsToOutputMutator(output, schemaHelper, footer, projectedParquetColumns);

    this.currentReader = new UnifiedParquetReader(
      context,
      readerFactory,
      tableSchema,
      projectedColumns,
      globalDictionaryFieldInfoMap,
      filterConditions,
      readerFactory.newFilterCreator(context, null, null, context.getAllocator()),
      ParquetDictionaryConvertor.DEFAULT,
      readEntry,
      fs,
      footer,
      dictionaries,
      schemaHelper,
      vectorize,
      enableDetailedTracing,
      supportsColocatedReads,
      inputStreamProvider,
      new ArrayList<>());
    currentReader.setup(output);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    if (currentReader == null) {
      return;
    }
    currentReader.allocate(vectorMap);
  }

  @Override
  public int next() {
    if (currentReader == null) {
      return 0;
    }

    return currentReader.next();
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    if (this.currentReader != null) {
      this.currentReader.addRuntimeFilter(runtimeFilter);
    }
  }

  @Override
  public void close() throws Exception {
    if (currentReader != null) {
      AutoCloseables.close(currentReader);
      currentReader = null;
    } else {
      AutoCloseables.close(inputStreamProvider);
    }
  }

  @Override
  public List<SchemaPath> getColumnsToBoost() {
    return currentReader.getColumnsToBoost();
  }
}
