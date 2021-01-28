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
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.Type;

import com.dremio.common.AutoCloseables;
import com.dremio.common.arrow.DremioArrowSchema;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SampleMutator;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;

/**
 * Parquet reader for Iceberg datasets. This will be an inner reader of a
 * coercion reader to support up promotion of column data types.
 */
public class IcebergParquetReader implements RecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergParquetReader.class);
  private final OperatorContext context;
  private final ParquetReaderFactory readerFactory;
  private final BatchSchema tableSchema;
  private final ParquetScanProjectedColumns projectedColumns;
  private final Map<String, GlobalDictionaryFieldInfo> globalDictionaryFieldInfoMap;
  private final List<ParquetFilterCondition> filterConditions;
  private final ParquetProtobuf.ParquetDatasetSplitScanXAttr readEntry;
  private final FileSystem fs;
  private final MutableParquetMetadata footer;
  private final GlobalDictionaries dictionaries;
  private final SchemaDerivationHelper schemaHelper;
  private final boolean vectorize;
  private final boolean enableDetailedTracing;
  private final boolean supportsColocatedReads;
  private final InputStreamProvider inputStreamProvider;
  private UnifiedParquetReader currentReader;

  public IcebergParquetReader(
    OperatorContext context,
    ParquetReaderFactory readerFactory,
    BatchSchema tableSchema,
    ParquetScanProjectedColumns projectedColumns,
    Map<String, GlobalDictionaryFieldInfo> globalDictionaryFieldInfoMap,
    List<ParquetFilterCondition> filterConditions,
    ParquetProtobuf.ParquetDatasetSplitScanXAttr readEntry,
    FileSystem fs,
    MutableParquetMetadata footer,
    GlobalDictionaries dictionaries,
    SchemaDerivationHelper schemaHelper,
    boolean vectorize,
    boolean enableDetailedTracing,
    boolean supportsColocatedReads,
    InputStreamProvider inputStreamProvider) {
    this.context = context;
    this.readerFactory = readerFactory;
    this.tableSchema = tableSchema;
    this.projectedColumns = projectedColumns;
    this.globalDictionaryFieldInfoMap = globalDictionaryFieldInfoMap;
    this.filterConditions = filterConditions;
    this.readEntry = readEntry;
    this.fs = fs;
    this.footer = footer;
    this.dictionaries = dictionaries;
    this.schemaHelper = schemaHelper;
    this.vectorize = vectorize;
    this.enableDetailedTracing = enableDetailedTracing;
    this.supportsColocatedReads = supportsColocatedReads;
    this.inputStreamProvider = inputStreamProvider;
  }


  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    Preconditions.checkArgument(output instanceof SampleMutator, "Unexpected output mutator");
    ParquetColumnResolver columnResolver = projectedColumns.getColumnResolver(footer.getFileMetaData().getSchema());

    Schema arrowSchema;
    try {
      arrowSchema = DremioArrowSchema.fromMetaData(footer.getFileMetaData().getKeyValueMetaData());
    } catch (Exception e) {
      arrowSchema = null;
      logger.warn("Invalid Arrow Schema", e);
    }

    // create output vector based on schema in parquet file
    for (Type parquetField : footer.getFileMetaData().getSchema().getFields()) {
      SchemaPath columnSchemaPath = SchemaPath.getCompoundPath(parquetField.getName());
      for (SchemaPath projectedPath : columnResolver.getProjectedParquetColumns()) {
        String name = projectedPath.getRootSegment().getNameSegment().getPath();
        if (parquetField.getName().equalsIgnoreCase(name)) {
          if (parquetField.isPrimitive()) {
            Field field = ParquetTypeHelper.createField(columnResolver.getBatchSchemaColumnPath(columnSchemaPath),
              parquetField.asPrimitiveType(), parquetField.getOriginalType(), schemaHelper);
            final Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(field);
            output.addField(field, clazz);
          } else {
            Preconditions.checkState(arrowSchema != null, "Invalid parquet schema");
            Field groupField = arrowSchema.findField(parquetField.getName());
            List<Field> arrowField = new ArrayList<>();
            arrowField.add(groupField);
            List<Field> dremioField = CompleteType.convertToDremioFields(arrowField);
            Field dremioGroupField = dremioField.get(0);
            Field field = new Field(columnResolver.getBatchSchemaColumnName(parquetField.getName()), true,
              dremioGroupField.getType(), dremioGroupField.getChildren());
            final Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(field);
            output.addField(field, clazz);
          }
          break;
        }
      }
    }
    ((SampleMutator)output).getContainer().buildSchema();
    output.getAndResetSchemaChanged();

    currentReader = new UnifiedParquetReader(
      context,
      readerFactory,
      this.tableSchema,
      projectedColumns,
      this.globalDictionaryFieldInfoMap,
      this.filterConditions,
      readerFactory.newFilterCreator(context, ParquetReaderFactory.ManagedSchemaType.ICEBERG, null, context.getAllocator()),
      ParquetDictionaryConvertor.DEFAULT,
      this.readEntry,
      fs,
      footer,
      this.dictionaries,
      schemaHelper,
      vectorize,
      enableDetailedTracing,
      supportsColocatedReads,
      inputStreamProvider,
      new ArrayList<>());
    currentReader.setIgnoreSchemaLearning(true);
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
    if (runtimeFilter != null) {
      this.currentReader.addRuntimeFilter(runtimeFilter);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(currentReader);
  }
}
