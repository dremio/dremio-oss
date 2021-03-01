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
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.schema.Type;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SampleMutator;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;

/**
 * Parquet reader for DeltaLake datasets. This will be an inner reader of a
 * coercion reader to support up promotion of column data types.
 */
public class DeltaLakeParquetReader extends TransactionalTableParquetReader {

  public DeltaLakeParquetReader(
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
    super(context, readerFactory, tableSchema, projectedColumns, globalDictionaryFieldInfoMap, filterConditions,
            readEntry, fs, footer, dictionaries, schemaHelper, vectorize, enableDetailedTracing, supportsColocatedReads,
            inputStreamProvider);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    Preconditions.checkArgument(output instanceof SampleMutator, "Unexpected output mutator");
    ParquetColumnResolver columnResolver = projectedColumns.getColumnResolver(footer.getFileMetaData().getSchema());

    // create output vector based on schema in parquet file
    for (Type parquetField : footer.getFileMetaData().getSchema().getFields()) {
      for (SchemaPath projectedPath : columnResolver.getProjectedParquetColumns()) {
        final String name = projectedPath.getRootSegment().getNameSegment().getPath();
        if (!parquetField.getName().equalsIgnoreCase(name)) {
          continue;
        }
        final Optional<Field> field = ParquetTypeHelper.toField(parquetField, schemaHelper);
        if (!field.isPresent()) {
          break;
        }
        final Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(field.get());
        output.addField(field.get(), clazz);
        break;
      }
    }
    ((SampleMutator) output).getContainer().buildSchema();
    output.getAndResetSchemaChanged();
    setupCurrentReader(output);
  }
}
