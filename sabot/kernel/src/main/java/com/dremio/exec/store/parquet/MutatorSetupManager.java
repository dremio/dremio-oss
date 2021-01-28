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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.arrow.DremioArrowSchema;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * A manager to set up {@link OutputMutator}.
 */
public class MutatorSetupManager {
  private static final Logger logger = LoggerFactory.getLogger(IcebergParquetReader.class);

  private final OperatorContext context;
  private final BatchSchema tableSchema;
  private final MutableParquetMetadata footer;
  private final SchemaDerivationHelper schemaHelper;
  private final ParquetColumnResolver columnResolver;

  public MutatorSetupManager(OperatorContext context, BatchSchema tableSchema, MutableParquetMetadata footer,
                             SchemaDerivationHelper schemaHelper, ParquetColumnResolver columnResolver) {
    this.context = context;
    this.tableSchema = tableSchema;
    this.footer = footer;
    this.schemaHelper = schemaHelper;
    this.columnResolver = columnResolver;
  }

  public void setupMutator(OutputMutator outputMutator, Collection<SchemaPath> resolvedColumns) {
    Map<String, Type> parquetTypeMap = new HashMap<>();
    for (Type field : footer.getFileMetaData().getSchema().getFields()) {
      parquetTypeMap.putIfAbsent(field.getName().toLowerCase(), field);
    }

    Schema arrowSchema = null;
    try {
      arrowSchema = DremioArrowSchema.fromMetaData(footer.getFileMetaData().getKeyValueMetaData());
    } catch (IOException e) {
      logger.warn("Invalid Arrow Schema", e);
    }

    for (SchemaPath schemaPath : resolvedColumns) {
      final String name = schemaPath.getRootSegment().getPath();
      Type parquetType = parquetTypeMap.get(name.toLowerCase());

      Optional<Field> fieldFromParquet = Optional.empty();
      if (arrowSchema != null) {
        // We're reading a parquet file written by Dremio
        Field field;
        try {
          field = arrowSchema.findField(name);
        } catch (Exception e) {
          field = null;
        }
        fieldFromParquet = Optional.ofNullable(field);
      } else if (parquetType != null) {
        fieldFromParquet = ParquetTypeHelper.toField(parquetType, schemaHelper);
      }

      if (!fieldFromParquet.isPresent()) {
        continue;
      }

      String schemaName = columnResolver.getBatchSchemaColumnName(name);
      Optional<Field> fieldFromBatchSchema = tableSchema.findFieldIgnoreCase(schemaName);
      if (!fieldFromBatchSchema.isPresent()) {
        outputMutator.addField(fieldFromParquet.get(), CompleteType.fromField(fieldFromParquet.get()).getValueVectorClass());
        continue;
      }

      BatchSchema schemaFromBatchField = BatchSchema.of(fieldFromBatchSchema.get());
      BatchSchema schemaFromParquetField = BatchSchema.of(fieldFromParquet.get());

      boolean mixedTypesDisabled = context.getOptions().getOption(ExecConstants.MIXED_TYPES_DISABLED);
      BatchSchema finalSchema = schemaFromBatchField.merge(schemaFromParquetField, mixedTypesDisabled);
      if (!finalSchema.equalsTypesWithoutPositions(schemaFromBatchField)) {
        // schema of field after merge is not same as original schema, remove old schema and add new one
        outputMutator.removeField(fieldFromBatchSchema.get());
        Field mergedField = finalSchema.findField(schemaName);
        outputMutator.addField(mergedField, CompleteType.fromField(mergedField).getValueVectorClass());
        if (outputMutator.getCallBack() != null) {
          // set schema change flag
          outputMutator.getCallBack().doWork();
        }
      }
    }
  }
}