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
package com.dremio.exec.store;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.NoSupportedUpPromotionOrCoercionException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetTypeCoercion;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * FilteringCoercionReader for files
 */
public class EasyCoercionReader extends HiveParquetCoercionReader {
  private static final Logger logger = LoggerFactory.getLogger(EasyCoercionReader.class);
  private final List<String> tableSchemaPath;

  public EasyCoercionReader(OperatorContext context,
                            List<SchemaPath> columns,
                            RecordReader inner,
                            BatchSchema targetSchema,
                            List<String> tableSchemaPath,
                            List<ParquetFilterCondition> parqfilterConditions) {
    super(context, columns, inner, targetSchema, getParquetTypeCoercion(targetSchema), parqfilterConditions);
    this.tableSchemaPath = tableSchemaPath;
  }

  @Override
  public int next() {
    switch (nextMethodState) {
      case FIRST_CALL_BY_FILTERING_READER:
        // called by this.filteringReader. we just need to return number of records written by projector
        nextMethodState = NextMethodState.REPEATED_CALL_BY_FILTERING_READER;
        break;
      case NOT_CALLED_BY_FILTERING_READER:
        outgoingMutator = this.outputMutator;
        recordCount = inner.next();
        if (recordCount == 0) {
          return 0;
        }
        if (needsFilteringAfterCoercion) {
          projectorOutput = filteringReaderInputMutator.getContainer();
          this.outgoingMutator = filteringReaderInputMutator;
        }

        if (mutator.getSchemaChanged()) {
          incoming.buildSchema();
          // reset the schema change callback
          mutator.getAndResetSchemaChanged();

          BatchSchema outingSchema = outgoing.getSchema();
          BatchSchema finalSchema = getFinalSchema(incoming.getSchema(), outingSchema);
          if (!finalSchema.equalsTypesWithoutPositions(outingSchema)) {
            // schema of field after merge is not same as original schema, remove old schema and add new one
            outingSchema.getFields().forEach(field -> outputMutator.removeField(field));
            finalSchema.getFields().forEach(field -> outputMutator.addField(field, CompleteType.fromField(field).getValueVectorClass()));
            outputMutator.getContainer().buildSchema();
            if (outputMutator.getCallBack() != null) {
              // set schema change flag
              outputMutator.getCallBack().doWork();
            }
            return 1;
          }

          setupProjector(this.outgoingMutator, projectorOutput);
        } else if (!initialProjectorSetUpDone) {
          setupProjector(this.outgoingMutator, projectorOutput);
        }
        initialProjectorSetUpDone = true;
        incoming.setAllCount(recordCount);

        runProjector(recordCount);
        projectorOutput.setAllCount(recordCount);

        if (needsFilteringAfterCoercion) {
          nextMethodState = NextMethodState.FIRST_CALL_BY_FILTERING_READER;
          recordCount = filteringReader.next();
          outgoing.setAllCount(recordCount);
          int recordCountCopy = recordCount;
          resetReaderState();
          return recordCountCopy;
        }
        break;
      case REPEATED_CALL_BY_FILTERING_READER:
        recordCount = inner.next();
        if (recordCount == 0) {
          return 0;
        }

        if (mutator.getSchemaChanged()) {
          incoming.buildSchema();
          // reset the schema change callback
          mutator.getAndResetSchemaChanged();
          setupProjector(this.outgoingMutator, projectorOutput);
        }
        incoming.setAllCount(recordCount);

        runProjector(recordCount);
    }
    return recordCount;
  }

  private BatchSchema getFinalSchema(BatchSchema newSchema, BatchSchema outingSchema) {
    boolean mixedTypesDisabled = context.getOptions().getOption(ExecConstants.MIXED_TYPES_DISABLED);
    BatchSchema finalSchema;
    try {
      finalSchema = outingSchema.merge(newSchema, mixedTypesDisabled);
    } catch (NoSupportedUpPromotionOrCoercionException e) {
      e.addFilePath(this.inner.getFilePath());
      e.addDatasetPath(tableSchemaPath);
      throw UserException.unsupportedError().message(e.getMessage()).build(logger);
    }
    return finalSchema;
  }

  private static ParquetTypeCoercion getParquetTypeCoercion(BatchSchema targetSchema) {
    return new ParquetTypeCoercion(targetSchema.getFields().stream().collect(Collectors.toMap(field -> field.getName(), field -> field)));
  }
}
