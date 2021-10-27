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

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.NoSupportedUpPromotionOrCoercionException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * FilteringCoercionReader for excel, json and mongo sources
 */
public class EasyCoercionReader extends FilteringFileCoercionReader {
  private static final Logger logger = LoggerFactory.getLogger(EasyCoercionReader.class);
  private final List<String> tableSchemaPath;

  public EasyCoercionReader(OperatorContext context, List<SchemaPath> columns, RecordReader inner,
                            BatchSchema targetSchema, List<String> tableSchemaPath) {
    super(context, columns, inner, targetSchema, toTypeCoercion(targetSchema));
    this.tableSchemaPath = tableSchemaPath;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    inner.setup(mutator); // this will modify filters in schema mismatch case
    incoming.buildSchema();
    // reset the schema change callback
    mutator.getAndResetSchemaChanged();

    final BatchSchema resolvedSchema = getFinalSchema(incoming.getSchema(), originalSchema);
    if (!resolvedSchema.equalsTypesWithoutPositions(originalSchema)) {
      notifySchemaChange(resolvedSchema, tableSchemaPath);
    }

    for (Field field : originalSchema.getFields()) {
      ValueVector vector = outputMutator.getVector(field.getName());
      if (vector == null) {
        continue;
      }
      outgoing.add(vector);
    }
    outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  @Override
  public int next() {
    recordCount = inner.next();
    if (recordCount == 0) {
      return 0;
    }
    if (mutator.getSchemaChanged()) {
      incoming.buildSchema();
      // reset the schema change callback
      mutator.getAndResetSchemaChanged();
      BatchSchema outgoingSchema = outgoing.getSchema();
      BatchSchema finalSchema = getFinalSchema(incoming.getSchema(), outgoingSchema);
      if (!finalSchema.equalsTypesWithoutPositions(outgoingSchema)) {
        notifySchemaChange(finalSchema, tableSchemaPath);

        // schema of field after merge is not same as original schema, remove old schema and add new one
        outgoingSchema.getFields().forEach(field -> outputMutator.removeField(field));
        finalSchema.getFields().forEach(field -> outputMutator.addField(field, CompleteType.fromField(field).getValueVectorClass()));
        outputMutator.getContainer().buildSchema();
        if (outputMutator.getCallBack() != null) {
          // set schema change flag
          outputMutator.getCallBack().doWork();
        }
        return 1;
      }
      resetProjector();
      setupProjector(this.outputMutator, projectorOutput);
    } else if (!initialProjectorSetUpDone) {
      setupProjector(this.outputMutator, projectorOutput);
    }
    initialProjectorSetUpDone = true;
    incoming.setAllCount(recordCount);
    runProjector(recordCount);
    projectorOutput.setAllCount(recordCount);
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

  private static FileTypeCoercion toTypeCoercion(BatchSchema targetSchema) {
    return new FileTypeCoercion(targetSchema.getFields().stream().collect(Collectors.toMap(Field::getName, field -> field)));
  }

  /**
   * Callback to run if the schema from a new batch or the schema that was evaluated differs from the original schema.
   */
  protected void notifySchemaChange(BatchSchema newSchema, List<String> tableSchemaPath) {
  }
}
