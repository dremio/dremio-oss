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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.SupportsTypeCoercionsAndUpPromotions;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** FilteringCoercionReader for excel, json and mongo sources */
public class EasyCoercionReader extends FilteringFileCoercionReader
    implements SupportsTypeCoercionsAndUpPromotions {
  private static final Logger logger = LoggerFactory.getLogger(EasyCoercionReader.class);
  protected final List<String> tableSchemaPath;
  protected final boolean isSchemaLearningDisabledByUser;
  protected List<Field> droppedColumns = Collections.emptyList();
  protected List<Field> updatedColumns = Collections.emptyList();

  public EasyCoercionReader(
      OperatorContext context,
      List<SchemaPath> columns,
      RecordReader inner,
      BatchSchema targetSchema,
      List<String> tableSchemaPath,
      UserDefinedSchemaSettings userDefinedSchemaSettings) {
    super(context, columns, inner, targetSchema, toTypeCoercion(targetSchema));
    this.tableSchemaPath = tableSchemaPath;
    if (userDefinedSchemaSettings != null
        && userDefinedSchemaSettings.getDroppedColumns() != null) {
      droppedColumns =
          BatchSchema.deserialize(userDefinedSchemaSettings.getDroppedColumns()).getFields();
    }
    if (userDefinedSchemaSettings != null
        && userDefinedSchemaSettings.getModifiedColumns() != null) {
      updatedColumns =
          BatchSchema.deserialize(userDefinedSchemaSettings.getModifiedColumns()).getFields();
    }
    this.isSchemaLearningDisabledByUser =
        userDefinedSchemaSettings != null && !userDefinedSchemaSettings.getSchemaLearningEnabled();
  }

  public EasyCoercionReader(
      OperatorContext context,
      List<SchemaPath> columns,
      RecordReader inner,
      BatchSchema targetSchema,
      List<String> tableSchemaPath) {
    this(context, columns, inner, targetSchema, tableSchemaPath, null);
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
      notifySchemaChange(originalSchema, resolvedSchema, tableSchemaPath);
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
      BatchSchema outgoingSchema = outgoing.getSchema().removeNullFields();
      BatchSchema finalSchema = getFinalSchema(incoming.getSchema(), outgoingSchema);
      if (!finalSchema.equalsTypesWithoutPositions(outgoingSchema)) {
        notifySchemaChange(outgoingSchema, finalSchema, tableSchemaPath);

        // schema of field after merge is not same as original schema, remove old schema and add new
        // one
        outgoingSchema.getFields().forEach(field -> outputMutator.removeField(field));
        finalSchema
            .getFields()
            .forEach(
                field ->
                    outputMutator.addField(
                        field, CompleteType.fromField(field).getValueVectorClass()));
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

  protected BatchSchema getFinalSchema(BatchSchema newSchema, BatchSchema outgoingSchema) {
    boolean isUserDefinedSchemaEnabled =
        context.getOptions().getOption(ExecConstants.ENABLE_INTERNAL_SCHEMA);
    return outgoingSchema.applyUserDefinedSchemaAfterSchemaLearning(
        newSchema,
        droppedColumns,
        updatedColumns,
        isSchemaLearningDisabledByUser,
        isUserDefinedSchemaEnabled,
        this.inner.getFilePath(),
        tableSchemaPath,
        this);
  }

  private static FileTypeCoercion toTypeCoercion(BatchSchema targetSchema) {
    return new FileTypeCoercion(
        targetSchema.getFields().stream()
            .collect(Collectors.toMap(Field::getName, field -> field)));
  }

  /**
   * Callback to run if the schema from a new batch or the schema that was evaluated differs from
   * the original schema.
   */
  protected void notifySchemaChange(
      BatchSchema originalSchema, BatchSchema newSchema, List<String> tableSchemaPath) {}
}
