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

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties.Property;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.TypeCoercion;
import com.dremio.exec.store.copyinto.CopyIntoTransformationCompositeReader;
import com.dremio.exec.store.parquet.ParquetCoercionReader;
import com.dremio.exec.store.parquet.ParquetFilters;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Stopwatch;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.tuple.Pair;

public class CopyIntoTransformationParquetCoercionReader extends ParquetCoercionReader {

  protected final CopyIntoTransformationProperties copyIntoTransformationProperties;
  private final Map<String, String> renamedFieldNameMap = new HashMap<>();
  private final SampleMutator projectorIncomingMutator;
  private VectorContainer projectorIncoming;
  private final boolean isCopyIntoTransformations;

  protected CopyIntoTransformationParquetCoercionReader(
      OperatorContext context,
      List<SchemaPath> columns,
      RecordReader inner,
      BatchSchema originalSchema,
      TypeCoercion typeCoercion,
      ParquetFilters filters,
      CopyIntoTransformationProperties copyIntoTransformationProperties,
      BatchSchema targetSchema) {
    super(context, columns, inner, originalSchema, typeCoercion, filters);
    this.copyIntoTransformationProperties = copyIntoTransformationProperties;
    this.isCopyIntoTransformations = copyIntoTransformationProperties != null;
    this.projectorIncomingMutator = new SampleMutator(context.getAllocator());
    if (isCopyIntoTransformations) {
      this.compositeReader =
          new CopyIntoTransformationCompositeReader(
              projectorIncomingMutator,
              context,
              typeCoercion,
              Stopwatch.createUnstarted(),
              Stopwatch.createUnstarted(),
              originalSchema,
              copyIntoTransformationProperties.getProperties(),
              targetSchema,
              renamedFieldNameMap,
              0);
    }
  }

  public static RecordReader newInstance(
      OperatorContext context,
      List<SchemaPath> columns,
      RecordReader inner,
      BatchSchema originalSchema,
      TypeCoercion typeCoercion,
      ParquetFilters filters,
      CopyIntoTransformationProperties copyIntoTransformationProperties,
      BatchSchema targetSchema) {
    if (copyIntoTransformationProperties != null) {
      return new CopyIntoTransformationParquetCoercionReader(
          context,
          columns,
          inner,
          originalSchema,
          typeCoercion,
          filters,
          copyIntoTransformationProperties,
          targetSchema);
    }

    return ParquetCoercionReader.newInstance(
        context, columns, inner, originalSchema, typeCoercion, filters);
  }

  @Override
  protected void setupProjector(OutputMutator outgoing, VectorContainer projectorOutput) {
    if (isCopyIntoTransformations) {
      prepareProjectorInput();
      compositeReader.setupProjector(
          outgoing, projectorIncoming, projectorOptions, projectorOutput);
      outgoing.getAndResetSchemaChanged();
    } else {
      super.setupProjector(outgoing, projectorOutput);
    }
  }

  @Override
  protected void runProjector(int recordCount) {
    if (isCopyIntoTransformations) {
      compositeReader.runProjector(recordCount, projectorIncoming);
    } else {
      super.runProjector(recordCount);
    }
  }

  @Override
  protected void prepareOutgoing() {
    if (isCopyIntoTransformations) {
      copyIntoTransformationProperties.getProperties().stream()
          .map(Property::getTargetColName)
          .map(outputMutator::getVector)
          .filter(Objects::nonNull)
          .forEach(outgoing::add);
      outgoing.buildSchema(SelectionVectorMode.NONE);
    } else {
      super.prepareOutgoing();
    }
  }

  /**
   * Prepares the input for the projector by creating and configuring a transformation container
   * with renamed fields. The method updates the {@code projectorIncoming} with the transformed
   * schema and data.
   */
  private void prepareProjectorInput() {
    try (VectorContainer transformationContainer = new VectorContainer(context.getAllocator())) {
      BatchSchema incomingSchema = incoming.getSchema();
      // If the incoming schema is empty, initialize the projector with an empty schema and return.
      if (incomingSchema.getFields().isEmpty()) {
        projectorIncoming = projectorIncomingMutator.getContainer();
        projectorIncoming.buildSchema();
        projectorIncoming.setRecordCount(0);
        return;
      }
      // Build a new schema with renamed fields.
      SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
      for (Field field : incomingSchema.getFields()) {
        Field renamedField =
            new Field(
                (ColumnUtils.VIRTUAL_COLUMN_PREFIX + field.getName()).toLowerCase(),
                field.getFieldType(),
                field.getChildren());

        renamedFieldNameMap.put(field.getName(), renamedField.getName());
        schemaBuilder.addField(renamedField);
      }
      BatchSchema transformationSchema = schemaBuilder.build();
      // Add the transformed schema to the container and build it.
      transformationContainer.addSchema(transformationSchema);
      transformationContainer.buildSchema();

      // Add fields to the projector's mutator and transfer data from the original vectors.
      transformationSchema.getFields().stream()
          .map(
              f ->
                  Pair.of(
                      f, VectorUtil.getVectorFromSchemaPath(transformationContainer, f.getName())))
          .forEach(p -> projectorIncomingMutator.addField(p.getLeft(), p.getRight().getClass()));

      renamedFieldNameMap.entrySet().stream()
          .map(
              e ->
                  Pair.of(
                      mutator.getVector(e.getKey()),
                      projectorIncomingMutator.getVector(e.getValue())))
          .forEach(p -> p.getLeft().makeTransferPair(p.getRight()).transfer());
    }

    // Set up the projector's incoming container with the transformed data.
    projectorIncoming = projectorIncomingMutator.getContainer();
    projectorIncoming.buildSchema();
    projectorIncoming.setRecordCount(incoming.getRecordCount());
  }

  @Override
  public void close() throws Exception {
    super.close();
    AutoCloseables.close(projectorIncomingMutator, mutator);
  }
}
