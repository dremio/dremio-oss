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
package com.dremio.exec.store.copyinto;

import com.dremio.common.expression.BasePath;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties.Property;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.ComplexTypeCopiers;
import com.dremio.exec.store.ComplexTypeReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.TypeCoercion;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.vector.ValueVector;

public class CopyIntoTransformationComplexTypeReader extends ComplexTypeReader {

  private final List<CopyIntoTransformationProperties.Property> transformProperties;
  private final BatchSchema targetSchema;
  private final Map<String, String> renamedFieldNameMap;

  public CopyIntoTransformationComplexTypeReader(
      OperatorContext context,
      SampleMutator mutator,
      TypeCoercion typeCoercion,
      Stopwatch javaCodeGenWatch,
      Stopwatch gandivaCodeGenWatch,
      List<CopyIntoTransformationProperties.Property> transformationProperties,
      BatchSchema targetSchema,
      Map<String, String> renamedFieldNameMap,
      int depth) {
    super(context, mutator, typeCoercion, javaCodeGenWatch, gandivaCodeGenWatch, depth);
    this.targetSchema = targetSchema;
    this.transformProperties = transformationProperties;
    this.renamedFieldNameMap = renamedFieldNameMap;
  }

  @Override
  protected void createCopiers() {
    ArrayList<ValueVector> outComplexVectors = new ArrayList<>();
    ArrayList<ValueVector> inComplexVectors = new ArrayList<>();
    ArrayList<Integer> outFieldIds = new ArrayList<>();

    Preconditions.checkState(outputMutator != null, "Invalid state");
    VectorContainer outputContainer = outputMutator.getContainer();
    if (!outputContainer.hasSchema()) {
      outputContainer.buildSchema();
    }
    Iterator<ValueVector> outVectorIterator = outputMutator.getVectors().iterator();
    Map<String, ValueVector> inFieldMap = mutator.getFieldVectorMap();

    while (outVectorIterator.hasNext()) {
      ValueVector outV = outVectorIterator.next();
      if (!CompleteType.fromField(outV.getField()).isComplex()) {
        continue;
      }
      outFieldIds.add(
          outputContainer
              .getSchema()
              .getFieldId(BasePath.getSimple(outV.getName()))
              .getFieldIds()[0]);
      Optional<Property> transformationProperty =
          transformProperties.stream()
              .filter(p -> p.getTargetColName().equalsIgnoreCase(outV.getField().getName()))
              .findAny();
      if (transformationProperty.isEmpty()) {
        continue;
      }
      Optional<ValueVector> inV =
          transformationProperty.get().getSourceColNames().stream()
              .map(name -> inFieldMap.get(renamedFieldNameMap.get(name)))
              .findFirst();
      if (inV.isEmpty()) {
        continue;
      }
      inComplexVectors.add(inV.get());
      outComplexVectors.add(outV);
    }

    copiers =
        ComplexTypeCopiers.createCopiers(
            context,
            inComplexVectors,
            outComplexVectors,
            outFieldIds,
            outputMutator.getVector(ColumnUtils.COPY_HISTORY_COLUMN_NAME),
            typeCoercion,
            javaCodeGenWatch,
            gandivaCodeGenWatch,
            depth);
  }

  @Override
  protected ValueVector getInVector(String name, Map<String, ValueVector> inFieldMap) {
    for (int i = 0; i < transformProperties.size(); i++) {
      String mapping = transformProperties.get(i).getTargetColName();
      if (name.equalsIgnoreCase(mapping)) {
        String targetFieldName = targetSchema.getFields().get(i).getName();
        String key =
            renamedFieldNameMap.getOrDefault(targetFieldName.toLowerCase(), name.toLowerCase());
        return inFieldMap.getOrDefault(key, null);
      }
    }
    return null;
  }
}
