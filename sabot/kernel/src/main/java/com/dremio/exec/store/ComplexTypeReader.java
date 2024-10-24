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

import com.dremio.common.expression.BasePath;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.ComplexTypeCopiers.ComplexTypeCopier;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.apache.arrow.vector.ValueVector;

/** This class helps in handling coercion of nested complex type columns */
public class ComplexTypeReader implements AutoCloseable {
  protected final SampleMutator mutator;
  protected final OperatorContext context;
  protected final Stopwatch javaCodeGenWatch;
  protected final TypeCoercion typeCoercion;
  protected final Stopwatch gandivaCodeGenWatch;

  protected OutputMutator outputMutator;
  protected ComplexTypeCopier[] copiers;
  protected final int depth;

  public ComplexTypeReader(
      OperatorContext context,
      SampleMutator mutator,
      TypeCoercion typeCoercion,
      Stopwatch javaCodeGenWatch,
      Stopwatch gandivaCodeGenWatch,
      int depth) {
    Preconditions.checkArgument(mutator != null, "Invalid argument");
    this.mutator = mutator;
    this.context = context;
    this.typeCoercion = typeCoercion;
    this.javaCodeGenWatch = javaCodeGenWatch;
    this.gandivaCodeGenWatch = gandivaCodeGenWatch;
    this.depth = depth;
  }

  protected void createCopiers() {
    ArrayList<ValueVector> outComplexVectors = new ArrayList<>();
    ArrayList<ValueVector> inComplexVectors = new ArrayList<>();
    ArrayList<Integer> outFieldIds = new ArrayList<>();

    Preconditions.checkState(outputMutator != null, "Invalid state");
    Iterator<ValueVector> outVectorIterator = outputMutator.getVectors().iterator();
    Map<String, ValueVector> inFieldMap = mutator.getFieldVectorMap();

    // for each outvector find matching invector by doing case insensitive name search
    while (outVectorIterator.hasNext()) {
      ValueVector outV = outVectorIterator.next();
      ValueVector inV = getInVector(outV.getField().getName(), inFieldMap);
      final CompleteType targetType = CompleteType.fromField(outV.getField());
      if (targetType.isComplex()) {
        inComplexVectors.add(inV);
        outComplexVectors.add(outV);
        outFieldIds.add(
            outputMutator
                .getContainer()
                .getSchema()
                .getFieldId(BasePath.getSimple(outV.getName()))
                .getFieldIds()[0]);
      }
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

  protected ValueVector getInVector(String name, Map<String, ValueVector> inFieldMap) {
    return inFieldMap.getOrDefault(name.toLowerCase(), null);
  }

  public void setupProjector(
      OutputMutator output,
      VectorContainer incoming,
      ExpressionEvaluationOptions projectorOptions,
      VectorContainer projectorOutput) {
    outputMutator = output;
    createCopiers();
    for (ComplexTypeCopier copier : copiers) {
      copier.setupProjector(incoming, projectorOptions, projectorOutput);
    }
  }

  public void runProjector(int recordCount) {
    for (ComplexTypeCopier copier : copiers) {
      copier.copy(recordCount);
    }
  }

  @Override
  public void close() throws Exception {
    if (copiers != null) {
      for (ComplexTypeCopier copier : copiers) {
        copier.close();
      }
    }
  }
}
