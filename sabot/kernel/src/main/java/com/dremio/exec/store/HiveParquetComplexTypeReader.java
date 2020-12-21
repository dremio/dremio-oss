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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.arrow.vector.ValueVector;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * This class helps in handling coercion of nested complex type columns
 * of a Hive table
 */
public class HiveParquetComplexTypeReader implements AutoCloseable {
  private final SampleMutator mutator;
  private final OperatorContext context;
  private final Stopwatch javaCodeGenWatch;
  private final TypeCoercion hiveTypeCoercion;
  private final Stopwatch gandivaCodeGenWatch;

  private OutputMutator outputMutator;
  private HiveParquetCopier.ParquetCopier[] copiers;

  public HiveParquetComplexTypeReader(OperatorContext context, SampleMutator mutator, TypeCoercion hiveTypeCoercion,
                                      Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch) {
    Preconditions.checkArgument(mutator != null, "Invalid argument");
    this.mutator = mutator;
    this.context = context;
    this.hiveTypeCoercion = hiveTypeCoercion;
    this.javaCodeGenWatch = javaCodeGenWatch;
    this.gandivaCodeGenWatch = gandivaCodeGenWatch;
  }

  private void createCopiers() {
    ArrayList<ValueVector> outComplexVectors = new ArrayList<>();
    ArrayList<ValueVector> inComplexVectors = new ArrayList<>();

    Preconditions.checkState(outputMutator != null, "Invalid state");
    Iterator<ValueVector> outVectorIterator = outputMutator.getVectors().iterator();
    Map<String,ValueVector> inFieldMap = mutator.getFieldVectorMap();

    // for each outvector find matching invector by doing case insensitive name search
    while (outVectorIterator.hasNext()) {
      ValueVector outV = outVectorIterator.next();
      ValueVector inV = inFieldMap.getOrDefault(outV.getField().getName().toLowerCase(), null);
      final CompleteType targetType = CompleteType.fromField(outV.getField());
      if (targetType.isComplex()) {
        inComplexVectors.add(inV);
        outComplexVectors.add(outV);
      }
    }

    copiers = HiveParquetCopier.createCopiers(context, inComplexVectors, outComplexVectors,
      hiveTypeCoercion, javaCodeGenWatch, gandivaCodeGenWatch);
  }

  public void setupProjector(OutputMutator output, VectorContainer incoming,
                             ExpressionEvaluationOptions projectorOptions,
                             VectorContainer projectorOutput) {
    outputMutator = output;
    createCopiers();
    for (HiveParquetCopier.ParquetCopier copier: copiers) {
      copier.setupProjector(incoming, projectorOptions, projectorOutput);
    }
  }

  public void runProjector(int recordCount) {
    for (HiveParquetCopier.ParquetCopier copier: copiers) {
      copier.copy(recordCount);
    }
  }

  @Override
  public void close() throws Exception {
    if (copiers != null) {
      for (HiveParquetCopier.ParquetCopier copier : copiers) {
        copier.close();
      }
    }
  }
}
