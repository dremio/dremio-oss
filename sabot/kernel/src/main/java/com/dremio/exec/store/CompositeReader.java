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

import com.dremio.common.AutoCloseables;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Stopwatch;

/**
 * This is main class that does coercion of fields. It separates input fields into primitive and
 * complex, then processes them using respective coercion readers
 */
public class CompositeReader implements AutoCloseable {
  private final ComplexTypeReader complexTypeReader;
  private final PrimitiveTypeReader primitiveTypeReader;

  public CompositeReader(
      SampleMutator mutator,
      OperatorContext context,
      TypeCoercion typeCoercion,
      Stopwatch javaCodeGenWatch,
      Stopwatch gandivaCodeGenWatch,
      BatchSchema originalSchema) {
    this.primitiveTypeReader =
        new PrimitiveTypeReader(
            mutator, context, typeCoercion, javaCodeGenWatch, gandivaCodeGenWatch, originalSchema);
    this.complexTypeReader =
        new ComplexTypeReader(
            context, mutator, typeCoercion, javaCodeGenWatch, gandivaCodeGenWatch);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(complexTypeReader, primitiveTypeReader);
  }

  public void setupProjector(
      OutputMutator output,
      VectorContainer incoming,
      ExpressionEvaluationOptions projectorOptions,
      VectorContainer projectorOutput) {
    this.primitiveTypeReader.setupProjector(incoming, projectorOptions, projectorOutput);
    this.complexTypeReader.setupProjector(output, incoming, projectorOptions, projectorOutput);
  }

  public void runProjector(int recordCount, VectorContainer incoming) {
    this.primitiveTypeReader.runProjector(recordCount, incoming);
    this.complexTypeReader.runProjector(recordCount);
  }

  public void resetProjector() {
    primitiveTypeReader.resetProjector();
  }
}
