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

import static com.google.common.base.Preconditions.checkArgument;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

/**
 * This class is responsible for setting up complex field readers Also, copies data from appropriate
 * invector to outvector
 */
public class ComplexTypeCopiers {
  /** interface for parquet complex field copiers */
  public interface ComplexTypeCopier extends AutoCloseable {
    void copy(int count);

    void setupProjector(
        VectorContainer incoming,
        ExpressionEvaluationOptions projectorOptions,
        VectorContainer projectorOutput);

    void runProjector(int recordCount, VectorContainer incoming);
  }

  /**
   * Top level driver routine that setups copiers
   *
   * @param context
   * @param input List of input vectors
   * @param output List of output vectors
   * @param typeCoercion
   * @param javaCodeGenWatch
   * @param gandivaCodeGenWatch
   * @return
   */
  public static ComplexTypeCopier[] createCopiers(
      OperatorContext context,
      List<ValueVector> input,
      List<ValueVector> output,
      TypeCoercion typeCoercion,
      Stopwatch javaCodeGenWatch,
      Stopwatch gandivaCodeGenWatch) {
    checkArgument(
        input.size() == output.size(),
        "Invalid column size (" + input.size() + ", " + output.size() + ")");
    final int numColumns = output.size();

    // create one copier for each (input, output) pair
    final ComplexTypeCopier[] copiers = new ComplexTypeCopier[numColumns];
    for (int pos = 0; pos < numColumns; pos++) {
      copiers[pos] =
          createCopier(
              context,
              input.get(pos),
              output.get(pos),
              typeCoercion,
              javaCodeGenWatch,
              gandivaCodeGenWatch);
    }

    return copiers;
  }

  /** internal routine that pairs invector and outvector and returns a copier */
  private static ComplexTypeCopier createCopier(
      OperatorContext context,
      ValueVector inVector,
      ValueVector outVector,
      TypeCoercion typeCoercion,
      Stopwatch javaCodeGenWatch,
      Stopwatch gandivaCodeGenWatch) {
    checkArgument(outVector != null, "invalid argument");
    if (inVector == null) {
      // it is possible that table has extra fields and parquet file may not have those fields
      return new NoOpCopier();
    }

    // ListVector can be mapped to only ListVector
    if (outVector instanceof ListVector && inVector instanceof ListVector) {
      // return list copier
      return new ListCopier(
          context, inVector, outVector, typeCoercion, javaCodeGenWatch, gandivaCodeGenWatch);
    }

    // StructVector can be mapped to only StructVector
    if (outVector instanceof StructVector && inVector instanceof StructVector) {
      // return struct copier
      return new StructCopier(
          context, inVector, outVector, typeCoercion, javaCodeGenWatch, gandivaCodeGenWatch);
    }

    // control should not reach this place because of schema validation
    // that happens at the beginning of reader setup
    // returning NoOpCopier as a conservative measure, which results in null values
    return new NoOpCopier();
  }

  /** Class that copies ListVector by taking care of child field coercions */
  static class ListCopier implements ComplexTypeCopier {
    private final ListVector inVector;
    private final ListVector outVector;
    private final CompositeReader childFieldCompositeReader;
    private final SampleMutator outChildMutator;
    private final SampleMutator inChildMutator;

    /* only for testing */
    ListCopier(ValueVector in, ValueVector out) {
      inVector = (ListVector) in;
      outVector = (ListVector) out;
      childFieldCompositeReader = null;
      outChildMutator = null;
      inChildMutator = null;
    }

    public ListCopier(
        OperatorContext context,
        ValueVector in,
        ValueVector out,
        TypeCoercion typeCoercion,
        Stopwatch javaCodeGenWatch,
        Stopwatch gandivaCodeGenWatch) {
      inVector = (ListVector) in;
      outVector = (ListVector) out;

      // create a mutator for output child field vector
      outChildMutator = createChildMutator(context, outVector.getDataVector());

      // construct mutator for input child field vector
      inChildMutator = createChildMutator(context, inVector.getDataVector());

      // construct schema containing only child field
      BatchSchema targetSchema = outChildMutator.getContainer().getSchema();

      // recursively setup parquet reader for child vector
      childFieldCompositeReader =
          new CompositeReader(
              inChildMutator,
              context,
              typeCoercion.getChildTypeCoercion(out.getName(), targetSchema),
              javaCodeGenWatch,
              gandivaCodeGenWatch,
              targetSchema);
    }

    private SampleMutator createChildMutator(
        OperatorContext context, ValueVector childFieldVector) {
      SampleMutator sampleMutator = new SampleMutator(context.getAllocator());
      sampleMutator.addVector(childFieldVector);
      sampleMutator.getContainer().buildSchema();
      sampleMutator.getAndResetSchemaChanged();
      return sampleMutator;
    }

    @Override
    public void setupProjector(
        VectorContainer incoming,
        ExpressionEvaluationOptions projectorOptions,
        VectorContainer projectorOutput) {
      childFieldCompositeReader.setupProjector(
          outChildMutator,
          inChildMutator.getContainer(),
          projectorOptions,
          outChildMutator.getContainer());
    }

    @Override
    public void runProjector(int recordCount, VectorContainer incoming) {
      childFieldCompositeReader.runProjector(recordCount, incoming);
    }

    @VisibleForTesting
    void copyNonDataBufferRefs(int count) {
      ArrowFieldNode arrowFieldNode = new ArrowFieldNode(count, inVector.getNullCount());
      outVector.loadFieldBuffers(
          arrowFieldNode, Arrays.asList(inVector.getValidityBuffer(), inVector.getOffsetBuffer()));
    }

    @Override
    public void copy(int count) {
      copyNonDataBufferRefs(count);

      // Now handle child field copy
      if (count > 0) {
        int childCount = outVector.getOffsetBuffer().getInt(count * ListVector.OFFSET_WIDTH);
        runProjector(childCount, inChildMutator.getContainer());
      }
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(childFieldCompositeReader, outChildMutator, inChildMutator);
    }
  }

  /** Class that copies StructVector by taking care of child field coercions */
  static class StructCopier implements ComplexTypeCopier {
    private final StructVector inVector;
    private final StructVector outVector;
    private final CompositeReader childrenFieldCompositeReader;
    private final SampleMutator outChildMutator;
    private final SampleMutator inChildMutator;

    /* only for testing */
    StructCopier(ValueVector in, ValueVector out) {
      this.inVector = (StructVector) in;
      this.outVector = (StructVector) out;
      childrenFieldCompositeReader = null;
      outChildMutator = null;
      inChildMutator = null;
    }

    public StructCopier(
        OperatorContext context,
        ValueVector in,
        ValueVector out,
        TypeCoercion typeCoercion,
        Stopwatch javaCodeGenWatch,
        Stopwatch gandivaCodeGenWatch) {
      this.inVector = (StructVector) in;
      this.outVector = (StructVector) out;
      Map<String, ValueVector> inFields = new HashMap<>();

      outChildMutator = new SampleMutator(context.getAllocator());
      inChildMutator = new SampleMutator(context.getAllocator());

      int fieldCount = inVector.getChildFieldNames().size();
      for (int idx = 0; idx < fieldCount; ++idx) {
        String fieldName = inVector.getChildFieldNames().get(idx);
        inFields.put(fieldName.toLowerCase(), inVector.getChildVectorWithOrdinal(fieldName).vector);
      }

      // add all child outvectors to output mutator
      // add all child invectors if they are needed in the output
      int outFieldCount = outVector.getChildFieldNames().size();
      for (int pos = 0; pos < outFieldCount; ++pos) {
        String outFieldName = outVector.getChildFieldNames().get(pos);
        outChildMutator.addVector(outVector.getChildVectorWithOrdinal(outFieldName).vector);
        if (inFields.containsKey(outFieldName.toLowerCase())) {
          inChildMutator.addVector(inFields.get(outFieldName.toLowerCase()));
        }
      }

      outChildMutator.getContainer().buildSchema();
      outChildMutator.getAndResetSchemaChanged();

      inChildMutator.getContainer().buildSchema();
      inChildMutator.getAndResetSchemaChanged();

      // construct schema containing children
      BatchSchema targetSchema = outChildMutator.getContainer().getSchema();

      // recursively setup parquet reader for child fields
      childrenFieldCompositeReader =
          new CompositeReader(
              inChildMutator,
              context,
              typeCoercion.getChildTypeCoercion(out.getName(), targetSchema),
              javaCodeGenWatch,
              gandivaCodeGenWatch,
              targetSchema);
    }

    @Override
    public void setupProjector(
        VectorContainer incoming,
        ExpressionEvaluationOptions projectorOptions,
        VectorContainer projectorOutput) {
      childrenFieldCompositeReader.setupProjector(
          outChildMutator,
          inChildMutator.getContainer(),
          projectorOptions,
          outChildMutator.getContainer());
    }

    @Override
    public void runProjector(int recordCount, VectorContainer incoming) {
      childrenFieldCompositeReader.runProjector(recordCount, incoming);
    }

    @VisibleForTesting
    void copyNonDataBufferRefs(int count) {
      ArrowFieldNode arrowFieldNode = new ArrowFieldNode(count, inVector.getNullCount());
      outVector.loadFieldBuffers(
          arrowFieldNode, Collections.singletonList(inVector.getValidityBuffer()));
    }

    @Override
    public void copy(int count) {
      copyNonDataBufferRefs(count);

      // copy child fields
      if (count > 0) {
        runProjector(count, inChildMutator.getContainer());
      }
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(childrenFieldCompositeReader, outChildMutator, inChildMutator);
    }
  }

  /** NoOpCopier does nothing */
  private static class NoOpCopier implements ComplexTypeCopier {
    public NoOpCopier() {}

    @Override
    public void copy(int count) {}

    @Override
    public void setupProjector(
        VectorContainer incoming,
        ExpressionEvaluationOptions projectorOptions,
        VectorContainer projectorOutput) {}

    @Override
    public void runProjector(int recordCount, VectorContainer incoming) {}

    @Override
    public void close() throws Exception {}
  }
}
