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
package com.dremio.exec.expr;

import java.util.List;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorAccessibleComplexWriter;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.filter.Filterer;
import com.dremio.sabot.op.llvm.NativeFilter;
import com.dremio.sabot.op.llvm.NativeProjectEvaluator;
import com.dremio.sabot.op.llvm.NativeProjectorBuilder;
import com.dremio.sabot.op.project.Projector;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Evaluates several expressions splits - in Java and/or Gandiva
 */
class SplitStageExecutor implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplitStageExecutor.class);

  // Operator context. Used to allocate memory
  final OperatorContext context;

  // The incoming VectorAccessible capturing the schema
  final VectorAccessible incoming;

  // VectorContainer to allocate memory for intermediate outputs
  VectorContainer intermediateOutputs;

  // Builder for Gandiva execution
  final NativeProjectorBuilder nativeProjectorBuilder;

  // Class generator for Java classes
  final ClassGenerator<Projector> cg;

  // All java splits that are to be evaluated
  final List<ExpressionSplit> javaSplits = Lists.newArrayList();

  // All Gandiva splits that are to be evaluated
  final List<ExpressionSplit> gandivaSplits = Lists.newArrayList();

  // Gandiva projector
  NativeProjectEvaluator nativeProjectEvaluator;
  // Gandiva filter
  NativeFilter nativeFilter;

  // Java evaluator
  Projector javaProjector;

  // Vectors for intermediate output
  final List<ValueVector> allocationVectors = Lists.newArrayList();

  // Transfer pairs to transfer output from allocation vector
  final List<TransferPair> transferPairs = Lists.newArrayList();

  // list of complex writers for Java codegen option
  final List<ComplexWriter> complexWriters = Lists.newArrayList();

  // set to true if this SplitStageExecutor has a final split
  // used in filter to set up the filter
  boolean hasOriginalExpression;

  // the filter function to apply to filter
  TimedFilterFunction filterFunction;

  final SupportedEngines.Engine preferredEngine;

  // Helper references for adding splits to correct list.
  final List<ExpressionSplit> splitsForPreferredCodeGen;
  final List<ExpressionSplit> splitsForNonPreferredCodeGen;

  SplitStageExecutor(OperatorContext context, VectorAccessible incoming, SupportedEngines.Engine preferredExecType) {
    this.context = context;
    this.incoming = incoming;
    this.preferredEngine = preferredExecType;
    this.hasOriginalExpression = false;
    this.nativeFilter = null;
    this.nativeProjectorBuilder = NativeProjectEvaluator.builder(incoming, context.getFunctionContext());
    this.cg = context.getClassProducer().createGenerator(Projector.TEMPLATE_DEFINITION).getRoot();
    this.splitsForPreferredCodeGen = this.preferredEngine ==
      SupportedEngines.Engine.GANDIVA? gandivaSplits : javaSplits;
    this.splitsForNonPreferredCodeGen = this.preferredEngine ==
      SupportedEngines.Engine.GANDIVA? javaSplits : gandivaSplits;
    this.intermediateOutputs = context.createOutputVectorContainer();
  }

  // Adds a split to be executed as part of this
  void addSplit(ExpressionSplit split) {
    if (split.isOriginalExpression()) {
      this.hasOriginalExpression = true;
    }

    if (split.getExecutionEngine() == this.preferredEngine) {
      splitsForPreferredCodeGen.add(split);
    } else {
      splitsForNonPreferredCodeGen.add(split);
    }

  }

  // This is called to setup a split
  private Field setupSplit(ExpressionSplit split, VectorContainer outgoing, boolean gandivaCodeGen) throws GandivaException {
    NamedExpression namedExpression = split.getNamedExpression();
    LogicalExpression expr = namedExpression.getExpr();
    Field outputField = expr.getCompleteType().toField(namedExpression.getRef());
    ValueVector vector = intermediateOutputs.addOrGet(outputField);
    split.setOutputOfSplit(vector);

    if (outgoing != null) {
      // transfer the output to the outgoing VectorContainer
      ValueVector transferTo = outgoing.addOrGet(outputField);
      transferPairs.add(vector.makeTransferPair(transferTo));
    }

    // space needs to be allocated for this vector
    allocationVectors.add(vector);

    if (gandivaCodeGen) {
      logger.trace("Setting up split for {} in Gandiva", split.toString());
      nativeProjectorBuilder.add(expr, vector, split.getOptimize());
      return outputField;
    }

    logger.trace("Setting up split for {} in Java", split.toString());
    // setup in Java
    TypedFieldId fid = intermediateOutputs.getValueVectorId(SchemaPath.getSimplePath(outputField.getName()));
    boolean useSetSafe = !(vector instanceof FixedWidthVector);
    ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, useSetSafe);
    cg.addExpr(write, ClassGenerator.BlockCreateMode.NEW_IF_TOO_LARGE, true);

    if (expr instanceof ValueVectorReadExpression) {
      final ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
      if (!vectorRead.hasReadPath()) {
        final TypedFieldId id = vectorRead.getFieldId();
        final ValueVector vvIn = incoming.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();
        vvIn.makeTransferPair(vector);
      }
    }

    return outputField;
  }

  // setup the code generators
  void setupFinish(VectorContainer outgoing, Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch) throws GandivaException {
    gandivaCodeGenWatch.start();
    nativeProjectEvaluator = nativeProjectorBuilder.build(incoming.getSchema(), context.getStats());
    gandivaCodeGenWatch.stop();

    javaCodeGenWatch.start();
    javaProjector = cg.getCodeGenerator().getImplementationClass();
    javaProjector.setup(
      context.getFunctionContext(),
      incoming,
      intermediateOutputs,
      Lists.newArrayList(),
      new Projector.ComplexWriterCreator(){
        @Override
        public ComplexWriter addComplexWriter(String name) {
          VectorAccessibleComplexWriter vc = new VectorAccessibleComplexWriter(outgoing);
          ComplexWriter writer = new ComplexWriterImpl(name, vc);
          complexWriters.add(writer);
          return writer;
        }
      }
    );
    javaCodeGenWatch.stop();
  }

  // setup evaluation of projector for all splits
  void setupProjector(VectorContainer outgoing, Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch) throws GandivaException {
    for(ExpressionSplit split : Iterables.concat(javaSplits, gandivaSplits)) {
      if (split.isOriginalExpression()) {
        setupSplit(split, outgoing, gandivaSplits.contains(split));
      } else {
        setupSplit(split, null, gandivaSplits.contains(split));
      }
    }

    setupFinish(outgoing, javaCodeGenWatch, gandivaCodeGenWatch);
  }

  // setup evaluation of filter for all splits
  void setupFilter(VectorContainer outgoing, Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch) throws GandivaException,Exception {
    if (!hasOriginalExpression) {
      setupProjector(null, javaCodeGenWatch, gandivaCodeGenWatch);
      return;
    }

    // create the no-op projectors
    setupFinish(outgoing, javaCodeGenWatch, gandivaCodeGenWatch);

    // This SplitStageExecutor has the final split
    // For a filter, we support only one expression
    // There can be only one split in this SplitStageExecutor
    ExpressionSplit finalSplit = null;
    if ((javaSplits.size() + gandivaSplits.size()) != 1) {
      throw new Exception("There should be one ExpressionSplit for a Filter operation");
    }

    if (javaSplits.size() == 1) {
      finalSplit = javaSplits.get(0);
    } else {
      finalSplit = gandivaSplits.get(0);
    }

    if (finalSplit.getExecutionEngine() == SupportedEngines.Engine.GANDIVA) {
      logger.trace("Setting up filter for split in Gandiva {}", finalSplit.toString());
      gandivaCodeGenWatch.start();
      nativeFilter = NativeFilter.build(finalSplit.getNamedExpression().getExpr(), incoming, outgoing.getSelectionVector2(),
        context.getFunctionContext(), finalSplit.getOptimize());
      gandivaCodeGenWatch.stop();
      this.filterFunction = new NativeTimedFilter(nativeFilter);
      return;
    }

    logger.trace("Setting up filter for split in Java {}", finalSplit.toString());
    javaCodeGenWatch.start();
    final ClassGenerator<Filterer> filterClassGen = context.getClassProducer().createGenerator(Filterer.TEMPLATE_DEFINITION2).getRoot();
    filterClassGen.addExpr(new ReturnValueExpression(finalSplit.getNamedExpression().getExpr()), ClassGenerator.BlockCreateMode.MERGE, true);
    final Filterer javaFilter = filterClassGen.getCodeGenerator().getImplementationClass();
    javaFilter.setup(context.getClassProducer().getFunctionContext(), incoming, outgoing);
    javaCodeGenWatch.stop();
    this.filterFunction = new JavaTimedFilter(javaFilter);
  }

  private void allocateNew(int recordsToConsume) {
    for(ValueVector vv : allocationVectors) {
      AllocationHelper.allocateNew(vv, recordsToConsume);
    }

    for (final ComplexWriter writer : complexWriters) {
      writer.allocate();
    }
  }

  private void setValueCount(int numRecords) {
    for(ValueVector vv : allocationVectors) {
      vv.setValueCount(numRecords);
    }

    for (final ComplexWriter writer : complexWriters) {
      writer.setValueCount(numRecords);
    }
  }

  // Transfer output of intermediate and final output
  private void transferOut() {
    // transfer intermediate outputs to the ValueVectorReadExpression
    // for other splits to read the output
    for(ExpressionSplit split : Iterables.concat(javaSplits, gandivaSplits)) {
      split.transferOut();
    }

    // transfer final output
    for(TransferPair tp : transferPairs) {
      tp.transfer();
    }
  }

  // Finished reading output from all pre-req splits
  private void markSplitOutputAsRead() {
    for(ExpressionSplit split : Iterables.concat(javaSplits, gandivaSplits)) {
      split.markOutputAsRead();
    }
  }

  void evaluateProjector(int recordsToConsume, Stopwatch javaWatch, Stopwatch gandivaWatch) throws Exception {
    try {
      allocateNew(recordsToConsume);

      gandivaWatch.start();
      nativeProjectEvaluator.evaluate(recordsToConsume);
      gandivaWatch.stop();
      javaWatch.start();
      javaProjector.projectRecords(recordsToConsume);
      javaWatch.stop();

      setValueCount(recordsToConsume);
      transferOut();
    } catch (Exception e) {
      // release memory allocated in case of an exception
      for(ValueVector vv : allocationVectors) {
        vv.clear();
      }

      for(ComplexWriter writer : complexWriters) {
        writer.clear();
      }

      throw e;
    } finally {
      markSplitOutputAsRead();
    }
  }

  int evaluateFilter(int recordsToConsume, Stopwatch javaWatch, Stopwatch gandivaWatch) throws Exception {
    try {
      return this.filterFunction.apply(recordsToConsume, javaWatch, gandivaWatch);
    } finally {
      markSplitOutputAsRead();
    }
  }

  @Override
  public void close() throws Exception {
    if (nativeProjectEvaluator != null) {
      nativeProjectEvaluator.close();
    }
    if (nativeFilter != null) {
      nativeFilter.close();
    }
  }

  class NativeTimedFilter implements TimedFilterFunction {
    final NativeFilter nativeFilter;

    NativeTimedFilter(NativeFilter nativeFilter) {
      this.nativeFilter = nativeFilter;
    }

    @Override
    public Integer apply(Integer recordsToConsume, Stopwatch javaWatch, Stopwatch gandivaWatch) throws Exception {
      gandivaWatch.start();
      try {
        return nativeFilter.filterBatch(recordsToConsume);
      } finally {
        gandivaWatch.stop();
      }
    }
  }

  class JavaTimedFilter implements TimedFilterFunction {
    final Filterer javaFilter;

    JavaTimedFilter(Filterer javaFilter) {
      this.javaFilter = javaFilter;
    }

    @Override
    public Integer apply(Integer recordsToConsume, Stopwatch javaWatch, Stopwatch gandivaWatch) throws Exception {
      javaWatch.start();
      try {
        return javaFilter.filterBatch(recordsToConsume);
      } finally {
        javaWatch.stop();
      }
    }
  }

  interface TimedFilterFunction {
    Integer apply(Integer recordsToConsume, Stopwatch javaWatch, Stopwatch gandivaWatch) throws Exception;
  }
}
