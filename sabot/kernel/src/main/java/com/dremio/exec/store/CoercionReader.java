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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import com.carrotsearch.hppc.IntHashSet;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.util.BatchPrinter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.project.Projector;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class CoercionReader extends AbstractRecordReader {
  private static final boolean DEBUG_PRINT = false;

  protected final List<ValueVector> allocationVectors = Lists.newArrayList();
  protected final SampleMutator mutator;
  protected final RecordReader inner;
  protected final BatchSchema targetSchema;
  protected final List<NamedExpression> exprs;
  protected final ExpressionEvaluationOptions projectorOptions;

  protected Projector projector;
  protected VectorContainer outgoing;
  protected VectorContainer incoming;
  protected ExpressionSplitter splitter;
  protected OutputMutator outputMutator;
  protected Stopwatch javaCodeGenWatch = Stopwatch.createUnstarted();
  protected Stopwatch gandivaCodeGenWatch = Stopwatch.createUnstarted();

  public CoercionReader(OperatorContext context, List<SchemaPath> columns, RecordReader inner, BatchSchema targetSchema) {
    super(context, columns);
    this.mutator = new SampleMutator(context.getAllocator());
    this.incoming = mutator.getContainer();
    this.inner = inner;
    this.outgoing = new VectorContainer(context.getAllocator());
    this.targetSchema = targetSchema;
    this.exprs = new ArrayList<>(targetSchema.getFieldCount());
    this.projectorOptions = new ExpressionEvaluationOptions(context.getOptions());
    this.projectorOptions.setCodeGenOption(context.getOptions().getOption(ExecConstants.QUERY_EXEC_OPTION.getOptionName()).getStringVal());
  }

  protected void createCoercions() {
    for (Field field : targetSchema.getFields()) {
      final FieldReference inputRef = FieldReference.getWithQuotedRef(field.getName());
      final CompleteType targetType = CompleteType.fromField(field);
      if (targetType.isUnion() || targetType.isComplex()) {
        // we are assuming that map and list fields won't need coercion but inner reader may rely on sampling
        // a handful of rows to figure out the schema and if the list/map is empty in those rows, the schema will be
        // incomplete
        exprs.add(new NamedExpression(inputRef, inputRef));
        // one way to fix this issue is to add the target field in the incoming container and rely on
        // schema learning to handle any changes we hit when reading from the underlying reader
        mutator.addField(field, TypeHelper.getValueVectorClass(field));
      } else {
        addExpression(field, inputRef);
      }
      //TODO check that the expression type is a subset of the targetSchema type
    }
  }

  protected void addExpression(Field field, FieldReference inputRef) {
    final MajorType majorType = MajorTypeHelper.getMajorTypeForField(field);
    LogicalExpression cast = FunctionCallFactory.createCast(majorType, inputRef);
    exprs.add(new NamedExpression(cast, inputRef));
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    createCoercions();
    this.outputMutator = output;
    inner.setup(mutator);
    newSchema(outgoing, outputMutator);
  }

  public void newSchema(VectorContainer projectorOut, OutputMutator projectorOutputMutator) {
    incoming.buildSchema();
    for (Field field : targetSchema.getFields()) {
      ValueVector vector = projectorOutputMutator.getVector(field.getName());
      if (vector == null) {
        continue;
      }
      projectorOut.add(vector);
    }
    projectorOut.buildSchema(SelectionVectorMode.NONE);

    // reset the schema change callback
    mutator.getAndResetSchemaChanged();

    setupProjector(projectorOut);
  }

  /**
   * set up projector to write output to given container
   * @param projectorOutput
   */
  protected void setupProjector(VectorContainer projectorOutput) {
    if (DEBUG_PRINT) {
      debugPrint(projectorOutput);
    }

    if (incoming.getSchema() == null || incoming.getSchema().getFieldCount() == 0) {
      return;
    }

    final ClassGenerator<Projector> cg = context.getClassProducer().createGenerator(Projector.TEMPLATE_DEFINITION).getRoot();
    final IntHashSet transferFieldIds = new IntHashSet();
    final List<TransferPair> transfers = Lists.newArrayList();

    try {
      splitter = ProjectOperator.createSplitterWithExpressions(incoming, exprs, transfers, cg,
          transferFieldIds, context, projectorOptions, projectorOutput, targetSchema);
      splitter.setupProjector(projectorOutput, javaCodeGenWatch, gandivaCodeGenWatch);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    javaCodeGenWatch.start();
    this.projector = cg.getCodeGenerator().getImplementationClass();
    this.projector.setup(context.getFunctionContext(), incoming, projectorOutput, transfers, name -> null);
    javaCodeGenWatch.stop();
    OperatorStats stats = context.getStats();
    stats.addLongStat(ScanOperator.Metric.JAVA_BUILD_TIME_NS, javaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.addLongStat(ScanOperator.Metric.GANDIVA_BUILD_TIME_NS, gandivaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    gandivaCodeGenWatch.reset();
    javaCodeGenWatch.reset();
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    super.allocate(vectorMap);
    inner.allocate(mutator.getFieldVectorMap());
  }

  @Override
  public int next() {
    int recordCount = inner.next();
    if (mutator.getAndResetSchemaChanged()) {
      newSchema(outgoing, outputMutator);
    }
    incoming.setAllCount(recordCount);

    if (DEBUG_PRINT) {
      debugPrint(outgoing);
    }

    runProjector(recordCount);
    return recordCount;
  }

  @Override
  public List<SchemaPath> getColumnsToBoost() {
    return inner.getColumnsToBoost();
  }

  protected void runProjector(int recordCount) {
    if (projector != null) {
      try {
        if (recordCount > 0) {
          splitter.projectRecords(recordCount, javaCodeGenWatch, gandivaCodeGenWatch);
        }
        javaCodeGenWatch.start();
        projector.projectRecords(recordCount);
        javaCodeGenWatch.stop();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      for (final ValueVector v : allocationVectors) {
        v.setValueCount(recordCount);
      }
    }
    OperatorStats stats = context.getStats();
    stats.addLongStat(ScanOperator.Metric.JAVA_EXECUTE_TIME_NS, javaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.addLongStat(ScanOperator.Metric.GANDIVA_EXECUTE_TIME_NS, gandivaCodeGenWatch.elapsed(TimeUnit.NANOSECONDS));
    javaCodeGenWatch.reset();
    gandivaCodeGenWatch.reset();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(outgoing, incoming, inner, mutator, splitter);
  }

  protected void debugPrint(VectorContainer projectorOut) {
    FragmentHandle h = context.getFragmentHandle();
    String op = String.format("CoercionReader:%d:%d:%d, %s --> %s", h.getMajorFragmentId(), h.getMinorFragmentId(), context.getStats().getOperatorId(), incoming.getSchema(), projectorOut.getSchema());
    System.out.println(op);
    mutator.getContainer().setAllCount(2);
    BatchPrinter.printBatch(mutator.getContainer());
  }
}
