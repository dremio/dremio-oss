/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
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
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.ValueVectorWriteExpression;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.util.BatchPrinter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.project.Projector;
import com.dremio.sabot.op.project.Projector.ComplexWriterCreator;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.collect.Lists;

public class CoercionReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoercionReader.class);

  private VectorContainer outgoing;
  private VectorContainer incoming;
  private final List<ValueVector> allocationVectors = Lists.newArrayList();
  private final SampleMutator mutator;
  private final RecordReader inner;
  private Projector projector;
  private final BatchSchema targetSchema;
  private final List<NamedExpression> exprs;

  private OutputMutator outputMutator;

  private static final boolean DEBUG_PRINT = false;

  public CoercionReader(OperatorContext context,
                        List<SchemaPath> columns,
                        RecordReader inner,
                        BatchSchema targetSchema) {
    super(context, columns);
    this.mutator = new SampleMutator(context.getAllocator());
    this.incoming = mutator.getContainer();
    this.inner = inner;
    this.outgoing = new VectorContainer(context.getAllocator());
    this.targetSchema = targetSchema;
    this.exprs = new ArrayList<>(targetSchema.getFieldCount());

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
        final MajorType majorType = MajorTypeHelper.getMajorTypeForField(field);
        LogicalExpression cast = FunctionCallFactory.createCast(majorType, inputRef);
        exprs.add(new NamedExpression(cast, inputRef));
      }
      //TODO check that the expression type is a subset of the targetSchema type
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    inner.setup(mutator);
    newSchema();
  }

  public void newSchema() {
    incoming.buildSchema();
    for (Field field : targetSchema.getFields()) {
      ValueVector vector = outputMutator.getVector(field.getName());
      if (vector == null) {
        continue;
      }
      outgoing.add(vector);
    }
    outgoing.buildSchema(SelectionVectorMode.NONE);

    // reset the schema change callback
    mutator.isSchemaChanged();

    if (DEBUG_PRINT) {
      FragmentHandle h = context.getFragmentHandle();
      String op = String.format("CoercionReader:%d:%d:%d, %s --> %s", h.getMajorFragmentId(), h.getMinorFragmentId(), context.getStats().getOperatorId(), incoming.getSchema(), outgoing.getSchema());
      System.out.println(op);
      mutator.getContainer().setAllCount(2);
      BatchPrinter.printBatch(mutator.getContainer());
    }

    if (incoming.getSchema() == null || incoming.getSchema().getFieldCount() == 0) {
      return;
    }

    final ClassGenerator<Projector> cg = context.getClassProducer().createGenerator(Projector.TEMPLATE_DEFINITION).getRoot();
    final IntHashSet transferFieldIds = new IntHashSet();
    final List<TransferPair> transfers = Lists.newArrayList();

    for (int i = 0; i < exprs.size(); i++) {
      final NamedExpression namedExpression = exprs.get(i);

      // it is possible that a filter removed all output or the shard has no data, so we don't have any incoming vectors
      if (incoming.getValueVectorId(SchemaPath.getSimplePath(targetSchema.getFields().get(i).getName())) == null) {
        continue;
      }

      final LogicalExpression expr = context.getClassProducer().materializeAndAllowComplex(namedExpression.getExpr(), incoming);
      final Field outputField = expr.getCompleteType().toField(namedExpression.getRef());

      switch(ProjectOperator.getEvalMode(incoming, expr, transferFieldIds)){

      case COMPLEX: {
        assert false : "Should not see complex expression in coercion reader";
        break;
      }
      case DIRECT: {
        final ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
        final TypedFieldId id = vectorRead.getFieldId();
        final ValueVector vvIn = incoming.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();
        final FieldReference ref = namedExpression.getRef();
        final ValueVector vvOut = outgoing.addOrGet(vectorRead.getCompleteType().toField(ref));
        final TransferPair tp = vvIn.makeTransferPair(vvOut);
        transfers.add(tp);
        transferFieldIds.add(vectorRead.getFieldId().getFieldIds()[0]);
        break;
      }
      case EVAL: {
        final ValueVector vector = outgoing.addOrGet(outputField);
        allocationVectors.add(vector);
        final TypedFieldId fid = outgoing.getValueVectorId(SchemaPath.getSimplePath(outputField.getName()));
        final boolean useSetSafe = !(vector instanceof FixedWidthVector);
        final ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, useSetSafe);
        final HoldingContainer hc = cg.addExpr(write, ClassGenerator.BlockCreateMode.NEW_IF_TOO_LARGE);
        if (expr instanceof ValueVectorReadExpression) {
          assert false : "Should not have value vector read expressions in coercion reader";
        }
        break;
      }
      default:
        assert false : "Unsupported eval mode, " + ProjectOperator.getEvalMode(incoming, expr, transferFieldIds);
        throw new UnsupportedOperationException();
      }
    }

    this.projector = cg.getCodeGenerator().getImplementationClass();
    this.projector.setup(
      context.getFunctionContext(),
      incoming,
      outgoing,
      transfers,
      new ComplexWriterCreator(){
        @Override
        public ComplexWriter addComplexWriter(String name) {
          return null;
        }
      }
    );
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    super.allocate(vectorMap);
    inner.allocate(mutator.getFieldVectorMap());
  }

  @Override
  public int next() {
    int recordCount = inner.next();
    if (mutator.isSchemaChanged()) {
      newSchema();
    }
    incoming.setAllCount(recordCount);
    if (DEBUG_PRINT) {
      FragmentHandle h = context.getFragmentHandle();
      outgoing.buildSchema();
      String op = String.format("CoercionReader:%d:%d:%d --> (%d), %s", h.getMajorFragmentId(), h.getMinorFragmentId(), context.getStats().getOperatorId(), recordCount, outgoing.getSchema());
      System.out.println(op);
      BatchPrinter.printBatch(mutator.getContainer());
    }
    if (projector != null) {
      projector.projectRecords(recordCount);
      for (final ValueVector v : allocationVectors) {
        v.setValueCount(recordCount);
      }
    }
    return recordCount;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(outgoing, incoming, inner, mutator);
  }
}
