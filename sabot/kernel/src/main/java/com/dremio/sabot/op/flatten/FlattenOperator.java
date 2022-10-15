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
package com.dremio.sabot.op.flatten;

import java.util.List;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.RepeatedValueVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import com.carrotsearch.hppc.IntHashSet;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.ValueVectorWriteExpression;
import com.dremio.exec.physical.config.FlattenPOP;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorAccessibleComplexWriter;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.project.Projector.ComplexWriterCreator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;

// TODO - handle the case where a user tries to flatten a scalar, should just act as a project all of the columns exactly
// as they come in
public class FlattenOperator implements SingleInputOperator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlattenOperator.class);

  private final FlattenPOP config;
  private final OperatorContext context;
  private final VectorContainer outgoing;
  private final List<ValueVector> allocationVectors;

  private State state = State.NEEDS_SETUP;
  private VectorAccessible incoming;
  private int recordCount;
  private int childCount;
  private Flattener flattener;
  private List<ComplexWriter> complexWriters;
  private boolean hasRemainder = false;
  private int remainderIndex = 0;
  private ValueVector flattenVector;

  public FlattenOperator(OperatorContext context, FlattenPOP pop) throws OutOfMemoryException {
    this.config = pop;
    this.context = context;
    this.outgoing = context.createOutputVectorContainer();
    this.allocationVectors = Lists.newArrayList();
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) throws Exception {
    state.is(State.NEEDS_SETUP);
    this.incoming = incoming;
    final List<NamedExpression> exprs = getExpressionList();
    final List<TransferPair> transfers = Lists.newArrayList();

    final ClassGenerator<Flattener> cg = context.getClassProducer().createGenerator(Flattener.TEMPLATE_DEFINITION).getRoot();
    final IntHashSet transferFieldIds = new IntHashSet();

    final NamedExpression flattenExpr = new NamedExpression(config.getColumn(), new FieldReference(config.getColumn()));
    final ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) context.getClassProducer()
        .materializeAndAllowComplex(flattenExpr.getExpr(), incoming);
    final TransferPair tp = getFlattenFieldTransferPair(flattenExpr.getRef());

    if (tp != null) {
      transfers.add(tp);
      outgoing.add(tp.getTo());
      flattenVector = tp.getTo();
      transferFieldIds.add(vectorRead.getFieldId().getFieldIds()[0]);
    }

    for (int i = 0; i < exprs.size(); i++) {
      final NamedExpression namedExpression = exprs.get(i);

      final String outputName = namedExpression.getRef().getRootSegment().getPath();
      final LogicalExpression expr = context.getClassProducer().materializeAndAllowComplex(namedExpression.getExpr(),
          incoming);
      final Field outputField = expr.getCompleteType().toField(outputName);
      switch (ProjectOperator.getEvalMode(incoming, expr, transferFieldIds)) {
      case COMPLEX:
        complexWriters = Lists.newArrayList();
        cg.addExpr(expr, namedExpression.getRef());
        break;

      case DIRECT:
      case EVAL:
      default:
        // since we'll be unwinding the repeated vector, we need to copy the rest.
        ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
        allocationVectors.add(vector);
        TypedFieldId fid = outgoing.add(vector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
        cg.addExpr(write);
        break;

      }
      cg.rotateBlock();

    }
    cg.getEvalBlock()._return(JExpr.TRUE);


    this.flattener = cg.getCodeGenerator().getImplementationClass();
    long outputMemoryLimit = context.getOptions().getOption(ExecConstants.FLATTEN_OPERATOR_OUTPUT_MEMORY_LIMIT);

    flattener.setup(
        context.getAllocator(),
        context.getClassProducer().getFunctionContext(),
        incoming,
        outgoing,
        transfers,
        new ComplexWriterCreator(){
          @Override
          public ComplexWriter addComplexWriter(String name) {
            VectorAccessibleComplexWriter vc = new VectorAccessibleComplexWriter(outgoing);
            ComplexWriter writer = new ComplexWriterImpl(name, vc);
            complexWriters.add(writer);
            return writer;
          }
        },
        outputMemoryLimit,
        context.getTargetBatchSize()
        );

    try {
      final TypedFieldId typedFieldId = incoming.getSchema().getFieldId(config.getColumn());
      final Field field = incoming.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
      final ListVector vector = ListVector.class.cast(
          incoming.getValueAccessorById(TypeHelper.getValueVectorClass(field), typedFieldId.getFieldIds()).getValueVector());

      flattener.setFlattenField(vector);
    } catch (Exception ex) {
      throw UserException.unsupportedError(ex).message("Trying to flatten a non-repeated field.").build(logger);
    }

    outgoing.buildSchema(SelectionVectorMode.NONE);
    outgoing.setInitialCapacity(context.getTargetBatchSize());

    state = State.CAN_CONSUME;
    return outgoing;
  }


  @Override
  public void consumeData(int incomingRecordCount) throws Exception {
    state.is(State.CAN_CONSUME);
    this.recordCount = incomingRecordCount;
    state = State.CAN_PRODUCE;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);
    final int records;
    if(!hasRemainder){
      records = handleInitial(recordCount);
    }else{
      records = handleRemainder();
    }
    outgoing.setRecordCount(records);
    return records;
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    state = State.DONE;
  }

  private int handleInitial(int incomingRecordCount) {
    doAlloc();

    childCount = 0;
    for (int i = 0; i < incomingRecordCount; ++i) {
      childCount += ((BaseRepeatedValueVector) flattener.getFlattenField()).getInnerValueCountAt(i);
    }

    int outputRecords = flattener.flattenRecords(incomingRecordCount, 0, monitor);
    // TODO - change this to be based on the repeated vector length
    if (outputRecords < childCount) {
      setValueCount(outputRecords);
      hasRemainder = true;
      remainderIndex = outputRecords;
      return remainderIndex;
    } else {
      setValueCount(outputRecords);
      flattener.resetGroupIndex();
      state = State.CAN_CONSUME;
      return outputRecords;
    }

  }

  private int handleRemainder() {
    doAlloc();
    final int remainingRecordCount = childCount - remainderIndex;
    int projRecords = flattener.flattenRecords(remainingRecordCount, 0, monitor);
    if (projRecords < remainingRecordCount) {
      remainderIndex += projRecords;
    } else {
      hasRemainder = false;
      remainderIndex = 0;
      flattener.resetGroupIndex();
      state = State.CAN_CONSUME;
    }

    setValueCount(projRecords);
    return projRecords;

  }

  public void addComplexWriter(ComplexWriter writer) {
    complexWriters.add(writer);
  }

  private void doAlloc() {
    //Allocate vv in the allocationVectors.
    for (ValueVector v : this.allocationVectors) {
      v.allocateNew();
    }

    //Allocate vv for complexWriters.
    if (complexWriters == null) {
      return;
    }

    for (ComplexWriter writer : complexWriters) {
      writer.allocate();
    }

  }

  private void setValueCount(int count) {
    for (ValueVector v : allocationVectors) {
      v.setValueCount(count);
    }

    if(flattenVector != null){
      flattenVector.setValueCount(count);
    }

    if (complexWriters != null) {
      for (ComplexWriter writer : complexWriters) {
        writer.setValueCount(count);
      }
    }


  }

  /**
   * The data layout is the same for the actual data within a repeated field, as it is in a scalar vector for
   * the same sql type. For example, a repeated int vector has a vector of offsets into a regular int vector to
   * represent the lists. As the data layout for the actual values in the same in the repeated vector as in the
   * scalar vector of the same type, we can avoid making individual copies for the column being flattened, and just
   * use vector copies between the inner vector of the repeated field to the resulting scalar vector from the flatten
   * operation. This is completed after we determine how many records will fit (as we will hit either a batch end, or
   * the end of one of the other vectors while we are copying the data of the other vectors alongside each new flattened
   * value coming out of the repeated field.)
   */
  private TransferPair getFlattenFieldTransferPair(FieldReference outputName) {
    final TypedFieldId fieldId = incoming.getSchema().getFieldId(config.getColumn());
    final Class<? extends ValueVector> vectorClass = TypeHelper.getValueVectorClass(incoming.getSchema().getColumn(fieldId.getFieldIds()[0]));
    final ValueVector flattenField = incoming.getValueAccessorById(vectorClass, fieldId.getFieldIds()).getValueVector();


    final ValueVector vvIn = RepeatedValueVector.class.cast(flattenField).getDataVector();
    return vvIn.getTransferPair(outputName.getAsNamePart().getName(), context.getAllocator());
  }

  private List<NamedExpression> getExpressionList() {
    List<NamedExpression> exprs = Lists.newArrayList();
    for (Field field : incoming.getSchema()) {
      if (field.getName().equalsIgnoreCase(config.getColumn().getAsUnescapedPath())) {
        continue;
      }
      exprs.add(new NamedExpression(SchemaPath.getSimplePath(field.getName()), new FieldReference(field.getName())));
    }
    return exprs;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }


  @Override
  public void close() throws Exception {
    AutoCloseables.close(outgoing);
  }

  // TODO: Move this to allocator based determination.
  private final Flattener.Monitor monitor = new Flattener.Monitor() {
    @Override
    public int getBufferSizeFor(int recordCount) {
      int bufferSize = 0;
      for(final ValueVector vv : allocationVectors) {
        bufferSize += vv.getBufferSizeFor(recordCount);
      }
      return bufferSize;
    }
  };

  public static class FlattenBatchCreator implements SingleInputOperator.Creator<FlattenPOP>{

    @Override
    public SingleInputOperator create(OperatorContext context, FlattenPOP operator) throws ExecutionSetupException {
      return new FlattenOperator(context, operator);
    }

  }

}
