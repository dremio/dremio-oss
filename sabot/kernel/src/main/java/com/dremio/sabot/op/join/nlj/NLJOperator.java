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
package com.dremio.sabot.op.join.nlj;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.physical.config.NestedLoopJoinPOP;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.sort.external.RecordBatchData;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.google.common.base.Preconditions;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;

/*
 * SqlOperatorImpl implementation for the nested loop join operator
 */
public class NLJOperator implements DualInputOperator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NLJOperator.class);

  // We accumulate all the batches on the right side in a hyper container.
  private ExpandableHyperContainer allRight;

  // Record count of the individual batches in the right hyper container
  private final LinkedList<Integer> rightCounts = new LinkedList<>();

  private final OperatorContext context;
  private final NestedLoopJoinPOP config;

  // Left input to the nested loop join operator
  private VectorAccessible left;

  // Right input to the nested loop join operator.
  private VectorAccessible right;

  private int currentOutputPosition;

  private final VectorContainer outgoing;

  // Runtime generated class implementing the NestedLoopJoin interface
  private NLJWorker nljWorker;

  private enum LeftState {
    INIT,
    HAS_DATA,
    COMPLETED
  }

  private LeftState leftState = LeftState.INIT;

  private State state = State.NEEDS_SETUP;

  protected NLJOperator(OperatorContext context, NestedLoopJoinPOP config) {
    this.context = context;
    this.config = config;
    this.outgoing = context.createOutputVectorContainer();
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public VectorAccessible setup(VectorAccessible left, VectorAccessible right) throws Exception {
    state.is(State.NEEDS_SETUP);
    this.left = left;
    this.right = right;
    outgoing.addSchema(right.getSchema());
    outgoing.addSchema(left.getSchema());
    allRight = new ExpandableHyperContainer(context.getAllocator(), right.getSchema());
    outgoing.buildSchema(SelectionVectorMode.NONE);
    outgoing.setInitialCapacity(context.getTargetBatchSize());
    state = State.CAN_CONSUME_R;
    return outgoing;
  }

  @Override
  public void consumeDataRight(int records) throws Exception {
    state.is(State.CAN_CONSUME_R);

    @SuppressWarnings("resource")
    final RecordBatchData batchCopy = new RecordBatchData(right, context.getAllocator());

    logger.debug("Adding batch on right. {} records", right.getRecordCount());
    rightCounts.addLast(right.getRecordCount());
    allRight.addBatch(batchCopy.getContainer());
  }

  @Override
  public void noMoreToConsumeRight() throws Exception {
    state.is(State.CAN_CONSUME_R);

    if (rightCounts.isEmpty()) {
      logger.debug("No more to consume on right. Right is empty. Done");
      state = State.DONE;
    } else {
      setupWorker();
      logger.debug("No more to consume on right. Can consume left");
      state = State.CAN_CONSUME_L;
    }
  }

  @Override
  public void consumeDataLeft(int records) throws Exception {
    state.is(State.CAN_CONSUME_L);
    leftState = LeftState.HAS_DATA;
    state = State.CAN_PRODUCE;
  }

  @Override
  public void noMoreToConsumeLeft() throws Exception {
    state.is(State.CAN_CONSUME_L);
    Preconditions.checkState(leftState != LeftState.COMPLETED);
    if (leftState == LeftState.INIT) {
      logger.debug("No more to consume on left. Left is empty. Done");
      state = State.DONE;
    } else {
      logger.debug("No more to consume on left. Can produce");
      state = State.CAN_PRODUCE;
    }
    leftState = LeftState.COMPLETED;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    logger.debug("LeftState: {}, currentOutputPosition: {}", leftState, currentOutputPosition);

    if (leftState == LeftState.COMPLETED && currentOutputPosition >= 0) {
      // the left is completed and we have pending output.
      state = State.DONE;
      return outgoing.setAllCount(currentOutputPosition);
    }

    if (currentOutputPosition == 0) {
      outgoing.allocateNew();
    }

    final int outputIndex = nljWorker.emitRecords(currentOutputPosition, context.getTargetBatchSize());

    logger.debug("outputIndex: {}", outputIndex);

    if (outputIndex < 0) {
      // ran out of room.
      currentOutputPosition = 0;
      logger.debug("ran out of room {}", -outputIndex);
      return outgoing.setAllCount(-outputIndex);

    } else if (outputIndex > 0) {
      // we output data but didn't hit a boundary.

      if (leftState == LeftState.COMPLETED) {
        state = State.DONE;
        logger.debug("finished {}", outputIndex);
        outgoing.setAllCount(outputIndex);
        return outputIndex;
      } else {
        // need more input, (emitted zero records or less than max).
        currentOutputPosition = outputIndex;
        state = State.CAN_CONSUME_L;
        logger.debug("not finished {}", outputIndex);
        // this batch isn't ready, we won't return the records.
        return 0;
      }

    } else {
      state = State.CAN_CONSUME_L;
      return 0;
    }

  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitDualInput(this, value);
  }

  /**
   * Method generates the runtime code needed for NLJ. Other than the setup method to set the input and output value
   * vector references we implement two more methods
   * 1. emitLeft()  -> Project record from the left side
   * 2. emitRight() -> Project record from the right side (which is a hyper container)
   * @return the runtime generated class that implements the NestedLoopJoin interface
   * @throws Exception
   * @throws IOException
   * @throws ClassTransformationException
   */
  private void setupWorker() throws Exception {

    final GeneratorMapping emitRight = GeneratorMapping.create("doSetup", "emitRight", null, null);
    final GeneratorMapping emitRightConstant = GeneratorMapping.create("doSetup", "doSetup", null, null);
    final MappingSet emitRightMapping = new MappingSet("rightCompositeIndex", "outIndex", "rightContainer", "outgoing",
        emitRightConstant, emitRight);

    final GeneratorMapping emitLeft = GeneratorMapping.create("doSetup", "emitLeft", null, null);
    final GeneratorMapping emitLeftConstant = GeneratorMapping.create("doSetup", "doSetup", null, null);
    final MappingSet emitLeftMapping = new MappingSet("leftIndex", "outIndex", "leftBatch", "outgoing",
        emitLeftConstant, emitLeft);

    final CodeGenerator<NLJWorker> nLJCodeGenerator = context.getClassProducer()
        .createGenerator(NLJWorker.TEMPLATE_DEFINITION);
    final ClassGenerator<NLJWorker> nLJClassGenerator = nLJCodeGenerator.getRoot();

    int outputFieldId = 0;
    final JExpression outIndex = JExpr.direct("outIndex");

    {
      int fieldId = 0;
      nLJClassGenerator.setMappingSet(emitRightMapping);
      JExpression batchIndex = JExpr.direct("batchIndex");
      JExpression recordIndexWithinBatch = JExpr.direct("recordIndexWithinBatch");

      // Set the input and output value vector references corresponding to the
      // right batch
      for (Field field : right.getSchema()) {
        final CompleteType fieldType = CompleteType.fromField(field);
        // Add the vector to our output container
        outgoing.addOrGet(field);

        JVar inVV = nLJClassGenerator.declareVectorValueSetupAndMember("rightContainer",
            new TypedFieldId(fieldType, true, fieldId));
        JVar outVV = nLJClassGenerator.declareVectorValueSetupAndMember("outgoing",
            new TypedFieldId(fieldType, false, outputFieldId));
        nLJClassGenerator.getEvalBlock().add(
            outVV.invoke("copyFromSafe").arg(recordIndexWithinBatch).arg(outIndex).arg(inVV.component(batchIndex)));

        fieldId++;
        outputFieldId++;
      }
    }

    {
      int fieldId = 0;
      nLJClassGenerator.setMappingSet(emitLeftMapping);
      JExpression leftIndex = JExpr.direct("leftIndex");

      // Set the input and output value vector references corresponding to the
      // left batch
      for (Field field : left.getSchema()) {
        final CompleteType fieldType = CompleteType.fromField(field);

        // Add the vector to the output container
        outgoing.addOrGet(field);

        JVar inVV = nLJClassGenerator.declareVectorValueSetupAndMember("leftBatch",
            new TypedFieldId(fieldType, false, fieldId));
        JVar outVV = nLJClassGenerator.declareVectorValueSetupAndMember("outgoing",
            new TypedFieldId(fieldType, false, outputFieldId));

        nLJClassGenerator.getEvalBlock().add(outVV.invoke("copyFromSafe").arg(leftIndex).arg(outIndex).arg(inVV));

        fieldId++;
        outputFieldId++;
      }
    }

    nljWorker = nLJCodeGenerator.getImplementationClass();
    nljWorker.setupNestedLoopJoin(context.getClassProducer().getFunctionContext(), left, allRight, rightCounts,
        outgoing);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close((AutoCloseable) allRight, outgoing);
  }

  public static class Creator implements DualInputOperator.Creator<NestedLoopJoinPOP> {
    @Override
    public DualInputOperator create(OperatorContext context, NestedLoopJoinPOP config) throws ExecutionSetupException {
      return new NLJOperator(context, config);
    }

  }

}
