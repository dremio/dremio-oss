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
package com.dremio.sabot.op.join.merge;

import static com.dremio.exec.compile.sig.GeneratorMapping.GM;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import com.dremio.exec.physical.config.MergeJoinPOP;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.common.hashtable.Comparator;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.google.common.base.Preconditions;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;

/**
 * MergeJoinOperator assumes both left and right input is already sorted by join conditions,
 * and merge them.
 */
public class MergeJoinOperator implements DualInputOperator {

  enum InternalState {
    NEEDS_SETUP,
    OUT_OF_LOOPS,
    IN_OUTER_LOOP,
    IN_INNER_LOOP
  }

  private State state = State.NEEDS_SETUP;

  // Join type, INNER, LEFT, RIGHT or OUTER
  private final JoinRelType joinType;
  private final OperatorContext context;
  private final OperatorStats stats;
  private final List<JoinCondition> conditions;

  private final VectorContainer outgoing;

  private VectorAccessible left; // aka r
  private VectorAccessible right; // aka s

  private MarkedAsyncIterator leftIterator;
  private MarkedAsyncIterator rightIterator;

  private MergeJoinComparator comparator = null;
  private boolean compareFinished = false;

  private boolean noMoreLeft = false;
  private boolean noMoreRight = false;

  public MergeJoinOperator(OperatorContext context, MergeJoinPOP popConfig) {
    this.context = context;
    this.joinType = popConfig.getJoinType();
    this.conditions = popConfig.getConditions();
    this.stats = context.getStats();

    this.outgoing = context.createOutputVectorContainer();
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    // TODO: logic here can be simplified
    if (compareFinished) {
      switch (joinType) {
      case INNER:
        done();
        break;
      case LEFT:
        if (noMoreLeft) {
          done();
        } else {
          state = comparator.finishNonMatching() ? State.CAN_CONSUME_L : State.CAN_PRODUCE;
        }
        break;
      case RIGHT:
        if (noMoreRight) {
          done();
        } else {
          state = comparator.finishNonMatching() ? State.CAN_CONSUME_R : State.CAN_PRODUCE;
        }
        break;
      case FULL:
        if (!noMoreLeft) {
          state = comparator.finishNonMatching() ? State.CAN_CONSUME_L : State.CAN_PRODUCE;
        } else if (!noMoreRight) {
          state = comparator.finishNonMatching() ? State.CAN_CONSUME_R : State.CAN_PRODUCE;
        } else {
          done();
        }
        break;
      default:
        throw new IllegalStateException("Unsupported join type");
      }
    } else {
      // because we may come to this state from CAN_PRODUCE, we attempt to continue
      // again here
      if (!leftIterator.hasNext()) {
        state = State.CAN_CONSUME_L;
      } else if (!rightIterator.hasNext()) {
        state = State.CAN_CONSUME_R;
      } else {
        // not at the end of either batch, so we continue join with next outputData
        comparator.continueJoin();
        state = State.CAN_PRODUCE;
      }
    }

    int ret = comparator.getOutputSize();
    comparator.resetOutputCounter();
    return outgoing.setAllCount(ret);
  }

  private void done() throws Exception {
    leftIterator.close();
    rightIterator.close();
    state = State.DONE;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitDualInput(this, value);
  }

  @Override
  public void close() throws Exception {
    List<AutoCloseable> autoCloseables = new ArrayList<>();
    autoCloseables.add(outgoing);
    autoCloseables.add(leftIterator);
    autoCloseables.add(rightIterator);
    AutoCloseables.close(autoCloseables);
  }

  @Override
  public VectorAccessible setup(VectorAccessible left, VectorAccessible right) throws Exception {
    state.is(State.NEEDS_SETUP);
    Preconditions.checkArgument(getInternalState() == InternalState.NEEDS_SETUP);

    this.left = left;
    this.right = right;

    // TODO get proper child allocator
    // TODO set proper memory limit & initial size
    leftIterator = new InMemoryMarkedIterator(context.getAllocator(), left.getSchema());
    rightIterator = new InMemoryMarkedIterator(context.getAllocator(), right.getSchema());

    outgoing.addSchema(right.getSchema());
    outgoing.addSchema(left.getSchema());
    outgoing.buildSchema(SelectionVectorMode.NONE);
    outgoing.setInitialCapacity(context.getTargetBatchSize());

    state = State.CAN_CONSUME_L;

    generateComparator();

    return outgoing;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void noMoreToConsumeLeft() throws Exception {
    state.is(State.CAN_CONSUME_L);

    noMoreLeft = true;

    if (compareFinished) {
      state = State.CAN_PRODUCE;
      return;
    }

    Preconditions.checkState(getInternalState() == InternalState.OUT_OF_LOOPS ||
        getInternalState() == InternalState.IN_OUTER_LOOP,
        "Reach unknown internal state");
    // end of INNER JOIN, move to produce to flush output buffer
    state = State.CAN_PRODUCE;
    compareFinished = true;
  }

  @Override
  public void noMoreToConsumeRight() throws Exception {
    state.is(State.CAN_CONSUME_R);

    noMoreRight = true;

    if (compareFinished) {
      state = State.CAN_PRODUCE;
      return;
    }

    switch (getInternalState()) {
    case OUT_OF_LOOPS:
      // end of INNER JOIN, move to produce to flush output buffer
      state = State.CAN_PRODUCE;
      compareFinished = true;
      break;
    case IN_INNER_LOOP:
      // for this r, we have reached the end of right table
      // we continue matching, the logic to handle end of right table
      // in inner loop exits in the comparator
      state = continueMatching();
      break;
    default:
      throw new IllegalStateException(String.format("Reach unknown internal state: %s", getInternalState()));
    }
  }

  @Override
  public void consumeDataLeft(int records) throws Exception {
    state.is(State.CAN_CONSUME_L);
    Runnable callback = leftIterator.asyncAcceptBatch(left);
    if (compareFinished) {
      state = comparator.finishNonMatching() ? State.CAN_CONSUME_L : State.CAN_PRODUCE;
    } else {
      state = continueMatching();
    }
    callback.run();
  }

  @Override
  public void consumeDataRight(int records) throws Exception {
    state.is(State.CAN_CONSUME_R);
    Runnable callback = rightIterator.asyncAcceptBatch(right);
    if (compareFinished) {
      state = comparator.finishNonMatching() ? State.CAN_CONSUME_R : State.CAN_PRODUCE;
    } else {
      state = continueMatching();
    }
    callback.run();
  }

  // do matching, and return next proper state of the Op
  private State continueMatching() {
    comparator.continueJoin();
    if (comparator.getOutputSize() >= context.getTargetBatchSize()) {
      return State.CAN_PRODUCE;
    } else {
      // cases where more data is needed
      if (!leftIterator.hasNext() && !noMoreLeft) {
        return State.CAN_CONSUME_L;
      } else if (!rightIterator.hasNext() && !noMoreRight) {
        return State.CAN_CONSUME_R;
      }

      // special logic for reaching the end of right table in inner loop
      if (getInternalState() == InternalState.IN_INNER_LOOP && noMoreRight && !rightIterator.hasNext()) {
        return State.CAN_CONSUME_R;
      }

      // end of comparison from outer of loops
      if (getInternalState() == InternalState.OUT_OF_LOOPS && (noMoreRight || noMoreLeft)) {
        compareFinished = true;
        return State.CAN_PRODUCE;
      }

      throw new IllegalStateException("Reach unknown internal state");
    }
  }

  private InternalState getInternalState() {
    return comparator != null ? comparator.getInternalState() : InternalState.NEEDS_SETUP;
  }

  private void generateComparator() {
    final CodeGenerator<MergeJoinComparator> cg = context.getClassProducer().createGenerator(MergeJoinComparator.TEMPLATE_DEFINITION);
    final ClassGenerator<MergeJoinComparator> g = cg.getRoot();
    final ClassProducer producer = context.getClassProducer();

    final GeneratorMapping compareMapping = GM("doSetup", "doCompare", null, null);
    final GeneratorMapping projectMapping = GM("doSetup", "doProject", null, null);

    // TODO: why pass two mappings?
    final MappingSet mainMappingSet = new MappingSet( (String) null, null, compareMapping, compareMapping);
    final MappingSet leftCompareMappingSet =
        new MappingSet("leftIndex", null, "leftBatch", "outgoing", compareMapping, compareMapping);
    final MappingSet rightCompareMappingSet =
        new MappingSet("rightIndex", null, "rightBatch", "outgoing", compareMapping, compareMapping);

    final MappingSet leftProjectMappingSet =
        new MappingSet("leftIndex", "outIndex", "leftBatch", "outgoing", projectMapping, projectMapping);
    final MappingSet rightProjectMappingSet =
        new MappingSet("rightIndex", "outIndex", "rightBatch", "outgoing", projectMapping, projectMapping);


    // create left and right logical expressions
    LogicalExpression[] leftExpr = new LogicalExpression[conditions.size()];
    LogicalExpression[] rightExpr = new LogicalExpression[conditions.size()];

    // Set up project blocks
    int outputFieldId = 0;

    g.setMappingSet(rightProjectMappingSet);
    JConditional jcRightNotMatched = g.getEvalBlock()._if(JExpr.direct("rightIndex").ne(JExpr.lit(-1)));

    g.setMappingSet(leftProjectMappingSet);
    JConditional jcLeftNotMatched = g.getEvalBlock()._if(JExpr.direct("leftIndex").ne(JExpr.lit(-1)));

    {
      int fieldId = 0;

      JExpression recordIndexWithinBatch = JExpr.direct("rightIndex");
      JExpression outIndex = JExpr.direct("outIndex");

      for (Field field : right.getSchema()) {
        final CompleteType fieldType = CompleteType.fromField(field);
        {
          g.setMappingSet(rightProjectMappingSet);
          JVar inVV = g.declareVectorValueSetupAndMember("rightBatch", new TypedFieldId(fieldType, false, fieldId));
          JVar outVV = g.declareVectorValueSetupAndMember("outgoing",
              new TypedFieldId(fieldType, false, outputFieldId));
          jcRightNotMatched._then().add(outVV.invoke("copyFromSafe").arg(recordIndexWithinBatch).arg(outIndex).arg(inVV));
        }

        fieldId++;
        outputFieldId++;
      }
    }

    {
      int fieldId = 0;

      JExpression recordIndexWithinBatch = JExpr.direct("leftIndex");
      JExpression outIndex = JExpr.direct("outIndex");

      for (Field field : left.getSchema()) {
        final CompleteType fieldType = CompleteType.fromField(field);
        {
          g.setMappingSet(leftProjectMappingSet);
          JVar inVV = g.declareVectorValueSetupAndMember("leftBatch", new TypedFieldId(fieldType, false, fieldId));
          JVar outVV = g.declareVectorValueSetupAndMember("outgoing",
              new TypedFieldId(fieldType, false, outputFieldId));
          jcLeftNotMatched._then().add(outVV.invoke("copyFromSafe").arg(recordIndexWithinBatch).arg(outIndex).arg(inVV));
        }

        fieldId++;
        outputFieldId++;
      }
    }

    // Compare block
    for (int i = 0; i < conditions.size(); i++) {
      JoinCondition condition = conditions.get(i);
      leftExpr[i] = producer.materialize(condition.getLeft(), left);
      rightExpr[i] = producer.materialize(condition.getRight(), right);
    }

    g.setMappingSet(mainMappingSet);

    for (int i = 0; i < conditions.size(); i++) {
      g.setMappingSet(leftCompareMappingSet);
      HoldingContainer left = g.addExpr(leftExpr[i], ClassGenerator.BlockCreateMode.MERGE);

      g.setMappingSet(rightCompareMappingSet);
      HoldingContainer right = g.addExpr(rightExpr[i], ClassGenerator.BlockCreateMode.MERGE);

      g.setMappingSet(mainMappingSet);

      final boolean nullsEqual =
          JoinUtils.checkAndReturnSupportedJoinComparator(conditions.get(i)) == Comparator.IS_NOT_DISTINCT_FROM;
      final boolean nullsHigh = true; /* TODO null_high, should use upstream config */
      final boolean ascSorted = true; // TODO: ASC or DESC order sorted?

      // handle case when null != null
      if (!nullsEqual) {
        JConditional jc;
        jc = g.getEvalBlock()._if(left.getIsSet().eq(JExpr.lit(0)).cand(right.getIsSet().eq(JExpr.lit(0))));
        jc._then()._return(JExpr.lit(-1)); // ordering does not really matter in null comparison
      }

      LogicalExpression fh = FunctionGenerationHelper.getOrderingComparator(nullsHigh,
          left, right, producer);
      HoldingContainer out = g.addExpr(fh, ClassGenerator.BlockCreateMode.MERGE);

      // now we have the compare_to value in "out"

      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      // Not equal case
      if (ascSorted) {
        jc._then()._return(out.getValue());
      } else {
        jc._then()._return(out.getValue().minus());
      }
    }
    g.getEvalBlock()._return(JExpr.lit(0));

    comparator = cg.getImplementationClass();
    comparator.setupMergeJoin(context.getClassProducer().getFunctionContext(),
        joinType, leftIterator, rightIterator, outgoing, context.getTargetBatchSize());
  }

  public static class Creator implements DualInputOperator.Creator<MergeJoinPOP>{
    @Override
    public DualInputOperator create(OperatorContext context, MergeJoinPOP config) throws ExecutionSetupException {
      return new MergeJoinOperator(context, config);
    }

  }
}
