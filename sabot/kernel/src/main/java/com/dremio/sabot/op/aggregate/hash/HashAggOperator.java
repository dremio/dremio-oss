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
package com.dremio.sabot.op.aggregate.hash;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.expr.FunctionHolderExpr;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorWriteExpression;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.InjectedOutOfMemoryError;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.op.aggregate.vectorized.nospill.VectorizedHashAggOperatorNoSpill;
import com.dremio.sabot.op.common.hashtable.Comparator;
import com.dremio.sabot.op.common.hashtable.HashTable;
import com.dremio.sabot.op.common.hashtable.HashTableConfig;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JVar;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Calculate a number of aggregate expressions (known as values in this code) for a combination of
 * keys.
 *
 * <p>There must be at least one key to use Hash Aggregation. If there are no aggregation keys
 * (a.k.a. a straight aggregate), planning must choose to use the StreamingAggregation operation.
 *
 * <p>We can hold up to 2^16 batches of 2^16 records each. However, we're actually limited by the
 * Hash Tables load factor (defaults to 0.75).
 *
 * <p>Must hold the entire aggregate table in memory.
 */
@Options
public class HashAggOperator implements SingleInputOperator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(HashAggOperator.class);

  public static final TypeValidators.PositiveLongValidator HASHAGG_MINMAX_CARDINALITY_LIMIT =
      new TypeValidators.PositiveLongValidator(
          "exec.operator.aggregate.minmax_cardinality_limit", Long.MAX_VALUE, 10000);

  // this option sets the capacity of an ArrowBuf in BufferManager from which various buffers may be
  // sliced.
  public static final TypeValidators.PowerOfTwoLongValidator BUF_MANAGER_CAPACITY =
      new TypeValidators.PowerOfTwoLongValidator(
          "exec.operator.aggregate.bufmgr.capacity", 1 << 24, 1 << 16);

  private static final ControlsInjector injector =
      ControlsInjectorFactory.getInjector(HashAggOperator.class);

  @VisibleForTesting public static final String INJECTOR_DO_WORK_OOM = "doWork";
  @VisibleForTesting public static final String INJECTOR_DO_WORK_OOM_ERROR = "doWork-error";

  private final OperatorContext context;
  private final OperatorStats stats;
  private final VectorContainer outgoing;
  private final HashAggregate popConfig;
  private final List<Comparator> comparators;

  private State state = State.NEEDS_SETUP;

  private HashAggregator aggregator;
  private LogicalExpression[] aggrExprs;
  private TypedFieldId[] groupByOutFieldIds;
  private TypedFieldId[] aggrOutFieldIds; // field ids for the outgoing batch
  private VectorAccessible incoming;
  private int outputBatchIndex;
  private boolean isCardinalityLimited;
  private long cardinalityLimit;

  public HashAggOperator(HashAggregate popConfig, OperatorContext context)
      throws ExecutionSetupException {
    this.stats = context.getStats();
    this.context = context;
    this.outgoing = context.createOutputVectorContainer();
    this.popConfig = popConfig;

    final int numGrpByExprs = popConfig.getGroupByExprs().size();
    comparators = Lists.newArrayListWithExpectedSize(numGrpByExprs);
    for (int i = 0; i < numGrpByExprs; i++) {
      // nulls are equal in group by case
      comparators.add(Comparator.IS_NOT_DISTINCT_FROM);
    }

    this.isCardinalityLimited = false;
    final OptionManager options = context.getOptions();
    this.cardinalityLimit = options.getOption(HASHAGG_MINMAX_CARDINALITY_LIMIT);
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    int recordCount = aggregator.outputBatch(outputBatchIndex);
    outputBatchIndex++;
    if (!(outputBatchIndex < aggregator.batchCount())) {
      state = State.DONE;
    }
    outgoing.setRecordCount(recordCount);
    return recordCount;
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    if (aggregator.batchCount() > 0) {
      outputBatchIndex = 0;
      state = State.CAN_PRODUCE;
    } else {
      state = State.DONE;
    }
  }

  @Override
  public void consumeData(int records) throws Exception {
    aggregator.addBatch(records);
    if (isCardinalityLimited && aggregator.numHashTableEntries() > cardinalityLimit) {
      throw UserException.functionError()
          .message(
              "Computing the min() or max() measures of variable-length columns only supported for low-cardinality aggregations")
          .build(logger);
    }
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    injector.injectChecked(
        context.getExecutionControls(), INJECTOR_DO_WORK_OOM, OutOfMemoryException.class);
    injector.injectChecked(
        context.getExecutionControls(), INJECTOR_DO_WORK_OOM_ERROR, InjectedOutOfMemoryError.class);
    this.incoming = accessible;
    this.aggregator = createAggregatorInternal();
    state = State.CAN_CONSUME;
    return outgoing;
  }

  @Override
  public State getState() {
    return state;
  }

  private HashAggregator createAggregatorInternal()
      throws SchemaChangeException, ClassTransformationException, IOException {
    CodeGenerator<HashAggregator> top =
        context.getClassProducer().createGenerator(HashAggregator.TEMPLATE_DEFINITION);
    ClassGenerator<HashAggregator> cg = top.getRoot();
    ClassGenerator<HashAggregator> cgInner = cg.getInnerGenerator("BatchHolder");

    outgoing.clear();

    int numGroupByExprs =
        (popConfig.getGroupByExprs() != null) ? popConfig.getGroupByExprs().size() : 0;
    int numAggrExprs = (popConfig.getAggrExprs() != null) ? popConfig.getAggrExprs().size() : 0;
    aggrExprs = new LogicalExpression[numAggrExprs];
    groupByOutFieldIds = new TypedFieldId[numGroupByExprs];
    aggrOutFieldIds = new TypedFieldId[numAggrExprs];

    int i;

    for (i = 0; i < numGroupByExprs; i++) {
      NamedExpression ne = popConfig.getGroupByExprs().get(i);
      final LogicalExpression expr = context.getClassProducer().materialize(ne.getExpr(), incoming);
      if (expr == null) {
        continue;
      }

      final Field outputField = expr.getCompleteType().toField(ne.getRef());
      ValueVector vv = TypeHelper.getNewVector(outputField, context.getAllocator());

      // add this group-by vector to the output container
      groupByOutFieldIds[i] = outgoing.add(vv);
    }

    for (i = 0; i < numAggrExprs; i++) {
      NamedExpression ne = popConfig.getAggrExprs().get(i);
      final LogicalExpression expr = context.getClassProducer().materialize(ne.getExpr(), incoming);

      if (expr instanceof IfExpression) {
        throw UserException.unsupportedError(
                new UnsupportedOperationException(
                    "Union type not supported in aggregate functions"))
            .build(logger);
      }

      if (expr == null) {
        continue;
      }

      final Field outputField = expr.getCompleteType().toField(ne.getRef());
      ValueVector vv = TypeHelper.getNewVector(outputField, context.getAllocator());
      aggrOutFieldIds[i] = outgoing.add(vv);

      aggrExprs[i] = new ValueVectorWriteExpression(aggrOutFieldIds[i], expr, true);

      if (isCardinalityLimitNeeded(expr, expr.getCompleteType())) {
        isCardinalityLimited = true;
      }
    }

    setupUpdateAggrValues(cgInner);
    setupGetIndex(cg);
    cg.getBlock("resetValues")._return(JExpr.TRUE);

    outgoing.buildSchema(SelectionVectorMode.NONE);
    HashAggregator agg = top.getImplementationClass();

    HashTableConfig htConfig =
        // TODO - fix the validator on this option
        new HashTableConfig(
            (int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE),
            HashTable.DEFAULT_LOAD_FACTOR,
            popConfig.getGroupByExprs(),
            null /* no probe exprs */,
            comparators);

    agg.setup(
        popConfig,
        htConfig,
        context.getClassProducer(),
        stats,
        context.getAllocator(),
        incoming,
        aggrExprs,
        cgInner.getWorkspaceTypes(),
        groupByOutFieldIds,
        outgoing,
        context.getOptions());

    return agg;
  }

  // Certain measures have to be cardinality-limited, or else we run out of heap
  private boolean isCardinalityLimitNeeded(LogicalExpression expr, CompleteType exprType) {
    // Cardinality is limited for min()/max() of variable-length types
    if (!(expr instanceof FunctionHolderExpr)) {
      return false;
    }
    String functionName = ((FunctionHolderExpr) expr).getName();
    if (!"min".equals(functionName) && !"max".equals(functionName)) {
      return false;
    }
    return !exprType.isFixedWidthScalar();
  }

  @SuppressWarnings("checkstyle:LocalFinalVariableName")
  private void setupUpdateAggrValues(ClassGenerator<HashAggregator> cg) {
    final GeneratorMapping UPDATE_AGGR_INSIDE =
        GeneratorMapping.create(
            "setupInterior", "updateAggrValuesInternal", "resetValues", "cleanup");
    final GeneratorMapping UPDATE_AGGR_OUTSIDE =
        GeneratorMapping.create("setupInterior", "outputRecordValues", "resetValues", "cleanup");
    final MappingSet UpdateAggrValuesMapping =
        new MappingSet(
            "incomingRowIdx",
            "outRowIdx",
            "htRowIdx",
            "incoming",
            "outgoing",
            "aggrValuesContainer",
            UPDATE_AGGR_INSIDE,
            UPDATE_AGGR_OUTSIDE,
            UPDATE_AGGR_INSIDE);

    cg.setMappingSet(UpdateAggrValuesMapping);

    for (LogicalExpression aggr : aggrExprs) {
      cg.addExpr(aggr, ClassGenerator.BlockCreateMode.NEW_BLOCK);
    }
  }

  private void setupGetIndex(ClassGenerator<HashAggregator> cg) {
    switch (incoming.getSchema().getSelectionVectorMode()) {
      case FOUR_BYTE:
        {
          JVar var = cg.declareClassField("sv4_", cg.getModel()._ref(SelectionVector4.class));
          cg.getBlock("doSetup")
              .assign(var, JExpr.direct("incoming").invoke("getSelectionVector4"));
          cg.getBlock("getVectorIndex")._return(var.invoke("get").arg(JExpr.direct("recordIndex")));
          return;
        }
      case NONE:
        {
          cg.getBlock("getVectorIndex")._return(JExpr.direct("recordIndex"));
          return;
        }
      case TWO_BYTE:
        {
          JVar var = cg.declareClassField("sv2_", cg.getModel()._ref(SelectionVector2.class));
          cg.getBlock("doSetup")
              .assign(var, JExpr.direct("incoming").invoke("getSelectionVector2"));
          cg.getBlock("getVectorIndex")
              ._return(var.invoke("getIndex").arg(JExpr.direct("recordIndex")));
          return;
        }
      default:
        throw new IllegalStateException(
            "Unhandled SelectionVectorMode: " + incoming.getSchema().getSelectionVectorMode());
    }
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(
      OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    //    System.out.println("HA1######################## Nano Processing is " +
    // stats.getProcessingNanos());
    //    System.out.println("\tPeak Mem: " + context.getAllocator().getPeakMemoryAllocation());
    AutoCloseables.close(aggregator, outgoing);
  }

  public static class HashAggCreator implements SingleInputOperator.Creator<HashAggregate> {

    @Override
    public SingleInputOperator create(OperatorContext context, HashAggregate operator)
        throws ExecutionSetupException {
      if (operator.isVectorize()) {
        boolean useSpill = operator.isUseSpill();
        if (context
                .getOptions()
                .getOption(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR)
            && useSpill) {
          return new VectorizedHashAggOperator(operator, context);
        } else {
          return new VectorizedHashAggOperatorNoSpill(operator, context);
        }
      } else {
        return new HashAggOperator(operator, context);
      }
    } // create
  }
}
