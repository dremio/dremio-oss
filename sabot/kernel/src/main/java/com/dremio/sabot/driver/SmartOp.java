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
package com.dremio.sabot.driver;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.Describer;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.util.BatchPrinter;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.dremio.sabot.op.spi.Operator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.spi.TerminalOperator;
import java.util.Arrays;
import java.util.Optional;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;

/**
 * A set of utility classes that allows us to centralize operator management without using
 * inheritance and confusing state trees.
 *
 * @param <T> The type of operator wrapped.
 */
public abstract class SmartOp<T extends Operator> implements Wrapped<T> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SmartOp.class);

  // Flags to control where to log the records. Disabled by default.
  private static final boolean DEBUG_PRINT = false;
  private static final boolean DEBUG_LOG = false;
  private static final boolean PRINT_STATS_ON_CLOSE = false;

  protected final T inner;
  private final OperatorContext context;
  private final PhysicalOperator popConfig;
  private final FunctionLookupContext functionLookupContext;
  protected final OperatorStats stats;

  private SmartOp(
      T inner,
      OperatorContext context,
      PhysicalOperator popConfig,
      FunctionLookupContext functionLookupContext) {
    super();
    this.inner = inner;
    this.context = context;
    this.popConfig = popConfig;
    this.functionLookupContext = functionLookupContext;
    this.stats = context.getStats();
  }

  void checkSchema(BatchSchema initialSchema) {
    int propsSchemaHashCode = popConfig.getProps().getSchemaHashCode();
    int initialSchemaHashCode =
        initialSchema.clone(BatchSchema.SelectionVectorMode.NONE).toByteString().hashCode();
    checkState(
        propsSchemaHashCode == initialSchemaHashCode,
        String.format(
            "Schema checksums do not match. Actual schema:%d Config Schema:%d",
            initialSchemaHashCode, propsSchemaHashCode));
  }

  @Override
  public int getOperatorId() {
    return popConfig.getProps().getOperatorId();
  }

  @Override
  public void workOnOOB(OutOfBandMessage message) {
    inner.workOnOOB(message);
  }

  @Override
  public boolean shrinkMemory(long memoryUsed) throws Exception {
    checkState(
        inner instanceof ShrinkableOperator,
        "A message to shrinkMemory has been sent to an Operator that cannot shrinkMemory");
    ShrinkableOperator shrinkableOperator = (ShrinkableOperator) inner;
    long currentMemoryUsed = shrinkableOperator.shrinkableMemory();
    boolean doneSpilling = false;
    try {
      doneSpilling = shrinkableOperator.shrinkMemory(memoryUsed);
      return doneSpilling;
    } finally {
      logger.debug(
          "Operator {} reduced memory usage by {} bytes",
          shrinkableOperator.getOperatorId(),
          currentMemoryUsed - shrinkableOperator.shrinkableMemory());
      if (doneSpilling) {
        logger.info(
            "Operator {} spilled {} bytes",
            shrinkableOperator.getOperatorId(),
            (memoryUsed - shrinkableOperator.shrinkableMemory()));
        if (shrinkableOperator.canUseTooMuchMemoryInAPump()) {
          shrinkableOperator.setLimit(shrinkableOperator.getAllocatedMemory());
        }
      }
    }
  }

  @Override
  public T getInner() {
    return inner;
  }

  @Override
  public OperatorContext getContext() {
    return context;
  }

  void logClose() {
    FragmentHandle h = context.getFragmentHandle();
    logger.debug(
        String.format(
            "Closing operator: %d:%d:%d",
            h.getMajorFragmentId(), h.getMinorFragmentId(), context.getStats().getOperatorId()));
  }

  private static String getOperatorName(int operatorType) {
    CoreOperatorType type = CoreOperatorType.valueOf(operatorType);
    return type == null ? String.format("Unknown (%d)", operatorType) : type.name();
  }

  protected RuntimeException contextualize(Throwable e) {

    String operatorName = "Unknown";
    int operatorId = -1;
    try {
      operatorName = getOperatorName(context.getStats().getOperatorType());
    } catch (Exception ex) {
      e.addSuppressed(ex);
    }

    try {
      operatorId = context.getStats().getOperatorId();
    } catch (Exception ex) {
      e.addSuppressed(ex);
    }

    final FragmentHandle h = context.getFragmentHandle();
    UserException.Builder builder;
    String operatorInfo = String.format("SqlOperatorImpl %s", operatorName);
    String locationInfo =
        String.format(
            "Location %d:%d:%d", h.getMajorFragmentId(), h.getMinorFragmentId(), operatorId);
    if (e instanceof UserException) {
      context.getNodeDebugContextProvider().addErrorOrigin((UserException) e);
      UserBitShared.DremioPBError.ErrorType errorType = ((UserException) e).getErrorType();
      if (errorType == UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY) {
        return (UserException) e;
      }
    }
    if (ErrorHelper.isDirectMemoryException(e)) {
      builder = UserException.memoryError(e);
      context.getNodeDebugContextProvider().addMemoryContext(builder, e);
      builder.addContext(operatorInfo);
      builder.addContext(locationInfo);
    } else if (ErrorHelper.isJavaHeapOutOfMemory(e)) {
      builder = UserException.memoryError(e);
      context.getNodeDebugContextProvider().addHeapMemoryContext(builder, e);
      builder.addContext(operatorInfo);
      builder.addContext(locationInfo);
    } else {
      builder =
          UserException.systemError(e)
              .message("General execution failure.")
              .addContext(operatorInfo)
              .addContext(locationInfo);
      context.getNodeDebugContextProvider().addErrorOrigin(builder);
    }
    return builder.build(logger);
  }

  public static SmartSingleInput contextualize(
      SingleInputOperator operator,
      OperatorContext context,
      PhysicalOperator popConfig,
      FunctionLookupContext functionLookupContext) {
    return new SmartSingleInput(operator, context, popConfig, functionLookupContext);
  }

  public static SmartDualInput contextualize(
      DualInputOperator operator,
      OperatorContext context,
      PhysicalOperator popConfig,
      FunctionLookupContext functionLookupContext) {
    return new SmartDualInput(operator, context, popConfig, functionLookupContext);
  }

  public static SmartTerminal contextualize(
      TerminalOperator operator,
      OperatorContext context,
      PhysicalOperator popConfig,
      FunctionLookupContext functionLookupContext) {
    return new SmartTerminal(operator, context, popConfig, functionLookupContext);
  }

  public static SmartProducer contextualize(
      ProducerOperator operator,
      OperatorContext context,
      PhysicalOperator popConfig,
      FunctionLookupContext functionLookupContext) {
    return new SmartProducer(operator, context, popConfig, functionLookupContext);
  }

  static class SmartSingleInput extends SmartOp<SingleInputOperator>
      implements SingleInputOperator {

    private VectorAccessible incoming;
    private VectorAccessible outgoing;
    private BatchSchema initialSchema;

    public SmartSingleInput(
        SingleInputOperator inner,
        OperatorContext context,
        PhysicalOperator popConfig,
        FunctionLookupContext functionLookupContext) {
      super(inner, context, popConfig, functionLookupContext);
    }

    @Override
    public int outputData() throws Exception {
      stats.startProcessing(inner.getState());
      try {
        int outputRecords = inner.outputData();
        stats.recordBatchOutput(outputRecords, VectorUtil.getSize(outgoing));
        return verify(initialSchema, outgoing, outputRecords);
      } catch (Exception | AssertionError | AbstractMethodError e) {
        logger.error("Unexpected exception occurred", e);
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public <OUT, IN, EXCEP extends Throwable> OUT accept(
        OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
      try {
        return visitor.visitSingleInput(this, value);
      } catch (RuntimeException e) {
        throw contextualize(e);
      }
    }

    @Override
    public void close() throws Exception {
      logClose();
      stats.startProcessing(inner.getState());
      try {
        inner.close();
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }

      if (PRINT_STATS_ON_CLOSE) {
        System.out.println(stats);
      }
    }

    @Override
    public void noMoreToConsume() throws Exception {
      stats.startProcessing(inner.getState());
      try {
        inner.noMoreToConsume();
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public void consumeData(int records) throws Exception {
      stats.startProcessing(inner.getState());
      try {
        stats.batchReceived(0, records, VectorUtil.getSize(incoming));
        inner.consumeData(records);
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public VectorAccessible setup(VectorAccessible accessible) throws Exception {
      stats.startProcessing(inner.getState());
      try {
        stats.startSetup();
        try {
          incoming = accessible;
          outgoing = inner.setup(accessible);
          initialSchema = outgoing.getSchema();
          stats.setSchema(initialSchema);
          return outgoing;
        } catch (Exception | AssertionError | AbstractMethodError e) {
          throw contextualize(e);
        } finally {
          stats.stopSetup();
        }
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public State getState() {
      return inner.getState();
    }
  }

  static class SmartTerminal extends SmartOp<TerminalOperator> implements TerminalOperator {

    private VectorAccessible incoming;

    public SmartTerminal(
        TerminalOperator inner,
        OperatorContext context,
        PhysicalOperator popConfig,
        FunctionLookupContext functionLookupContext) {
      super(inner, context, popConfig, functionLookupContext);
    }

    @Override
    public <OUT, IN, EXCEP extends Throwable> OUT accept(
        OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
      try {
        return visitor.visitTerminalOperator(this, value);
      } catch (RuntimeException e) {
        throw contextualize(e);
      }
    }

    @Override
    public void close() throws Exception {
      logClose();
      stats.startProcessing(inner.getState());
      try {
        inner.close();
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }

      if (PRINT_STATS_ON_CLOSE) {
        System.out.println(stats);
      }
    }

    @Override
    public void noMoreToConsume() throws Exception {
      stats.startProcessing(inner.getState());
      try {
        inner.noMoreToConsume();
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public void consumeData(int records) throws Exception {
      stats.startProcessing(inner.getState());
      try {
        stats.batchReceived(0, records, VectorUtil.getSize(incoming));
        inner.consumeData(records);
        stats.recordBatchOutput(records, VectorUtil.getSize(incoming));
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public void setup(VectorAccessible accessible) throws Exception {
      stats.startProcessing(inner.getState());
      try {
        stats.startSetup();
        try {
          incoming = accessible;
          inner.setup(accessible);
        } catch (Exception | AssertionError | AbstractMethodError e) {
          throw contextualize(e);
        } finally {
          stats.stopSetup();
        }
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public State getState() {
      return inner.getState();
    }

    @Override
    public void receivingFragmentFinished(FragmentHandle handle) throws Exception {
      try {
        inner.receivingFragmentFinished(handle);
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      }
    }
  }

  static class SmartDualInput extends SmartOp<DualInputOperator> implements DualInputOperator {

    private VectorAccessible outgoing;
    private VectorAccessible left;
    private VectorAccessible right;
    private BatchSchema initialSchema;

    public SmartDualInput(
        DualInputOperator inner,
        OperatorContext context,
        PhysicalOperator popConfig,
        FunctionLookupContext functionLookupContext) {
      super(inner, context, popConfig, functionLookupContext);
    }

    @Override
    public int outputData() throws Exception {
      stats.startProcessing(inner.getState());
      try {
        int outputRecords = inner.outputData();
        stats.recordBatchOutput(outputRecords, VectorUtil.getSize(outgoing));
        return verify(initialSchema, outgoing, outputRecords);
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public <OUT, IN, EXCEP extends Throwable> OUT accept(
        OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
      try {
        return visitor.visitDualInput(this, value);
      } catch (RuntimeException e) {
        throw contextualize(e);
      }
    }

    @Override
    public void close() throws Exception {
      logClose();
      stats.startProcessing(inner.getState());
      try {
        inner.close();
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }

      if (PRINT_STATS_ON_CLOSE) {
        System.out.println(stats);
      }
    }

    @Override
    public void noMoreToConsumeLeft() throws Exception {
      stats.startProcessing(inner.getState());
      try {
        inner.noMoreToConsumeLeft();
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public void noMoreToConsumeRight() throws Exception {
      stats.startProcessing(inner.getState());
      try {
        inner.noMoreToConsumeRight();
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public void consumeDataLeft(int records) throws Exception {
      stats.startProcessing(inner.getState());
      try {
        stats.batchReceived(0, records, VectorUtil.getSize(left));
        inner.consumeDataLeft(records);
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public void consumeDataRight(int records) throws Exception {
      stats.startProcessing(inner.getState());
      try {
        stats.batchReceived(1, records, VectorUtil.getSize(right));
        inner.consumeDataRight(records);
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public VectorAccessible setup(VectorAccessible left, VectorAccessible right) throws Exception {
      stats.startProcessing(inner.getState());
      try {
        stats.startSetup();
        try {
          this.left = left;
          this.right = right;
          outgoing = inner.setup(left, right);
          initialSchema = outgoing.getSchema();
          stats.setSchema(initialSchema);
          return outgoing;
        } catch (Exception | AssertionError | AbstractMethodError e) {
          throw contextualize(e);
        } finally {
          stats.stopSetup();
        }
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public State getState() {
      return inner.getState();
    }
  }

  static class SmartProducer extends SmartOp<ProducerOperator> implements ProducerOperator {
    private VectorAccessible outgoing;
    private BatchSchema initialSchema;

    public SmartProducer(
        ProducerOperator inner,
        OperatorContext context,
        PhysicalOperator popConfig,
        FunctionLookupContext functionLookupContext) {
      super(inner, context, popConfig, functionLookupContext);
    }

    @Override
    public int outputData() throws Exception {
      stats.startProcessing(inner.getState());
      try {
        int outputRecords = inner.outputData();
        stats.recordBatchOutput(outputRecords, VectorUtil.getSize(outgoing));
        return verify(initialSchema, outgoing, outputRecords);
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public <OUT, IN, EXCEP extends Throwable> OUT accept(
        OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
      try {
        return visitor.visitProducer(this, value);
      } catch (RuntimeException e) {
        throw contextualize(e);
      }
    }

    @Override
    public void close() throws Exception {
      logClose();
      stats.startProcessing(inner.getState());
      try {
        inner.close();
      } catch (Exception | AssertionError | AbstractMethodError e) {
        throw contextualize(e);
      } finally {
        stats.stopProcessing(inner.getState());
      }

      if (PRINT_STATS_ON_CLOSE) {
        System.out.println(stats);
      }
    }

    @Override
    public VectorAccessible setup() throws Exception {
      stats.startProcessing(inner.getState());
      try {
        stats.startSetup();
        try {
          outgoing = inner.setup();
          initialSchema = outgoing.getSchema();
          stats.setSchema(initialSchema);
          return outgoing;
        } catch (Exception | AssertionError | AbstractMethodError e) {
          throw contextualize(e);
        } finally {
          stats.stopSetup();
        }
      } finally {
        stats.stopProcessing(inner.getState());
      }
    }

    @Override
    public State getState() {
      return inner.getState();
    }
  }

  @Override
  public String toString() {
    return inner.toString();
  }

  protected int verify(BatchSchema initialSchema, VectorAccessible outgoing, int records) {
    checkArgument(outgoing != null, "Outgoing vectors not available.");
    outgoing.getSchema(); // check if schema is set; see VectorContainer#getSchema
    assert actualVerify(initialSchema, outgoing, records);
    return records;
  }

  private boolean actualVerify(BatchSchema initialSchema, VectorAccessible outgoing, int records) {
    checkArgument(outgoing.getSchema() != null, "Schema not set.");
    checkArgument(
        outgoing.getSchema().equals(initialSchema),
        "Schema changed unexpectedly. Original: %s, New: %s.",
        initialSchema,
        outgoing.getSchema());
    if (records == 0) {
      if (DEBUG_PRINT || DEBUG_LOG) {
        FragmentHandle h = context.getFragmentHandle();
        String op =
            String.format(
                "%s:%d:%d:%d --> (%d)",
                getOperatorName(context.getStats().getOperatorType()),
                h.getMajorFragmentId(),
                h.getMinorFragmentId(),
                context.getStats().getOperatorId(),
                records);
        if (DEBUG_LOG) {
          logger.debug(op);
        }
        if (DEBUG_PRINT) {
          System.out.println(op);
        }
      }
      return true;
    }

    checkArgument(
        outgoing.getRecordCount() == records,
        "Reported output record count %s not equal to VectorContainer.getRecordCount() of %s",
        records,
        outgoing.getRecordCount());

    if (!(inner instanceof ScanOperator)) {
      checkSchema(outgoing.getSchema());
    }

    // check selection vector matches.
    switch (outgoing.getSchema().getSelectionVectorMode()) {
      case FOUR_BYTE:
        checkArgument(
            outgoing.getSelectionVector4().getCount() == records,
            "SV4 doesn't match outgoing records.");
        break;
      case TWO_BYTE:
        checkArgument(
            outgoing.getSelectionVector2().getCount() == records,
            "SV2 doesn't match outgoing records.");
        break;
      default:
        break;
    }

    for (VectorWrapper<?> w : outgoing) {
      switch (outgoing.getSchema().getSelectionVectorMode()) {
        case FOUR_BYTE:
          break;
        case NONE:
          {
            ValueVector vector = w.getValueVector();
            checkArgument(
                vector instanceof ZeroVector || vector.getValueCount() == records,
                "Output value count %s not equal to vector count %s for vector: %s",
                records,
                vector.getValueCount(),
                Describer.describe(vector.getField()));
            break;
          }
        case TWO_BYTE:
          {
            ValueVector vector = w.getValueVector();
            checkArgument(
                vector.getValueCount() >= records,
                "SV2: Top level value count %s should be less than equal to value count %s of vector %s when in SV2 mode.",
                records,
                vector.getValueCount(),
                Describer.describe(vector.getField()));
          }
          break;
        default:
          break;
      }

      // validate variable width vector consistency
      if (w.getValueVector() instanceof BaseVariableWidthVector) {
        BaseVariableWidthVector v = (BaseVariableWidthVector) w.getValueVector();
        VariableLengthValidator.validateVariable(v, records);
      }
    }

    if (DEBUG_PRINT || DEBUG_LOG) {
      FragmentHandle h = context.getFragmentHandle();
      String op =
          String.format(
              "%s:%d:%d:%d --> (%d), %s",
              getOperatorName(context.getStats().getOperatorType()),
              h.getMajorFragmentId(),
              h.getMinorFragmentId(),
              context.getStats().getOperatorId(),
              records,
              outgoing.getSchema());
      if (DEBUG_LOG) {
        logger.debug(op);
      }
      if (DEBUG_PRINT) {
        System.out.println(op);
      }
      BatchPrinter.printBatch(outgoing, DEBUG_PRINT, DEBUG_LOG);
    }
    return true;
  }

  public static String masterStateToStr(int masterState) {
    int tmpMasterState;
    String masterStateStr;
    if (masterState < 0) {
      masterStateStr = "-";
      tmpMasterState = -masterState;
    } else {
      tmpMasterState = masterState;
      masterStateStr = "";
    }
    MasterState[] stateValues = Operator.MasterState.values();
    Optional<MasterState> returnMasterState =
        Arrays.stream(stateValues).filter(s -> s.ordinal() == tmpMasterState).findFirst();
    if (returnMasterState.isPresent()) {
      return masterStateStr.concat(returnMasterState.get().name());
    } else {
      return masterStateStr.concat("UNKNOWN_STATE");
    }
  }
}
