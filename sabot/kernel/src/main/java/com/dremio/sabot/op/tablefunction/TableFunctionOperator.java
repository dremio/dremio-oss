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
package com.dremio.sabot.op.tablefunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.AbstractTableFunctionPOP;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.util.VectorUtil;
import com.dremio.exec.work.foreman.UnsupportedFunctionException;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Preconditions;

/**
 * Table function operator
 */
public class TableFunctionOperator implements SingleInputOperator {
  private static final Logger logger = LoggerFactory.getLogger(TableFunctionOperator.class);

  public enum Metric implements MetricDef {
    NUM_DATA_FILE,
    NUM_MANIFEST_FILE,
    MAX_SCHEMA_WIDTH;

    @Override
    public int metricId() {
      return ScanOperator.Metric.values().length + ordinal();
    }
  }

  private State state = State.NEEDS_SETUP;
  private VectorAccessible input;
  private VectorAccessible output;

  private  final OperatorContext context;
  private final AbstractTableFunctionPOP functionOperator;
  private final FragmentExecutionContext fec;
  private final TableFunctionFactory tableFunctionFactory;
  private TableFunction tableFunction;
  private int currentrow = -1;
  private int records;
  static long BATCH_LIMIT_BYTES = 1_048_576;

  public TableFunctionOperator(FragmentExecutionContext fec, OperatorContext context, AbstractTableFunctionPOP operator) {
    this.context = context;
    this.functionOperator = operator;
    this.fec = fec;
    this.tableFunctionFactory = new InternalTableFunctionFactory();
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);
    int outputRecords = 0;
    int totalOutputRecords = 0;
    int allowedTargetBatchSize;

    if (records == 0) {
      currentrow = -1;
      state = State.CAN_CONSUME;
      context.getStats().recordBatchOutput(0, 0);
      return 0;
    }

    if (currentrow == -1) {
      currentrow++;
      tableFunction.startRow(currentrow);
    }

    allowedTargetBatchSize = this.context.getTargetBatchSize();

    if (totalOutputRecords < allowedTargetBatchSize && currentrow < records) {
      int maxOutputRecordCount = allowedTargetBatchSize - totalOutputRecords;
      while ((outputRecords = tableFunction.processRow(totalOutputRecords, maxOutputRecordCount)) == 0) {
        tableFunction.closeRow();
        currentrow++;
        if (currentrow >= records) {
          break;
        }
        tableFunction.startRow(currentrow);
      }

      Preconditions.checkState(outputRecords <= maxOutputRecordCount, "Table function returned unexpected number of records");
      totalOutputRecords += outputRecords;
    }
    if (functionOperator.getFunction().getFillBatch()) {
      long firstRowSize = -1L;
      if (outputRecords != 0) {
        firstRowSize = tableFunction.getFirstRowSize();
      }
      if (firstRowSize > 0) {
        allowedTargetBatchSize = (int) Math.min(allowedTargetBatchSize, BATCH_LIMIT_BYTES / firstRowSize);
        logger.debug("firstRowSize: {}, allowedTargetBatchSize: {}", firstRowSize, allowedTargetBatchSize);
      }

      while (totalOutputRecords < allowedTargetBatchSize && currentrow < records) {
        int maxOutputRecordCount = allowedTargetBatchSize - totalOutputRecords;
        while ((outputRecords = tableFunction.processRow(totalOutputRecords, maxOutputRecordCount)) == 0) {
          tableFunction.closeRow();
          currentrow++;
          if (currentrow >= records) {
            break;
          }
          tableFunction.startRow(currentrow);
        }

        Preconditions.checkState(outputRecords <= maxOutputRecordCount, "Table function returned unexpected number of records");
        totalOutputRecords += outputRecords;

        if (!functionOperator.getFunction().getFillBatch()) {
          break;
        }
      }
    }

    if (currentrow >= records) {
      currentrow = -1;
      state = State.CAN_CONSUME;
    }
    context.getStats().recordBatchOutput(totalOutputRecords, VectorUtil.getSize(output));

    return totalOutputRecords;
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    // if there are any buffered records remaining, we transition back to CAN_PRODUCE state else we are done
    state = tableFunction.hasBufferedRemaining() ? State.CAN_PRODUCE : State.DONE;
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);
    this.records = records;
    state = State.CAN_PRODUCE;
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    state = State.CAN_CONSUME;
    tableFunction = tableFunctionFactory.createTableFunction(fec, context, functionOperator.getProps(), functionOperator.getFunction());
    input = accessible;
    output = tableFunction.setup(accessible);
    context.getStats().setRecordOutput(true);
    return output;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void workOnOOB(OutOfBandMessage message) {
    tableFunction.workOnOOB(message);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(tableFunction);
  }

  /**
   * Table function operator creator
   */
  public static class TableFunctionOperatorCreator implements SingleInputOperator.Creator<TableFunctionPOP> {
    @Override
    public SingleInputOperator create(OperatorContext context, TableFunctionPOP operator) throws ExecutionSetupException {
      throw new UnsupportedFunctionException("Not implemented");
    }

    @Override
    public SingleInputOperator create(FragmentExecutionContext fec, OperatorContext context, TableFunctionPOP operator) throws ExecutionSetupException {
      return new TableFunctionOperator(fec, context, operator);
    }
  }
}
