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

import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.parquet.CopyingFilteringReader;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * FilteringCoercionReader for Hive-Parquet tables
 * TODO: Remove duplicate code with FilteringCoercionReader
 */
public class HiveParquetCoercionReader extends AbstractRecordReader {

  private CopyingFilteringReader filteringReader;
  private final ParquetFilterCondition filterCondition;
  private final boolean filterConditionPresent; // true if filter condition is specified

  private ScanOperator.ScanMutator filteringReaderInputMutator;
  protected Stopwatch javaCodeGenWatch = Stopwatch.createUnstarted();
  protected Stopwatch gandivaCodeGenWatch = Stopwatch.createUnstarted();

  private NextMethodState nextMethodState;
  private boolean setupCalledByFilteringReader; // setUp() state
  private boolean closeCalledByFilteringReader; // close() state
  private VectorContainer projectorOutput; // can be this.outgoing or this.filteringReaderInputMutator.container
  private int recordCount;
  private boolean initialProjectorSetUpDone;
  protected final OperatorContext context;
  SampleMutator mutator;
  protected OutputMutator outputMutator;
  private OutputMutator outgoingMutator;
  protected VectorContainer outgoing;
  protected VectorContainer incoming;
  protected final RecordReader inner;
  protected final ExpressionEvaluationOptions projectorOptions;
  private final HiveParquetReader hiveParquetReader;

  private BatchSchema originalSchema; // actual schema including varchar and non-varchar fields


  public static HiveParquetCoercionReader newInstance(
      OperatorContext context, List<SchemaPath> columns, RecordReader inner,
      BatchSchema originalSchema, TypeCoercion hiveTypeCoercion,
      List<ParquetFilterCondition> filterConditions) {

    return new HiveParquetCoercionReader(context, columns, inner,
      originalSchema, hiveTypeCoercion, filterConditions);
  }

  private HiveParquetCoercionReader(OperatorContext context, List<SchemaPath> columns, RecordReader inner,
                                   BatchSchema originalSchema, TypeCoercion hiveTypeCoercion,
                                   List<ParquetFilterCondition> filterConditions) {
    super(context, columns);
    mutator = new SampleMutator(context.getAllocator());
    this.incoming = mutator.getContainer();
    this.outgoing = new VectorContainer(context.getAllocator());
    this.inner = inner;
    this.context = context;
    this.projectorOptions = new ExpressionEvaluationOptions(context.getOptions());
    this.projectorOptions.setCodeGenOption(context.getOptions().getOption(ExecConstants.QUERY_EXEC_OPTION.getOptionName()).getStringVal());

    this.originalSchema = originalSchema;

    hiveParquetReader = new HiveParquetReader(mutator,
      context, columns, hiveTypeCoercion, javaCodeGenWatch, gandivaCodeGenWatch, originalSchema);
    if (filterConditions != null && !filterConditions.isEmpty()) {
      Preconditions.checkArgument(filterConditions.size() == 1,
          "we only support a single filterCondition per rowGroupScan for now");
      filterCondition = filterConditions.get(0);
      this.filteringReader = new CopyingFilteringReader(this, context, filterCondition.getExpr());
      filterConditionPresent = true;
    } else {
      filterCondition = null;
      this.filteringReader = null;
      filterConditionPresent = false;
    }

    initialProjectorSetUpDone = false;
    resetReaderState();
  }

  private void resetReaderState() {
    setupCalledByFilteringReader = false;
    closeCalledByFilteringReader = false;
    projectorOutput = outgoing;
    recordCount = 0;
    nextMethodState = NextMethodState.NOT_CALLED_BY_FILTERING_READER;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    if (setupCalledByFilteringReader) {
      this.filteringReaderInputMutator = (ScanOperator.ScanMutator) output;
    } else {
      this.outputMutator = output;
      inner.setup(mutator);
      incoming.buildSchema();
      // reset the schema change callback
      mutator.getAndResetSchemaChanged();

      for (Field field : originalSchema.getFields()) {
        ValueVector vector = outputMutator.getVector(field.getName());
        if (vector == null) {
          continue;
        }
        outgoing.add(vector);
      }
      outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);

      if (filterConditionPresent) {
        setupCalledByFilteringReader = true;
        filteringReader.setup(output);
        setupCalledByFilteringReader = false;
      }
    }
  }

  protected void setupProjector(OutputMutator outgoing, VectorContainer projectorOutput) {
    hiveParquetReader.setupProjector(outgoing, incoming, projectorOptions, projectorOutput);
    outgoing.getAndResetSchemaChanged();
  }

  protected void runProjector(int recordCount) {
    hiveParquetReader.runProjector(recordCount, incoming);
  }

  @Override
  public int next() {
    switch (nextMethodState) {
      case FIRST_CALL_BY_FILTERING_READER:
        // called by this.filteringReader. we just need to return number of records written by projector
        nextMethodState = HiveParquetCoercionReader.NextMethodState.REPEATED_CALL_BY_FILTERING_READER;
        break;
      case NOT_CALLED_BY_FILTERING_READER:
        outgoingMutator = this.outputMutator;
        recordCount = inner.next();
        if (recordCount == 0) {
          return 0;
        }
        if (filterConditionPresent && filterCondition.isModifiedForPushdown()) {
          projectorOutput = filteringReaderInputMutator.getContainer();
          this.outgoingMutator = filteringReaderInputMutator;
        }

        // we haven't set up projector in this.setup() since we didn't know the container for projector's output
        // this is needed as incoming mutator(this.mutator) will not detect a schema change if the schema differs from first batch
        if (!initialProjectorSetUpDone) {
          setupProjector(this.outgoingMutator, projectorOutput);
          initialProjectorSetUpDone = true;
        }

        if (mutator.getSchemaChanged()) {
          incoming.buildSchema();
          // reset the schema change callback
          mutator.getAndResetSchemaChanged();
          setupProjector(this.outgoingMutator, projectorOutput);
        }
        incoming.setAllCount(recordCount);

        runProjector(recordCount);
        projectorOutput.setAllCount(recordCount);

        if (filterConditionPresent && filterCondition.isModifiedForPushdown()) {
          nextMethodState = HiveParquetCoercionReader.NextMethodState.FIRST_CALL_BY_FILTERING_READER;
          recordCount = filteringReader.next();
          outgoing.setAllCount(recordCount);
          int recordCountCopy = recordCount;
          resetReaderState();
          return recordCountCopy;
        }
        break;
      case REPEATED_CALL_BY_FILTERING_READER:
        recordCount = inner.next();
        if (recordCount == 0) {
          return 0;
        }

        if (mutator.getSchemaChanged()) {
          incoming.buildSchema();
          // reset the schema change callback
          mutator.getAndResetSchemaChanged();
          setupProjector(this.outgoingMutator, projectorOutput);
        }
        incoming.setAllCount(recordCount);

        runProjector(recordCount);
    }
    return recordCount;
  }

  @Override
  public void close() throws Exception {
    if (!closeCalledByFilteringReader) {
      try {
        AutoCloseables.close(hiveParquetReader, outgoing, inner);
      } finally {
        if (filterConditionPresent) {
          closeCalledByFilteringReader = true;
          AutoCloseables.close(filteringReader);
        }
      }
    }
    closeCalledByFilteringReader = false;
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    // do not allocate if called by FilteringReader
    if (nextMethodState == HiveParquetCoercionReader.NextMethodState.NOT_CALLED_BY_FILTERING_READER) {
      super.allocate(vectorMap);
      inner.allocate(mutator.getFieldVectorMap());
    }
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    inner.addRuntimeFilter(runtimeFilter);
  }

  private enum NextMethodState {
    NOT_CALLED_BY_FILTERING_READER,
    FIRST_CALL_BY_FILTERING_READER,
    REPEATED_CALL_BY_FILTERING_READER
  }
}
