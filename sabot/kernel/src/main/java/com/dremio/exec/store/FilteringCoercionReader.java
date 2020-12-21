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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.parquet.CopyingFilteringReader;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;

/**
 * Similar to CoercionReader, additionally applies filter after coercion if the filter was modified by inner reader
 * for pushdown
 * TODO(DX-26038): duplicate logic with HiveParquetCoercionReader; make HiveParquetCoercionReaader extend FilteringCoercionReader
 * and remove the duplicate logic
 */
public class FilteringCoercionReader extends CoercionReader {
  private static final boolean DEBUG_PRINT = false;

  protected int recordCount;
  private NextMethodState nextMethodState;
  private VectorContainer projectorOutput; // can be this.outgoing or this.filteringReaderInputMutator.container
  private boolean setupCalledByFilteringReader; // setUp() state
  private boolean closeCalledByFilteringReader; // close() state

  private boolean needsFilteringAfterCoercion; // true if a pushdown filter is modified
  private CopyingFilteringReader filteringReader;
  private final List<ParquetFilterCondition> filterConditions;
  private boolean initialProjectorSetUpDone;
  private ScanOperator.ScanMutator filteringReaderInputMutator;

  public FilteringCoercionReader(OperatorContext context, List<SchemaPath> columns, RecordReader inner,
                                   BatchSchema targetSchema, List<ParquetFilterCondition> parqfilterConditions) {
    super(context, columns, inner, targetSchema);
    filterConditions = Optional.ofNullable(parqfilterConditions).orElse(Collections.emptyList());
    initialProjectorSetUpDone = false;
    resetReaderState();
  }

  /**
   * call will result in another call by this.filteringReader, the second call provides the filtering reader's input
   * container (to which projector has to write when filtering is to be done)
   *
   * @param output
   * @throws ExecutionSetupException
   */
  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    if (setupCalledByFilteringReader) {
      this.filteringReaderInputMutator = (ScanOperator.ScanMutator) output;
    } else {
      createCoercions();
      this.outputMutator = output;
      inner.setup(mutator); // this will modify filters in schema mismatch case
      incoming.buildSchema();
      // reset the schema change callback
      mutator.getAndResetSchemaChanged();

      for (Field field : targetSchema.getFields()) {
        ValueVector vector = outputMutator.getVector(field.getName());
        if (vector == null) {
          continue;
        }
        outgoing.add(vector);
      }
      outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);

      if (!filterConditions.isEmpty()) {

        // filter expressions on columns with schema mismatch
        final List<LogicalExpression> logicalExpressions = filterConditions.stream()
          .filter(fc -> fc.getFilter().exact() && fc.isModifiedForPushdown())
          .map(ParquetFilterCondition::getExpr)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());

        if (!logicalExpressions.isEmpty()) {
          this.needsFilteringAfterCoercion = true;
          this.filteringReader = new CopyingFilteringReader(this, context,
            logicalExpressions.size() == 1 ? logicalExpressions.get(0) :
              FunctionCallFactory.createBooleanOperator("and", logicalExpressions));
          setupCalledByFilteringReader = true;
          this.filteringReader.setup(output);
          setupCalledByFilteringReader = false;
        }
      }
    }
  }

  @Override
  public int next() {
    switch (nextMethodState) {
      case FIRST_CALL_BY_FILTERING_READER:
        // called by this.filteringReader. we just need to return number of records written by projector
        nextMethodState = NextMethodState.REPEATED_CALL_BY_FILTERING_READER;
        break;
      case NOT_CALLED_BY_FILTERING_READER:
        recordCount = inner.next();
        if (recordCount == 0) {
          return 0;
        }
        if (needsFilteringAfterCoercion) {
          projectorOutput = filteringReaderInputMutator.getContainer();
        }

        // we haven't set up projector in this.setup() since we didn't know the container for projector's output
        // this is needed as incoming mutator(this.mutator) will not detect a schema change if the schema differs from first batch
        if (!initialProjectorSetUpDone) {
          setupProjector(projectorOutput);
          initialProjectorSetUpDone = true;
        }

        if (mutator.getSchemaChanged()) {
          incoming.buildSchema();
          // reset the schema change callback
          mutator.getAndResetSchemaChanged();
          setupProjector(projectorOutput);
        }
        incoming.setAllCount(recordCount);

        if (DEBUG_PRINT) {
          debugPrint(projectorOutput);
        }
        runProjector(recordCount);
        projectorOutput.setAllCount(recordCount);

        if (needsFilteringAfterCoercion) {
          nextMethodState = NextMethodState.FIRST_CALL_BY_FILTERING_READER;
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
          setupProjector(projectorOutput);
        }
        incoming.setAllCount(recordCount);

        if (DEBUG_PRINT) {
          debugPrint(projectorOutput);
        }
        runProjector(recordCount);
    }
    return recordCount;
  }

  @Override
  protected void setupProjector(VectorContainer projectorOutput) {
    super.setupProjector(projectorOutput);
    // when individual fields of a struct column are projected, currently it results
    // in setting schema changed flag. Resetting the flag in iceberg flow, since
    // schema learning should not happen in iceberg flow
    outputMutator.getAndResetSchemaChanged();
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    // do not allocate if called by FilteringReader
    if (nextMethodState == NextMethodState.NOT_CALLED_BY_FILTERING_READER) {
      super.allocate(vectorMap);
    }
  }

  @Override
  public void close() throws Exception {
    if (!closeCalledByFilteringReader) {
      closeCalledByFilteringReader = true;
      AutoCloseables.close(super::close, filteringReader);
    }
    closeCalledByFilteringReader = false;
  }

  private void resetReaderState() {
    setupCalledByFilteringReader = false;
    closeCalledByFilteringReader = false;
    projectorOutput = outgoing;
    recordCount = 0;
    nextMethodState = NextMethodState.NOT_CALLED_BY_FILTERING_READER;
  }

  private enum NextMethodState {
    NOT_CALLED_BY_FILTERING_READER,
    FIRST_CALL_BY_FILTERING_READER,
    REPEATED_CALL_BY_FILTERING_READER
  }
}
