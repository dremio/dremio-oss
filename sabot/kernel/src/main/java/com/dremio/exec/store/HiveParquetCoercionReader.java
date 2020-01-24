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

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.parquet.CopyingFilteringReader;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Preconditions;

/**
 * Similar to CoercionReader, additionally applies filter after coercion if the filter was modified by inner reader
 * for pushdown
 */
public class HiveParquetCoercionReader extends CoercionReader {

  private static final boolean DEBUG_PRINT = false;
  private final CopyingFilteringReader filteringReader;
  private final ParquetFilterCondition filterCondition;
  private final boolean filterConditionPresent; // true if filter condition is specified
  private ScanOperator.ScanMutator filteringReaderInputMutator;

  private NextMethodState nextMethodState;
  private boolean setupCalledByFilteringReader; // setUp() state
  private boolean closeCalledByFilteringReader; // close() state
  private VectorContainer projectorOutput; // can be this.outgoing or this.filteringReaderInputMutator.container
  private int recordCount;
  private boolean initialProjectorSetUpDone;

  public HiveParquetCoercionReader(OperatorContext context, List<SchemaPath> columns, RecordReader inner,
                                   BatchSchema targetSchema, EnumSet<Options> options,
                                   TypeCoercion hiveTypeCoercion, List<ParquetFilterCondition> filterConditions) {
    super(context, columns, inner, targetSchema, options, hiveTypeCoercion);

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
      this.outputMutator = output;
      inner.setup(mutator);
      incoming.buildSchema();
      // reset the schema change callback
      mutator.isSchemaChanged();

      for (Field field : targetSchema.getFields()) {
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
        if (filterConditionPresent && filterCondition.isModifiedForPushdown()) {
          projectorOutput = filteringReaderInputMutator.getContainer();
        }

        // we haven't set up projector in this.setup() since we didn't know the container for projector's output
        // this is needed as incoming mutator(this.mutator) will not detect a schema change if the schema differs from first batch
        if (!initialProjectorSetUpDone) {
          setupProjector(projectorOutput);
          initialProjectorSetUpDone = true;
        }

        if (mutator.isSchemaChanged()) {
          incoming.buildSchema();
          // reset the schema change callback
          mutator.isSchemaChanged();
          setupProjector(projectorOutput);
        }
        incoming.setAllCount(recordCount);

        if (DEBUG_PRINT) {
          debugPrint(projectorOutput);
        }
        runProjector(recordCount);

        if (filterConditionPresent && filterCondition.isModifiedForPushdown()) {
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

        if (mutator.isSchemaChanged()) {
          incoming.buildSchema();
          // reset the schema change callback
          mutator.isSchemaChanged();
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
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    // do not allocate if called by FilteringReader
    if (nextMethodState == NextMethodState.NOT_CALLED_BY_FILTERING_READER) {
      super.allocate(vectorMap);
    }
  }

  @Override
  public void close() throws Exception {
    if (!closeCalledByFilteringReader) {
      try {
        super.close();
      } finally {
        if (filterConditionPresent) {
          closeCalledByFilteringReader = true;
          AutoCloseables.close(filteringReader);
        }
      }
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
