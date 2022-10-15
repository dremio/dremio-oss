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
package com.dremio.exec.store.parquet;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.FilteringFileCoercionReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.TypeCoercion;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * ParquetCoercionReader for Parquet files
 * TODO(DX-26038): Remove duplicate code with FilteringCoercionReader
 */
public class ParquetCoercionReader extends FilteringFileCoercionReader {
  private static final Logger logger = LoggerFactory.getLogger(ParquetCoercionReader.class);

  private final ParquetFilters filters;
  private OutputMutator outgoingMutator;
  private boolean setupCalledByFilteringReader; // setUp() state
  private boolean closeCalledByFilteringReader; // close() state
  private boolean needsFilteringAfterCoercion; // true if a pushdown filter is modified
  private CopyingFilteringReader filteringReader;
  private OutputMutator filteringReaderInputMutator;

  protected ParquetCoercionReader(OperatorContext context, List<SchemaPath> columns, RecordReader inner,
                                  BatchSchema originalSchema, TypeCoercion typeCoercion,
                                  ParquetFilters filters) {
    super(context, columns, inner, originalSchema, typeCoercion);
    this.filters = filters;
    resetReaderState();
  }

  public static ParquetCoercionReader newInstance(OperatorContext context, List<SchemaPath> columns,
                                                  RecordReader inner, BatchSchema originalSchema,
                                                  TypeCoercion typeCoercion, ParquetFilters filters) {
    return new ParquetCoercionReader(context, columns, inner, originalSchema, typeCoercion, filters);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    if (setupCalledByFilteringReader) {
      this.filteringReaderInputMutator = output;
    } else {
      if (inner instanceof UpPromotingParquetReader) {
        ((UpPromotingParquetReader) inner).setupMutator(output);
        if (output.getSchemaChanged()) {
          logger.info("Detected schema change. Not initializing further readers");
          return;
        }
      }
      this.outputMutator = output;
      inner.setup(mutator); // this will modify filters in schema mismatch case
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

      if (filters.hasPushdownFilters()) {

        // filter expressions on columns with schema mismatch
        final List<LogicalExpression> logicalExpressions = filters.getPushdownFilters().stream()
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
        outgoingMutator = this.outputMutator;
        recordCount = inner.next();
        if (recordCount == 0) {
          return 0;
        }
        if (needsFilteringAfterCoercion) {
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
      closeCalledByFilteringReader = true;
      AutoCloseables.close(super::close, filteringReader);
    }
    closeCalledByFilteringReader = false;
  }

  protected void resetReaderState() {
    setupCalledByFilteringReader = false;
    closeCalledByFilteringReader = false;
    projectorOutput = outgoing;
    recordCount = 0;
    nextMethodState = NextMethodState.NOT_CALLED_BY_FILTERING_READER;
  }
}
