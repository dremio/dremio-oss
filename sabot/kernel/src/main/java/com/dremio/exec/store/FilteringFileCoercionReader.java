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

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Stopwatch;

/**
 * FilteringFileCoercionReader
 */
public abstract class FilteringFileCoercionReader extends AbstractRecordReader {
  protected final RecordReader inner;
  protected final OperatorContext context;
  protected final BatchSchema originalSchema;
  private final CompositeReader compositeReader;
  private final ExpressionEvaluationOptions projectorOptions;

  protected int recordCount;
  protected SampleMutator mutator;
  protected VectorContainer incoming;
  protected VectorContainer outgoing;
  protected OutputMutator outputMutator;
  protected VectorContainer projectorOutput;
  protected boolean initialProjectorSetUpDone;
  protected NextMethodState nextMethodState = NextMethodState.NOT_CALLED_BY_FILTERING_READER;

  protected FilteringFileCoercionReader(OperatorContext context, List<SchemaPath> columns, RecordReader inner,
                                        BatchSchema originalSchema, TypeCoercion typeCoercion) {
    super(context, columns);
    this.mutator = new SampleMutator(context.getAllocator());
    this.incoming = mutator.getContainer();
    this.outgoing = new VectorContainer(context.getAllocator());
    this.inner = inner;
    this.context = context;
    this.projectorOptions = new ExpressionEvaluationOptions(context.getOptions());
    this.projectorOptions.setCodeGenOption(context.getOptions().getOption(ExecConstants.QUERY_EXEC_OPTION.getOptionName()).getStringVal());
    this.originalSchema = originalSchema;
    this.compositeReader = new CompositeReader(mutator, context, typeCoercion, Stopwatch.createUnstarted(), Stopwatch.createUnstarted(), originalSchema);
    this.projectorOutput = outgoing;
  }

  @Override
  public List<SchemaPath> getColumnsToBoost() {
    return inner.getColumnsToBoost();
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    inner.addRuntimeFilter(runtimeFilter);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    // Do not allocate if called by FilteringReader when the status is 'FIRST_CALL_BY_FILTERING_READER'.
    // DX-40891, if the FilteringReader excludes all rows for a batch and needs to call 'inner' reader
    // to read another batch to process, it needs to allocate more memory for field vectors in 'mutator'.
    if (nextMethodState == NextMethodState.NOT_CALLED_BY_FILTERING_READER ||
      nextMethodState == NextMethodState.REPEATED_CALL_BY_FILTERING_READER) {
      super.allocate(vectorMap);
      inner.allocate(mutator.getFieldVectorMap());
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(compositeReader, outgoing, inner);
  }

  protected void setupProjector(OutputMutator outgoing, VectorContainer projectorOutput) {
    compositeReader.setupProjector(outgoing, incoming, projectorOptions, projectorOutput);
    outgoing.getAndResetSchemaChanged();
  }

  protected void runProjector(int recordCount) {
    compositeReader.runProjector(recordCount, incoming);
  }

  protected void resetProjector() {
    compositeReader.resetProjector();
  }

  protected enum NextMethodState {
    NOT_CALLED_BY_FILTERING_READER,
    FIRST_CALL_BY_FILTERING_READER,
    REPEATED_CALL_BY_FILTERING_READER
  }
}
