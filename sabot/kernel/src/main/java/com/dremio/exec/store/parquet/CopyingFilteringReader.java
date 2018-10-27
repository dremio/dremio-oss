/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ReturnValueExpression;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.exec.store.RecordReader;
import com.dremio.sabot.driver.SchemaChangeMutator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.copier.Copier;
import com.dremio.sabot.op.copier.CopierOperator;
import com.dremio.sabot.op.filter.Filterer;
import com.dremio.sabot.op.filter.VectorContainerWithSV;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.scan.ScanOperator.ScanMutator;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

/**
 * Implementation of {@link RecordReader} that wraps another record reader and provider filter push down handling.
 */
public class CopyingFilteringReader implements RecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopyingFilteringReader.class);

  private final RecordReader delegate;
  private final OperatorContext context;
  private final LogicalExpression filterCondition;

  private final Map<String, ValueVector> fieldVectorMap = Maps.newHashMap();

  private final List<TransferPair> copierToOutputTransfers = new ArrayList<>();

  /*
   * Because we use an internal container, readerOutput, as output to the delegate reader, we need a separate, internal,
   * schema change callback to handle schema changes from the delegate reader.
   * We can't just use the "external" callback passed with the OutputMutator, because the caller, e.g. ScanOperator,
   * doesn't expect schema changes during setup.
   * We still need to keep a reference to the external callback so it can be notified whenever the schema changes on
   * the internal one.
   */
  private final SchemaChangeCallBack innerCallback = new SchemaChangeCallBack();
  private CallBack externalCallback;

  private VectorContainer readerOutput = new VectorContainer();
  private VectorContainer copyOutput;
  private ScanMutator mutator;
  private Filterer filter;
  private Copier copier;

  public CopyingFilteringReader(RecordReader delegate, OperatorContext context, LogicalExpression filterCondition) {
    this.delegate = delegate;
    this.context = context;
    this.filterCondition = filterCondition;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.externalCallback = output.getCallBack();

    // create an inner vector container, this will be populated by the delegate reader
    readerOutput = new VectorContainerWithSV(context.getAllocator(), new SelectionVector2(context.getAllocator()));
    mutator = new ScanMutator(readerOutput, fieldVectorMap, context, innerCallback);

    // ScanOperator already materialized the schema in the output mutator
    // copy the schema to the inner mutator
    for (ValueVector v : output.getVectors()) {
      final Field f = v.getField();
      mutator.addField(f, (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(f));
    }
    // we just added a bunch of fields, we don't need to report this as a schema change
    innerCallback.getSchemaChangedAndReset();
    readerOutput.buildSchema();

    // setup the delegate reader with our own mutator
    delegate.setup(mutator);

    // generate a filterer using the passed filterCondition
    final ClassGenerator<Filterer> cg = context.getClassProducer().createGenerator(Filterer.TEMPLATE_DEFINITION2).getRoot();

    final LogicalExpression expr = context.getClassProducer().materializeAndAllowComplex(filterCondition, readerOutput);
    cg.addExpr(new ReturnValueExpression(expr), ClassGenerator.BlockCreateMode.MERGE);

    // we only need the filterer to set the selection vector of scanOutput, that's why we use a SV2Holder instead of a VectorContainer
    final SelectionVector2 filteredSV2 = new SelectionVector2(context.getAllocator());
    final SV2Holder sv2Holder = new SV2Holder(filteredSV2);
    this.filter = cg.getCodeGenerator().getImplementationClass();
    filter.setup(context.getClassProducer().getFunctionContext(), readerOutput, sv2Holder);

    // generate a copier that takes as input the reader output (along with the filtered SV2) and copies the data to the copyOutput
    final VectorAccessible copyInput = new ContainerAndSV2(readerOutput, filteredSV2);
    copyOutput = VectorContainer.create(context.getAllocator(), readerOutput.getSchema());
    copyOutput.setInitialCapacity(context.getTargetBatchSize());
    copier = CopierOperator.getGenerated2Copier(context.getClassProducer(), copyInput, copyOutput);

    // prepare the transfer pairs that will be used to transfer buffers from the copy container to the output mutator
    for (VectorWrapper<?> wrapper : copyOutput) {
      final Field field = wrapper.getField();
      copierToOutputTransfers.add(wrapper.getValueVector().makeTransferPair(output.getVector(field.getName())));
    }
  }

  @Override
  public SchemaChangeMutator getSchemaChangeMutator() {
    return delegate.getSchemaChangeMutator();
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    delegate.allocate(vectorMap);
  }


  @Override
  public int next() {
    int recordCount;
    final Stopwatch copyWatch = Stopwatch.createUnstarted();
    final Stopwatch filterWatch = Stopwatch.createUnstarted();

    delegate.allocate(fieldVectorMap);

    // keep reading until the delegate reader is done or the filter doesn't filter everything
    while ((recordCount = delegate.next()) > 0) {
      if (mutator.isSchemaChanged()) {
        // report the schema change to the caller but keep reading from the reader
        // This is similar to the behavior of ScanOperator.outputData()
        externalCallback.doWork();
      }

      filterWatch.start();
      recordCount = filter.filterBatch(recordCount);
      filterWatch.stop();
      if (recordCount > 0) {
        break;
      }

      // filter excluded all rows, we need to call the delegate reader again
      readerOutput.allocateNew();
    }

    copyOutput.allocateNew();

    copyWatch.start();
    int copied = copier.copyRecords(0, recordCount);
    copyWatch.stop();
    if (copied != recordCount) { // copier may return earlier if it runs out of memory
      throw UserException.memoryError().message("Ran out of memory while trying to copy the records.").build(logger);
    }

    for (TransferPair t : copierToOutputTransfers) {
      t.transfer();
    }

    context.getStats().addLongStat(ScanOperator.Metric.COPY_MS, copyWatch.elapsed(TimeUnit.MILLISECONDS));
    context.getStats().addLongStat(ScanOperator.Metric.FILTER_MS, filterWatch.elapsed(TimeUnit.MILLISECONDS));
    return recordCount;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(copier, copyOutput, readerOutput, delegate);
  }

  private static class ContainerAndSV2 implements VectorAccessible {
    private final VectorAccessible inner;
    private final SelectionVector2 sv2;
    private final BatchSchema schema;

    ContainerAndSV2(VectorAccessible inner, SelectionVector2 sv2) {
      this.inner = inner;
      this.sv2 = sv2;
      schema = inner.getSchema().clone(BatchSchema.SelectionVectorMode.TWO_BYTE);
    }

    @Override
    public <T extends ValueVector> VectorWrapper<T> getValueAccessorById(Class<T> clazz, int... fieldIds) {
      return inner.getValueAccessorById(clazz, fieldIds);
    }

    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
      return inner.getValueVectorId(path);
    }

    @Override
    public BatchSchema getSchema() {
      return schema;
    }

    @Override
    public int getRecordCount() {
      return sv2.getCount();
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      return sv2;
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<VectorWrapper<?>> iterator() {
      return inner.iterator();
    }
  }

  private static class SV2Holder implements VectorAccessible {

    private final SelectionVector2 sv2;

    SV2Holder(SelectionVector2 sv2) {
      this.sv2 = sv2;
    }

    @Override
    public Iterator<VectorWrapper<?>> iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends ValueVector> VectorWrapper<T> getValueAccessorById(Class<T> clazz, int... fieldIds) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BatchSchema getSchema() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getRecordCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      return sv2;
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      throw new UnsupportedOperationException();
    }
  }
}
