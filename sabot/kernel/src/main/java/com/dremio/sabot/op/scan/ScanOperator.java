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
package com.dremio.sabot.op.scan;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.FileNotFoundException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.exception.SchemaChangeExceptionContext;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.GlobalDictionaries;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.values.EmptyValuesCreator.EmptyRecordReader;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import io.netty.buffer.ArrowBuf;

/**
 * Record batch used for a particular scan. Operators against one or more
 */
public class ScanOperator implements ProducerOperator {
  private static final Logger logger = LoggerFactory.getLogger(ScanOperator.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ScanOperator.class);

  /** Main collection of fields' value vectors. */
  private final VectorContainer outgoing;

  public enum Metric implements MetricDef {
    @Deprecated
    SETUP_NS, // @deprecated use stats time in OperatorStats instead.
    NUM_READERS, // tracks how many readers were opened so far
    NUM_REMOTE_READERS, // tracks how many readers are non-local
    NUM_ROW_GROUPS, // number of rowGroups for the FileSplitParquetRecordReader
    NUM_VECTORIZED_COLUMNS,
    NUM_NON_VECTORIZED_COLUMNS,
    COPY_MS,
    FILTER_MS,
    PARQUET_EXEC_PATH, // type of readers (vectorized, non-vectorized or combination used) in parquet
    FILTER_EXISTS, // Is there a filter pushed into scan?
    PARQUET_BYTES_READ, // Represents total number of actual bytes (uncompressed) read while parquet scan.
    NUM_ASYNC_STREAMS,  // Represents number of async streams created
    NUM_ASYNC_READS,    // Total number of async read requests made
    NUM_ASYNC_BYTES_READ, // Total number of bytes read
    NUM_EXTENDING_READS, // Total number of extending chunks
    EXTENDING_READ_BYTES // Size of the extending chunks.
    ;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  /** Fields' value vectors indexed by fields' keys. */
  private final Map<String, ValueVector> fieldVectorMap = Maps.newHashMap();
  private State state = State.NEEDS_SETUP;
  private final OperatorContext context;
  private Iterator<RecordReader> readers;
  private RecordReader currentReader;
  private final ScanMutator mutator;
  private SchemaChangeCallBack callBack = new SchemaChangeCallBack();
  private final BatchSchema schema;
  private final ImmutableList<SchemaPath> selectedColumns;
  private final UserGroupInformation readerUGI;
  private final List<String> tableSchemaPath;
  private final SubScan config;
  private final GlobalDictionaries globalDictionaries;
  private final Stopwatch readTime = Stopwatch.createUnstarted();

  public ScanOperator(SubScan config, OperatorContext context, Iterator<RecordReader> readers) {
    this(config, context, readers, null, ImpersonationUtil.getProcessUserUGI());
  }

  public ScanOperator(SubScan config, OperatorContext context, Iterator<RecordReader> readers, UserGroupInformation readerUGI) {
    this(config, context, readers, null, readerUGI);
  }

  public ScanOperator(SubScan config, OperatorContext context,
                      Iterator<RecordReader> readers, GlobalDictionaries globalDictionaries, UserGroupInformation readerUGI) {
    if (!readers.hasNext()) {
      this.readers = ImmutableList.<RecordReader>of(new EmptyRecordReader(context)).iterator();
    } else {
      this.readers = readers;
    }
    this.context = context;
    this.config = config;
    this.schema = config.getFullSchema();
    // Arbitrarily take the first element of the referenced tables list.
    // Currently, the only way to have multiple entries in the referenced table list is to write a
    // Join across multiple JDBC tables. This scenario doesn't use checkAndLearnSchema(). If a schema
    // change happens there, it gets corrected in the Foreman (when an InvalidMetadataError is thrown).
    this.tableSchemaPath = Iterables.getFirst(config.getReferencedTables(), null);
    this.selectedColumns = config.getColumns() == null ? null : ImmutableList.copyOf(config.getColumns());

    this.readerUGI = Preconditions.checkNotNull(readerUGI, "The reader UserGroupInformation is missing.");

    final OperatorStats stats = context.getStats();
    try {
      // be in the processing state as the fetching the next reader could trigger wait which expects the current state
      // to be processing.
      stats.startProcessing();
      this.currentReader = this.readers.next();
    } finally {
      stats.stopProcessing();
    }

    this.outgoing = context.createOutputVectorContainer();

    this.globalDictionaries = globalDictionaries;

    this.mutator = new ScanMutator(outgoing, fieldVectorMap, context, callBack);

    context.getStats().addLongStat(Metric.NUM_READERS, 1);
  }

  @Override
  public VectorAccessible setup() throws Exception {
    schema.maskAndReorder(config.getColumns()).materializeVectors(selectedColumns, mutator);
    outgoing.buildSchema(SelectionVectorMode.NONE);
    callBack.getSchemaChangedAndReset();
    setupReader(currentReader);

    state = State.CAN_PRODUCE;
    return outgoing;
  }

  @Override
  public State getState() {
    return state;
  }

  private void setupReader(RecordReader reader) throws Exception {
    try(RollbackCloseable commit = AutoCloseables.rollbackable(reader)){
      BatchSchema initialSchema = outgoing.getSchema();
      setupReaderAsCorrectUser(reader);
      checkAndLearnSchema();
      Preconditions.checkArgument(initialSchema.equals(outgoing.getSchema()), "Schema changed but not detected.");
      commit.commit();
    } catch (Exception e) {
      if (ErrorHelper.findWrappedCause(e, FileNotFoundException.class) != null) {
        if (e instanceof UserException) {
          throw UserException.invalidMetadataError(e.getCause())
              .addContext(((UserException)e).getOriginalMessage())
              .setAdditionalExceptionContext(
                  new InvalidMetadataErrorContext(
                      ImmutableList.copyOf(config.getReferencedTables())))
              .build(logger);
        } else {
          throw UserException.invalidMetadataError(e)
            .setAdditionalExceptionContext(
              new InvalidMetadataErrorContext(
                ImmutableList.copyOf(config.getReferencedTables())))
            .build(logger);
        }
      } else {
        throw e;
      }
    }
  }

  private void setupReaderAsCorrectUser(final RecordReader reader) throws Exception {
    readerUGI.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run()
          throws Exception {
        checkNotNull(reader).setup(mutator);
        return null;
      }
    });
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);

    injector.injectChecked(context.getExecutionControls(), "next-allocate", OutOfMemoryException.class);

    currentReader.allocate(fieldVectorMap);

    int recordCount;

    // get the next reader.
    readTime.start();

    final OperatorStats stats = context.getStats();
    while ((recordCount = currentReader.next()) == 0) {

      readTime.stop();
      readTime.reset();
      readTime.start();
      if (!readers.hasNext()) {
        // We're on the last reader, and it has no (more) rows.
        // no need to close the reader (will be done when closing the operator)
        // but we might as well release any memory that we're holding.
        outgoing.zeroVectors();
        state = State.DONE;
        outgoing.setRecordCount(0);
        stats.batchReceived(0, 0, 0);
        return 0;
      }

      // There are more readers, let's close the previous one and get the next one.
      currentReader.close();
      currentReader = readers.next();
      try {
        stats.startSetup();
        setupReader(currentReader);
      } finally {
        stats.stopSetup();
      }

      currentReader.allocate(fieldVectorMap);
      stats.addLongStat(Metric.NUM_READERS, 1);
    }

    readTime.stop();

    stats.batchReceived(0, recordCount, VectorUtil.getSize(outgoing));

    checkAndLearnSchema();
    return outgoing.setAllCount(recordCount);
  }

  private void checkAndLearnSchema(){
    if (mutator.isSchemaChanged()) {
      outgoing.buildSchema(SelectionVectorMode.NONE);
      final BatchSchema newSchema = mutator.transformFunction.apply(outgoing.getSchema());
      if (config.mayLearnSchema() && tableSchemaPath != null) {
        throw UserException.schemaChangeError()
            .message("New schema found. Please reattempt the query. Multiple attempts may be necessary to fully learn the schema.")
            .setAdditionalExceptionContext(new SchemaChangeExceptionContext(tableSchemaPath, newSchema))
            .build(logger);
      } else {
        // TODO: change error if we can't update. No reason to re-run if we didn't update.
        throw UserException.schemaChangeError().message("Schema change detected but unable to learn schema, query failed. Original: %s, New: %s.", config.getFullSchema(), newSchema).build(logger);
      }
    }
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitProducer(this, value);
  }

  public static class ScanMutator implements OutputMutator {
    private final VectorContainer outgoing;
    private final Map<String, ValueVector> fieldVectorMap;
    private final OperatorContext context;
    private final SchemaChangeCallBack callBack;
    private Function<BatchSchema, BatchSchema> transformFunction = Functions.identity();

    public ScanMutator(VectorContainer outgoing, Map<String, ValueVector> fieldVectorMap, OperatorContext context,
                       SchemaChangeCallBack callBack) {
      this.outgoing = outgoing;
      this.fieldVectorMap = fieldVectorMap;
      this.context = context;
      this.callBack = callBack;
    }

    @Override
    public <T extends ValueVector> T addField(Field field,
                                              Class<T> clazz) throws SchemaChangeException {
      // Check if the field exists.
      final ValueVector v = fieldVectorMap.get(field.getName().toLowerCase());
      if (v == null || !clazz.isAssignableFrom(v.getClass())) {
        // Field does not exist--add it to the map and the output container.
        ValueVector newVector = TypeHelper.getNewVector(field, context.getAllocator(), callBack);
        if (!clazz.isAssignableFrom(newVector.getClass())) {
          throw new SchemaChangeException(
              String.format(
                  "The class that was provided, %s, does not correspond to the "
                  + "expected vector type of %s.",
                  clazz.getSimpleName(), newVector.getClass().getSimpleName()));
        }

        final ValueVector old = fieldVectorMap.put(field.getName().toLowerCase(), newVector);
        if (old != null) {
          old.clear();
          outgoing.remove(old);
        }

        outgoing.add(newVector);
        return clazz.cast(newVector);
      }

      return clazz.cast(v);
    }

    @Override
    public ValueVector getVector(String name) {
      return fieldVectorMap.get((name != null) ? name.toLowerCase() : name);
    }

    @Override
    public Collection<ValueVector> getVectors() {
      return fieldVectorMap.values();
    }

    @Override
    public void allocate(int recordCount) {
      for (final ValueVector v : fieldVectorMap.values()) {
        AllocationHelper.allocate(v, recordCount, 50, 10);
      }
    }

    @Override
    public ArrowBuf getManagedBuffer() {
      return context.getManagedBuffer();
    }

    @Override
    public CallBack getCallBack() {
      return callBack;
    }

    @Override
    public boolean isSchemaChanged() {
      return outgoing.isNewSchema() || callBack.getSchemaChangedAndReset();
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(outgoing, currentReader, globalDictionaries);
  }

}
