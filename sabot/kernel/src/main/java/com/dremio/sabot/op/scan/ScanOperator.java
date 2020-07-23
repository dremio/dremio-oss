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
package com.dremio.sabot.op.scan;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
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
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.GlobalDictionaries;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
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

/**
 * Record batch used for a particular scan. Operators against one or more
 */
public class ScanOperator implements ProducerOperator {
  private static final Logger logger = LoggerFactory.getLogger(ScanOperator.class);
  protected static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ScanOperator.class);

  /** Main collection of fields' value vectors. */
  protected final VectorContainer outgoing;

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
    EXTENDING_READ_BYTES, // Size of the extending chunks.
    TOTAL_BYTES_READ,     // Total bytes read
    LOCAL_BYTES_READ,     // Total number of local bytes read using local I/O
    SHORT_CIRCUIT_BYTES_READ, // Total number of bytes read using short circuit reads
    PRELOADED_BYTES,           // Number of bytes pre-loaded
    NUM_CACHE_HITS,       // Number of C3 hits
    NUM_CACHE_MISSES,     // Number of C3 misses
    AVG_PROCESSING_TIME,   // Average processing time of request by C3
    JAVA_BUILD_TIME,   // time taken by Java (setup+evaluation) for type conversions in CoercionReader
    JAVA_EXECUTE_TIME,
    GANDIVA_BUILD_TIME, // time taken by Gandiva (setup+evaluation) for type conversions in CoercionReader,
    GANDIVA_EXECUTE_TIME,
    NUM_FILTERS_MODIFIED,   // Number of parquet filters modified
    NUM_HIVE_PARQUET_TRUNCATE_VARCHAR, // Number of fixed-len varchar fields in hive_parquet
    TOTAL_HIVE_PARQUET_TRUNCATE_VARCHAR, // Total number of fixed-len varchar truncation in haveParquetCoercion
    TOTAL_HIVE_PARQUET_TRANSFER_VARCHAR, //  Total number of fixed-len varchar transfers in haveParquetCoercion
    HIVE_PARQUET_CHECK_VARCHAR_CAST_TIME, // Time spent checking if truncation is required for a varchar field
    NUM_ROW_GROUPS_PRUNED, // number of rowGroups pruned in ParquetVectorizedReader
    MAX_ROW_GROUPS_IN_HIVE_FILE_SPLITS, // max number of row groups across hive file splits
    NUM_HIVE_FILE_SPLITS_WITH_NO_ROWGROUPS, // Number of hive file splits with no rowgroups
    MIN_IO_READ_TIME,   // Minimum IO read time
    MAX_IO_READ_TIME,   // Maximum IO read time
    AVG_IO_READ_TIME,   // Average IO read time
    NUM_IO_READ,        // Total Number of IO reads
    NUM_HIVE_PARQUET_DECIMAL_COERCIONS, // Number of decimal coercions in hive parquet
    NUM_ROW_GROUPS_TRIMMED, // Number of row groups trimmed from footer in memory
    NUM_COLUMNS_TRIMMED,    // Number of columns trimmed from footer in memory
    ;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  /** Fields' value vectors indexed by fields' keys. */
  protected final Map<String, ValueVector> fieldVectorMap = Maps.newHashMap();
  protected State state = State.NEEDS_SETUP;
  protected final OperatorContext context;
  protected Iterator<RecordReader> readers;
  protected RecordReader currentReader;
  private final ScanMutator mutator;
  private MutatorSchemaChangeCallBack callBack = new MutatorSchemaChangeCallBack();
  private final BatchSchema schema;
  private final ImmutableList<SchemaPath> selectedColumns;
  private final List<String> tableSchemaPath;
  protected final SubScan config;
  private final GlobalDictionaries globalDictionaries;
  private final Stopwatch readTime = Stopwatch.createUnstarted();

  public ScanOperator(SubScan config, OperatorContext context, Iterator<RecordReader> readers) {
    this(config, context, readers, null);
  }

  public ScanOperator(SubScan config, OperatorContext context,
                      Iterator<RecordReader> readers, GlobalDictionaries globalDictionaries) {
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

  protected void setupReader(RecordReader reader) throws Exception {
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
    checkNotNull(reader).setup(mutator);
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

  protected void checkAndLearnSchema(){
    if (mutator.getSchemaChanged()) {
      outgoing.buildSchema(SelectionVectorMode.NONE);
      final BatchSchema newSchema = mutator.transformFunction.apply(outgoing.getSchema());
      if (config.mayLearnSchema() && tableSchemaPath != null) {
        throw UserException.schemaChangeError()
            .addContext("Original Schema", config.getFullSchema().toString())
            .addContext("New Schema", newSchema.toString())
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
    private final MutatorSchemaChangeCallBack callBack;
    private Function<BatchSchema, BatchSchema> transformFunction = Functions.identity();

    public ScanMutator(VectorContainer outgoing, Map<String, ValueVector> fieldVectorMap, OperatorContext context,
                       MutatorSchemaChangeCallBack callBack) {
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
      if (v == null || !clazz.isAssignableFrom(v.getClass()) || checkIfDecimalsTypesAreDifferent(v, field)) {
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

    public VectorContainer getContainer() {
      return outgoing;
    }

    private <T extends ValueVector> boolean checkIfDecimalsTypesAreDifferent(ValueVector v, Field field) {
      if (field.getType().getTypeID() != ArrowType.ArrowTypeID.Decimal) {
        return false;
      }
      return !v.getField().getType().equals(field.getType());
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
    public boolean getAndResetSchemaChanged() {
      boolean schemaChanged =  callBack.getSchemaChangedAndReset() || outgoing.isNewSchema();
      Preconditions.checkState(!callBack.getSchemaChanged(), "Unexpected state");
      return schemaChanged;
    }

    @Override
    public boolean getSchemaChanged() {
      return outgoing.isNewSchema() || callBack.getSchemaChanged();
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(outgoing, currentReader, globalDictionaries, readers instanceof AutoCloseable ? (AutoCloseable) readers : null);
    OperatorStats operatorStats = context.getStats();
    OperatorStats.IOStats ioStats = operatorStats.getReadIOStats();

    if (ioStats != null) {
      long minIOReadTime = ioStats.minIOTime.longValue() <= ioStats.maxIOTime.longValue() ? ioStats.minIOTime.longValue() : 0;
      operatorStats.setLongStat(Metric.MIN_IO_READ_TIME, minIOReadTime);
      operatorStats.setLongStat(Metric.MAX_IO_READ_TIME, ioStats.maxIOTime.longValue());
      operatorStats.setLongStat(Metric.AVG_IO_READ_TIME, ioStats.numIO.get() == 0 ? 0 : ioStats.totalIOTime.longValue() / ioStats.numIO.get());
      operatorStats.addLongStat(Metric.NUM_IO_READ, ioStats.numIO.longValue());

      operatorStats.setProfileDetails(UserBitShared.OperatorProfileDetails
        .newBuilder()
        .addAllSlowIoInfos(ioStats.slowIOInfoList)
        .build());
    }

    operatorStats.setLongStat(Metric.JAVA_BUILD_TIME, TimeUnit.NANOSECONDS.toMillis(operatorStats.getLongStat(Metric.JAVA_BUILD_TIME)));
    operatorStats.setLongStat(Metric.JAVA_EXECUTE_TIME, TimeUnit.NANOSECONDS.toMillis(operatorStats.getLongStat(Metric.JAVA_EXECUTE_TIME)));
    operatorStats.setLongStat(Metric.GANDIVA_BUILD_TIME, TimeUnit.NANOSECONDS.toMillis(operatorStats.getLongStat(Metric.GANDIVA_BUILD_TIME)));
    operatorStats.setLongStat(Metric.GANDIVA_EXECUTE_TIME, TimeUnit.NANOSECONDS.toMillis(operatorStats.getLongStat(Metric.GANDIVA_EXECUTE_TIME)));

  }

}
