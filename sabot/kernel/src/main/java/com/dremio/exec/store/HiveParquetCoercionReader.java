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
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import com.carrotsearch.hppc.IntHashSet;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CastExpressionWithOverflow;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.store.parquet.CopyingFilteringReader;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * FilteringCoercionReader for Hive-Parquet tables
 * TODO: Remove duplicate code with FilteringCoercionReader
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
  private final TypeCoercion typeCoercion;
  private boolean initialProjectorSetUpDone;
  private Stopwatch varcharCheckCastWatch = Stopwatch.createUnstarted();
  private BatchSchema originalSchema; // actual schema including varchar and non-varchar fields
  private Map<String, VarcharProjectorHelper> fixedLenVarCharMap = CaseInsensitiveMap.newHashMap();;

  /* Helper class to setup and run projector on each fixed-length varchar field */
  private class VarcharProjectorHelper implements AutoCloseable {
    int truncLen;
    Field field;
    NamedExpression castExpr;
    NamedExpression noCastExpr;
    ExpressionSplitter splitter; // used for truncating varchar
    TransferPair transferPair; // used for direct transfer

    public VarcharProjectorHelper(Field field) {
      this.field = field;
      TypeProtos.MajorType majorType = typeCoercion.getType(field);
      FieldReference inputRef = FieldReference.getWithQuotedRef(field.getName());
      this.truncLen = majorType.getWidth();
      this.castExpr = new NamedExpression(FunctionCallFactory.createCast(majorType, inputRef), inputRef);
      this.noCastExpr = new NamedExpression(inputRef, inputRef);
    }

    public void setupProjector() throws Exception {
      BatchSchema targetSchema = BatchSchema.newBuilder().addField(field).build();

      ExpressionSplitter varcharSplitter = ProjectOperator.createSplitterWithExpressions(incoming, Collections.singletonList(castExpr),
        null, null, null, context, projectorOptions, projectorOutput, targetSchema);
      varcharSplitter.setupProjector(projectorOutput, javaCodeGenWatch, gandivaCodeGenWatch);
      this.splitter = varcharSplitter;

      IntHashSet transferFieldIds = new IntHashSet();
      List<TransferPair> transfers = Lists.newArrayList();
      ProjectOperator.createSplitterWithExpressions(incoming, Collections.singletonList(noCastExpr),
        transfers, null, transferFieldIds, context, projectorOptions, projectorOutput, targetSchema);
      if (!transfers.isEmpty()) {
        this.transferPair = transfers.get(0);
      }
    }

    public void runProjector(BaseVariableWidthVector vector, int recordCount) throws Exception {
      if (transferPair == null) {
        return;
      }

      if (castRequired(vector, recordCount, truncLen)) {
        splitter.projectRecords(recordCount, javaCodeGenWatch, gandivaCodeGenWatch);
        context.getStats().addLongStat(ScanOperator.Metric.TOTAL_HIVE_PARQUET_TRUNCATE_VARCHAR, 1);
      } else {
        javaCodeGenWatch.start();
        transferPair.transfer();
        javaCodeGenWatch.stop();
        context.getStats().addLongStat(ScanOperator.Metric.TOTAL_HIVE_PARQUET_TRANSFER_VARCHAR, 1);
      }
      context.getStats().addLongStat(ScanOperator.Metric.HIVE_PARQUET_CHECK_VARCHAR_CAST_TIME,
        varcharCheckCastWatch.elapsed(TimeUnit.NANOSECONDS));
      varcharCheckCastWatch.reset();
    }

    private boolean castRequired(BaseVariableWidthVector vector, int recordCount, int truncLen) {
      varcharCheckCastWatch.start();
      if (vector.getNullCount() == recordCount) {
        varcharCheckCastWatch.stop();
        return false;
      }
      for (int i = 0; i < recordCount; ++i) {
        if (vector.getValueLength(i) > truncLen) {
          varcharCheckCastWatch.stop();
          return true;
        }
      }
      varcharCheckCastWatch.stop();
      return false;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(splitter);
    }
  }

  public static HiveParquetCoercionReader newInstance(
      OperatorContext context, List<SchemaPath> columns, RecordReader inner,
      BatchSchema originalSchema, TypeCoercion hiveTypeCoercion,
      List<ParquetFilterCondition> filterConditions) {
    // Creating a separate schema for columns that don't need varchar truncation
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (Field field : originalSchema.getFields()) {
      if (!isVarcharTruncationRequired(hiveTypeCoercion.getType(field))) {
        schemaBuilder.addField(field);
      }
    }
    BatchSchema targetNonVarcharSchema = schemaBuilder.build();

    HiveParquetCoercionReader hiveParquetCoercionReader = new HiveParquetCoercionReader(context, columns, inner,
      targetNonVarcharSchema, originalSchema, hiveTypeCoercion, filterConditions);
    return hiveParquetCoercionReader;
  }

  private static boolean isVarcharTruncationRequired(TypeProtos.MajorType majorType) {
    return (majorType.getMinorType().equals(TypeProtos.MinorType.VARCHAR) || majorType.getMinorType().equals(TypeProtos.MinorType.VARBINARY)) &&
      majorType.getWidth() < CompleteType.DEFAULT_VARCHAR_PRECISION;
  }

  private HiveParquetCoercionReader(OperatorContext context, List<SchemaPath> columns, RecordReader inner,
                                   BatchSchema nonVarcharSchema, BatchSchema originalSchema, TypeCoercion hiveTypeCoercion,
                                   List<ParquetFilterCondition> filterConditions) {
    super(context, columns, inner, nonVarcharSchema);
    this.originalSchema = originalSchema;
    this.typeCoercion = hiveTypeCoercion;
    for (Field field : originalSchema.getFields()) {
      if (isVarcharTruncationRequired(hiveTypeCoercion.getType(field))) {
        fixedLenVarCharMap.put(field.getName(), new VarcharProjectorHelper(field));
      }
    }
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

  @Override
  protected void addExpression(Field field, FieldReference inputRef) {
    TypeProtos.MajorType majorType = typeCoercion.getType(field);
    LogicalExpression cast;
    if (majorType.getMinorType().equals(TypeProtos.MinorType.VARCHAR) || majorType.getMinorType().equals(TypeProtos.MinorType.VARBINARY)){
      cast = inputRef;
    } else if (majorType.getMinorType().equals(TypeProtos.MinorType.DECIMAL)) {
      cast = new CastExpressionWithOverflow(inputRef, majorType);
    } else {
      cast = FunctionCallFactory.createCast(majorType, inputRef);
    }
    exprs.add(new NamedExpression(cast, inputRef));
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
      context.getStats().addLongStat(ScanOperator.Metric.NUM_HIVE_PARQUET_TRUNCATE_VARCHAR, fixedLenVarCharMap.size());

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

  @Override
  protected void setupProjector(VectorContainer projectorOutput) {
    super.setupProjector(projectorOutput);
    setUpAdditionalProjectors();
    outputMutator.getAndResetSchemaChanged();
  }

  private void setUpAdditionalProjectors() {
    try {
      // Setting up the projector for columns that need varchar truncation
      for (Map.Entry<String, VarcharProjectorHelper> entry : fixedLenVarCharMap.entrySet()) {
        entry.getValue().setupProjector();
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    OperatorStats stats = context.getStats();
    stats.addLongStat(ScanOperator.Metric.JAVA_BUILD_TIME, javaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    stats.addLongStat(ScanOperator.Metric.GANDIVA_BUILD_TIME, gandivaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    gandivaCodeGenWatch.reset();
    javaCodeGenWatch.reset();
  }

  @Override
  protected void runProjector(int recordCount) {
    super.runProjector(recordCount);
    runAdditionalProjectors(recordCount);
  }

  private void runAdditionalProjectors(int recordCount) {
    try {
      if (recordCount > 0) {
        if (!fixedLenVarCharMap.isEmpty()) {
          for (VectorWrapper<? extends ValueVector> wrapper : incoming) {
            if (fixedLenVarCharMap.containsKey(wrapper.getField().getName())) {
              VarcharProjectorHelper helper = fixedLenVarCharMap.get(wrapper.getField().getName());
              helper.runProjector((BaseVariableWidthVector) wrapper.getValueVector(), recordCount);
            }
          }
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    OperatorStats stats = context.getStats();
    stats.addLongStat(ScanOperator.Metric.JAVA_EXECUTE_TIME, javaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    stats.addLongStat(ScanOperator.Metric.GANDIVA_EXECUTE_TIME, gandivaCodeGenWatch.elapsed(TimeUnit.MILLISECONDS));
    javaCodeGenWatch.reset();
    gandivaCodeGenWatch.reset();
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
        AutoCloseables.close(fixedLenVarCharMap.values());
        fixedLenVarCharMap.clear();
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
