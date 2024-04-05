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
package com.dremio.exec.planner.logical.partition;

import static com.dremio.common.expression.CompleteType.fromMajorType;
import static com.dremio.proto.model.PartitionStats.PartitionStatsValue;
import static org.apache.calcite.sql.SqlKind.AND;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;

import com.dremio.common.AutoCloseables;
import com.dremio.common.VM;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.partition.FindSimpleFilters.StateHolder;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.FieldIdBroker;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.extra.exec.store.dfs.parquet.pushdownfilter.FilterExtractor;
import com.dremio.service.Pointer;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsEntry;
import org.apache.iceberg.PartitionStatsReader;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;

/** Implementation of {@link RecordPruner} which prunes based on Iceberg partition stats */
public class PartitionStatsBasedPruner extends RecordPruner {
  private static final List<SqlKind> SARG_PRUNEABLE_OPERATORS =
      Arrays.asList(
          EQUALS, AND, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL);

  private final PartitionSpec partitionSpec;
  private final CloseableIterator<PartitionStatsEntry> statsEntryIterator;
  private Map<Integer, Integer>
      partitionColIdToSpecColIdMap; // partition col ID to col ID in Iceberg partition spec
  private Map<String, Integer>
      partitionColNameToSpecColIdMap; // col name to col ID in Iceberg partition spec
  private Map<String, MajorType>
      partitionColNameToPartitionFunctionOutputType; // col name to output type of the partition
  // function
  private String fileLocation;
  private final ScanRelBase scan;

  public Map<String, MajorType> getPartitionColNameToPartitionFunctionOutputType() {
    return partitionColNameToPartitionFunctionOutputType;
  }

  public PartitionSpec getPartitionSpec() {
    return partitionSpec;
  }

  public PartitionStatsBasedPruner(
      String fileLocation,
      PartitionStatsReader partitionStatsReader,
      OptimizerRulesContext rulesContext,
      PartitionSpec partitionSpec,
      ScanRelBase scan) {
    super(rulesContext);
    this.statsEntryIterator = partitionStatsReader.iterator();
    this.partitionSpec = partitionSpec;
    this.fileLocation = fileLocation;
    this.scan = scan;
  }

  @WithSpan("partitions-stats-prune")
  @Override
  public PartitionStatsValue prune(
      Map<Integer, String> inUseColIdToNameMap,
      Map<String, Integer> partitionColToIdMap,
      Function<RexNode, List<Integer>> usedIndexes,
      List<SchemaPath> projectedColumns,
      TableMetadata tableMetadata,
      RexNode pruneCondition,
      BatchSchema batchSchema,
      RelDataType rowType,
      RelOptCluster cluster) {

    setup(inUseColIdToNameMap, batchSchema);
    populateMaps(inUseColIdToNameMap);

    FindSimpleFilters rexVisitor =
        new FindSimpleFilters(cluster.getRexBuilder(), true, false, SARG_PRUNEABLE_OPERATORS);
    ImmutableList<RexCall> rexConditions = ImmutableList.of();
    StateHolder holder = pruneCondition.accept(rexVisitor);
    if (!hasDecimalCols(holder)) {
      rexConditions = holder.getConditions();
    }

    RexNode remainingExpressionWithPartitionTransformsRemoved =
        holder.hasRemainingExpression()
            ? removeFilterOnNonTransformationPartitionedColumns(cluster, holder.getNode())
            : null;

    long qualifiedRecordCount = 0, qualifiedFileCount = 0;
    SargPrunableEvaluator sargPrunableEvaluator =
        SargPrunableEvaluator.newInstance(
            this, usedIndexes, projectedColumns, rexVisitor, rexConditions, rowType, cluster);
    if (canPruneWhollyWithSarg(holder, sargPrunableEvaluator)
        || canPrunePartiallyWithSargAndNoEvalPruningPossible(
            sargPrunableEvaluator, remainingExpressionWithPartitionTransformsRemoved)) {
      Pair<Long, Long> counts = sargPrunableEvaluator.evaluateWithSarg(tableMetadata);
      qualifiedFileCount = counts.getRight();
      qualifiedRecordCount = counts.getLeft();
      updateCounters(true);
    } else {
      // eval pruning
      RexNode pruneConditionWithPartitionTransformsRemoved =
          removeFilterOnNonTransformationPartitionedColumns(cluster, pruneCondition);
      if (pruneConditionWithPartitionTransformsRemoved == null
          || pruneConditionWithPartitionTransformsRemoved.isAlwaysTrue()) {
        // it is possible that all the partition filters were on columns that have Iceberg Transform
        // defined on them
        // unfortunately, we cannot prune those with eval pruning
        // if all the filters were removed, we will consider all the rows qualified
        qualifiedRecordCount = tableMetadata.getApproximateRecordCount();
        qualifiedFileCount =
            tableMetadata
                .getDatasetConfig()
                .getReadDefinition()
                .getManifestScanStats()
                .getRecordCount();
        return PartitionStatsValue.newBuilder()
            .setRowcount(qualifiedRecordCount)
            .setFilecount(getNoOfSplitsAdjustedNumberOfFiles(qualifiedFileCount))
            .build();
      }
      int batchIndex = 0;
      long[] recordCounts = null;
      long[] fileCounts = null;
      LogicalExpression materializedExpr = null;
      boolean countOverflowFound = false;
      while (!countOverflowFound && statsEntryIterator.hasNext()) {
        List<PartitionStatsEntry> entriesInBatch =
            createRecordBatch(sargPrunableEvaluator, batchIndex);
        int batchSize = entriesInBatch.size();
        if (batchSize == 0) {
          logger.debug("batch: {} is empty as sarg pruned all entries", batchIndex);
          break;
        }

        if (batchIndex == 0) {
          setupVectors(inUseColIdToNameMap, partitionColToIdMap, batchSize);
          materializedExpr =
              materializePruneExpr(pruneConditionWithPartitionTransformsRemoved, rowType, cluster);
          recordCounts = new long[batchSize];
          fileCounts = new long[batchSize];
        }
        populateVectors(batchIndex, recordCounts, fileCounts, entriesInBatch);
        evaluateExpr(materializedExpr, batchSize);

        // Count the number of records in the splits that survived the expression evaluation
        for (int i = 0; i < batchSize; ++i) {
          if (!outputVector.isNull(i) && outputVector.get(i) == 1) {
            qualifiedRecordCount += recordCounts[i];
            qualifiedFileCount += fileCounts[i];

            if (recordCounts[i] < 0
                || qualifiedRecordCount < 0
                || fileCounts[i] < 0
                || qualifiedFileCount < 0) {
              qualifiedRecordCount = tableMetadata.getApproximateRecordCount();
              qualifiedFileCount =
                  tableMetadata
                      .getDatasetConfig()
                      .getReadDefinition()
                      .getManifestScanStats()
                      .getRecordCount();
              countOverflowFound = true;
              logger.info(
                  "Record count overflowed while evaluating partition filter. Partition stats file {}. Partition Expression {}",
                  this.fileLocation,
                  materializedExpr.toString());
              break;
            }
          }
        }

        logger.debug(
            "Within batch: {}, qualified records: {}, qualified files: {}",
            batchIndex,
            qualifiedRecordCount,
            qualifiedFileCount);
        batchIndex++;
      }

      if (countOverflowFound) {
        qualifiedRecordCount = tableMetadata.getApproximateRecordCount();
        qualifiedFileCount =
            tableMetadata
                .getDatasetConfig()
                .getReadDefinition()
                .getManifestScanStats()
                .getRecordCount();
      }
      if (sargPrunableEvaluator
          .hasConditions()) { // we apply SargPrunable conditions too for eval case in
        // createRecordBatch()
        updateCounters(true);
      }
      updateCounters(false);
    }
    qualifiedRecordCount =
        (qualifiedRecordCount < 0)
            ? tableMetadata.getApproximateRecordCount()
            : qualifiedRecordCount;
    qualifiedFileCount =
        (qualifiedFileCount < 0)
            ? tableMetadata
                .getDatasetConfig()
                .getReadDefinition()
                .getManifestScanStats()
                .getRecordCount()
            : qualifiedFileCount;

    return PartitionStatsValue.newBuilder()
        .setRowcount(qualifiedRecordCount)
        .setFilecount(getNoOfSplitsAdjustedNumberOfFiles(qualifiedFileCount))
        .build();
  }

  /**
   * Returns true if the partition stats can be partially pruned by sargPrunableEvaluator and there
   * are no conditions that can be pruned by Eval pruning
   *
   * @param sargPrunableEvaluator sarg prunable evaluator built for this filter
   * @param pruneConditionWithPartitionTransformsRemoved remaining filter for eval pruning
   * @return true if the partition stats can be partially pruned by sargPrunableEvaluator, and no
   *     eval pruning possible
   */
  private boolean canPrunePartiallyWithSargAndNoEvalPruningPossible(
      SargPrunableEvaluator sargPrunableEvaluator,
      RexNode pruneConditionWithPartitionTransformsRemoved) {
    return sargPrunableEvaluator.hasConditions()
        && (pruneConditionWithPartitionTransformsRemoved == null
            || pruneConditionWithPartitionTransformsRemoved.isAlwaysTrue());
  }

  /**
   * Returns the number of splits using number of files and
   * getPlannerSettings().getNoOfSplitsPerFile()
   *
   * @param qualifiedFileCount files count
   * @return the number of splits adjusted by getPlannerSettings().getNoOfSplitsPerFile()
   */
  private long getNoOfSplitsAdjustedNumberOfFiles(long qualifiedFileCount) {
    return qualifiedFileCount * optimizerContext.getPlannerSettings().getNoOfSplitsPerFile();
  }

  /**
   * Removes any filter on Partition transformed columns by making them (true/none) The remaining
   * filter is returned
   *
   * @param cluster RelOptCluster to use
   * @param pruneCondition condition to prune
   * @return the filter pruned to remove any filter on partition columns transformed by an Iceberg
   *     transform
   */
  private RexNode removeFilterOnNonTransformationPartitionedColumns(
      RelOptCluster cluster, RexNode pruneCondition) {
    if (pruneCondition == null) {
      return null;
    }
    RexBuilder rexBuilder = cluster.getRexBuilder();
    final ImmutableBitSet partitionColumnsIdentityTransformOnly =
        MoreRelOptUtil.buildColumnSet(
            scan,
            partitionSpec.fields().stream()
                .filter(partitionField -> partitionField.transform().isIdentity())
                .map(partitionField -> partitionField.name())
                .collect(Collectors.toList()));

    RexNode partitionFilter =
        FilterExtractor.extractFilter(
            rexBuilder,
            pruneCondition,
            rexNode -> {
              ImmutableBitSet inputs = RelOptUtil.InputFinder.bits(rexNode);
              return partitionColumnsIdentityTransformOnly.contains(inputs);
            });
    return partitionFilter;
  }

  @Override
  public void close() {
    AutoCloseables.close(RuntimeException.class, statsEntryIterator, super::close);
  }

  protected void updateCounters(boolean isSarge) {
    // Update test counters only when assertions are enabled, which is the case for test flow
    if (VM.areAssertsEnabled()) {
      if (isSarge) {
        TestCounters
            .incrementRecordCountEvaluatedWithSargCounter(); // expression was partly or wholly
        // evaluated with sarg
      } else {
        TestCounters.incrementRecordCountEvaluatedWithEvalCounter();
      }
    }
  }

  private List<PartitionStatsEntry> createRecordBatch(
      SargPrunableEvaluator sargPrunableEvaluator, int batchIndex) {
    timer.start();
    List<PartitionStatsEntry> entriesInBatch = new ArrayList<>();
    while (entriesInBatch.size() < PARTITION_BATCH_SIZE && statsEntryIterator.hasNext()) {
      PartitionStatsEntry statsEntry = statsEntryIterator.next();
      StructLike partitionData = statsEntry.getPartition();
      if (!sargPrunableEvaluator.hasConditions()
          || sargPrunableEvaluator.isRecordMatch(partitionData)) {
        entriesInBatch.add(statsEntry);
      }
    }
    logger.debug(
        "Elapsed time to get list of partition stats entries for the current batch: {}ms within batchIndex: {}",
        timer.elapsed(TimeUnit.MILLISECONDS),
        batchIndex);
    timer.reset();
    return entriesInBatch;
  }

  private void populateVectors(
      int batchIndex,
      long[] recordCounts,
      long[] fileCounts,
      List<PartitionStatsEntry> entriesInBatch) {
    timer.start();
    // Loop over partition stats and populate record count, vectors etc.
    for (int i = 0; i < entriesInBatch.size(); i++) {
      PartitionStatsEntry statsEntry = entriesInBatch.get(i);
      StructLike partitionData = statsEntry.getPartition();
      recordCounts[i] = statsEntry.getRecordCount();
      fileCounts[i] = statsEntry.getFileCount();
      writePartitionValues(i, partitionData);
    }
    logger.debug(
        "Elapsed time to populate partition column vectors: {}ms within batchIndex: {}",
        timer.elapsed(TimeUnit.MILLISECONDS),
        batchIndex);
    timer.reset();
  }

  private boolean canPruneWhollyWithSarg(
      StateHolder holder, SargPrunableEvaluator conditionsByColumn) {
    return !holder.hasRemainingExpression() && conditionsByColumn.hasConditions();
  }

  /**
   * Given an input type, calculate the output type after an iceberg transform is performed on it
   *
   * @param inputMajorType input type
   * @param partitionField partition field, containing a transform
   * @return the resulting output type
   */
  private MajorType getPartitionFunctionOutputType(
      final MajorType inputMajorType, final PartitionField partitionField) {
    if (partitionField.transform().isIdentity()) {
      // identity will not change the type, just return the inputMajorType
      return inputMajorType;
    }
    // convert to Iceberg type, ask the transform what would the be Iceberg return type of the
    // transform
    // then convert the result back to MajorType and return it
    final SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    final Type inputType =
        schemaConverter.toIcebergType(
            fromMajorType(inputMajorType), null, new FieldIdBroker.UnboundedFieldIdBroker());
    final Type outputType = partitionField.transform().getResultType(inputType);
    final MinorType minorType = schemaConverter.fromIcebergType(outputType).toMinorType();
    return MajorType.newBuilder().setMinorType(minorType).build();
  }

  protected boolean hasDecimalCols(StateHolder holder) {
    Pointer<Boolean> hasDecimalCols = new Pointer<>(false);
    holder
        .getConditions()
        .forEach(
            condition ->
                condition
                    .getOperands()
                    .forEach(
                        operand -> {
                          if (operand.getKind().equals(SqlKind.INPUT_REF)
                              && operand.getType().getSqlTypeName().equals(SqlTypeName.DECIMAL)) {
                            hasDecimalCols.value = true;
                          }
                        }));
    return hasDecimalCols.value;
  }

  private void populateMaps(Map<Integer, String> fieldNameMap) {
    Set<Integer> partitionColIds = partitionColIdToTypeMap.keySet();
    partitionColIdToSpecColIdMap = new HashMap<>(partitionColIds.size());
    partitionColNameToSpecColIdMap = new HashMap<>(partitionColIds.size());
    partitionColNameToPartitionFunctionOutputType = new HashMap<>(partitionColIds.size());
    for (Integer partitionColIndex : partitionColIds) {
      final String colName = fieldNameMap.get(partitionColIndex);
      List<PartitionField> fields = partitionSpec.fields();
      for (int i = 0; i < fields.size(); i++) {
        final String fieldColName =
            IcebergUtils.getColumnName(fields.get(i), partitionSpec.schema());
        if (fieldColName.equalsIgnoreCase(colName)) {
          partitionColIdToSpecColIdMap.put(partitionColIndex, i);
          partitionColNameToSpecColIdMap.put(colName, partitionColIndex);
          partitionColNameToPartitionFunctionOutputType.put(
              colName,
              getPartitionFunctionOutputType(
                  partitionColNameToTypeMap.get(colName), fields.get(i)));
          break;
        }
      }
    }
  }

  /** Sets all the vectors for a given entry in the batch */
  private void writePartitionValues(int row, StructLike partitionData) {
    for (int partitionColIndex : partitionColIdToTypeMap.keySet()) {
      int indexInPartitionSpec = partitionColIdToSpecColIdMap.get(partitionColIndex);
      MajorType majorType = partitionColIdToTypeMap.get(partitionColIndex);
      ValueVector vv = vectors[partitionColIndex];

      switch (majorType.getMinorType()) {
        case BIT:
          BitVector bitVector = (BitVector) vv;
          Boolean boolVal = partitionData.get(indexInPartitionSpec, Boolean.class);
          if (boolVal != null) {
            bitVector.set(row, boolVal.equals(Boolean.TRUE) ? 1 : 0);
          }
          break;
        case INT:
          IntVector intVector = (IntVector) vv;
          Integer intVal = partitionData.get(indexInPartitionSpec, Integer.class);
          if (intVal != null) {
            intVector.set(row, intVal);
          }
          break;
        case BIGINT:
          BigIntVector bigIntVector = (BigIntVector) vv;
          Long longVal = partitionData.get(indexInPartitionSpec, Long.class);
          if (longVal != null) {
            bigIntVector.set(row, longVal);
          }
          break;
        case FLOAT4:
          Float4Vector float4Vector = (Float4Vector) vv;
          Float floatVal = partitionData.get(indexInPartitionSpec, Float.class);
          if (floatVal != null) {
            float4Vector.set(row, floatVal);
          }
          break;
        case FLOAT8:
          Float8Vector float8Vector = (Float8Vector) vv;
          Double doubleVal = partitionData.get(indexInPartitionSpec, Double.class);
          if (doubleVal != null) {
            float8Vector.set(row, doubleVal);
          }
          break;
        case VARBINARY:
          VarBinaryVector varBinaryVector = (VarBinaryVector) vv;
          ByteBuffer byteBuf = partitionData.get(indexInPartitionSpec, ByteBuffer.class);
          if (byteBuf != null) {
            byte[] bytes = byteBuf.array();
            varBinaryVector.setSafe(row, bytes, 0, bytes.length);
          }
          break;
        case FIXEDSIZEBINARY:
        case VARCHAR:
          VarCharVector varCharVector = (VarCharVector) vv;
          String stringVal = partitionData.get(indexInPartitionSpec, String.class);
          if (stringVal != null) {
            byte[] stringBytes = stringVal.getBytes();
            varCharVector.setSafe(row, stringBytes, 0, stringBytes.length);
          }
          break;
        case DATE:
          DateMilliVector dateVector = (DateMilliVector) vv;
          Integer dateVal = partitionData.get(indexInPartitionSpec, Integer.class);
          if (dateVal != null) {
            dateVector.set(row, TimeUnit.DAYS.toMillis(dateVal));
          }
          break;
        case TIME:
          TimeMilliVector timeVector = (TimeMilliVector) vv;
          Long timeVal = partitionData.get(indexInPartitionSpec, Long.class);
          if (timeVal != null) {
            // Divide by 1000 to convert microseconds to millis
            timeVector.set(row, Math.toIntExact(timeVal / 1000));
          }
          break;
        case TIMESTAMP:
          TimeStampMilliVector timeStampVector = (TimeStampMilliVector) vv;
          Long timestampVal = partitionData.get(indexInPartitionSpec, Long.class);
          if (timestampVal != null) {
            // Divide by 1000 to convert microseconds to millis
            timeStampVector.set(row, timestampVal / 1000);
          }
          break;
        case DECIMAL:
          DecimalVector decimal = (DecimalVector) vv;
          BigDecimal decimalVal = partitionData.get(indexInPartitionSpec, BigDecimal.class);
          if (decimalVal != null) {
            byte[] bytes = decimalVal.unscaledValue().toByteArray();
            /* set the bytes in LE format in the buffer of decimal vector, we will swap
             * the bytes while writing into the vector.
             */
            decimal.setBigEndian(row, bytes);
          }
          break;
        default:
          throw new UnsupportedOperationException("Unsupported type: " + majorType);
      }
    }
  }

  public CloseableIterator<PartitionStatsEntry> getStatsEntryIterator() {
    return statsEntryIterator;
  }

  public String getFileLocation() {
    return fileLocation;
  }

  public Map<String, Integer> getPartitionColNameToSpecColIdMap() {
    return partitionColNameToSpecColIdMap;
  }

  /** Maintains counters which tests can use to verify the behavior */
  @com.google.common.annotations.VisibleForTesting
  public static class TestCounters {
    private static final AtomicLong evaluatedRecordWithSarg = new AtomicLong(0);
    private static final AtomicLong evaluatedRecordWithEval = new AtomicLong(0);

    public static void incrementRecordCountEvaluatedWithSargCounter() {
      evaluatedRecordWithSarg.incrementAndGet();
    }

    public static void incrementRecordCountEvaluatedWithEvalCounter() {
      evaluatedRecordWithEval.incrementAndGet();
    }

    public static long getEvaluatedRecordCountWithSargCounter() {
      return evaluatedRecordWithSarg.get();
    }

    public static long getEvaluatedRecordCountWithEvalCounter() {
      return evaluatedRecordWithEval.get();
    }

    public static void resetCounters() {
      evaluatedRecordWithSarg.set(0);
      evaluatedRecordWithEval.set(0);
    }
  }
}
