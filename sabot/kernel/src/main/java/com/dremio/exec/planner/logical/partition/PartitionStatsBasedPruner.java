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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_SPEC_EVOL_TRANFORMATION;
import static com.dremio.exec.store.iceberg.IcebergUtils.getUsedIndices;
import static org.apache.calcite.sql.SqlKind.AND;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsEntry;
import org.apache.iceberg.PartitionStatsReader;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

import com.dremio.common.AutoCloseables;
import com.dremio.common.VM;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.common.ImmutableDremioFileAttrs;
import com.dremio.exec.planner.common.PartitionStatsHelper;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.partition.FindSimpleFilters.StateHolder;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.Pointer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Implementation of {@link RecordPruner} which prunes based on Iceberg partition stats
 */
public class PartitionStatsBasedPruner extends RecordPruner {
  private static final List<SqlKind> SARG_PRUNEABLE_OPERATORS = Arrays.asList(EQUALS, AND, LESS_THAN,
    LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL);

  private final PartitionSpec partitionSpec;
  private final CloseableIterator<PartitionStatsEntry> statsEntryIterator;
  private Map<Integer, Integer> partitionColIdToSpecColIdMap; // partition col ID to col ID in Iceberg partition spec
  private Map<String, Integer> partitionColNameToSpecColIdMap; // col name to col ID in Iceberg partition spec
  private String fileLocation;

  public PartitionStatsBasedPruner(String fileLocation, PartitionStatsReader partitionStatsReader, OptimizerRulesContext rulesContext,
                                   PartitionSpec partitionSpec) {
    super(rulesContext);
    this.statsEntryIterator = partitionStatsReader.iterator();
    this.partitionSpec = partitionSpec;
    this.fileLocation = fileLocation;
  }

  @WithSpan("partitions-stats-prune")
  @Override
  public Pair<Long, Long> prune(
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

    FindSimpleFilters rexVisitor = new FindSimpleFilters(cluster.getRexBuilder(), true, false, SARG_PRUNEABLE_OPERATORS);
    ImmutableList<RexCall> rexConditions = ImmutableList.of();
    StateHolder holder = pruneCondition.accept(rexVisitor);
    if (!hasDecimalCols(holder)) {
      rexConditions = holder.getConditions();
    }

    long qualifiedRecordCount = 0, qualifiedFileCount = 0;
    ConditionsByColumn conditionsByColumn = buildSargPrunableConditions(usedIndexes, projectedColumns, rexVisitor, rexConditions);
    if (canPruneWhollyWithSarg(holder, conditionsByColumn)) {
      Pair<Long, Long> counts = evaluateWithSarg(conditionsByColumn, tableMetadata);
      qualifiedFileCount = counts.getRight();
      qualifiedRecordCount = counts.getLeft();
    } else {
      int batchIndex = 0;
      long[] recordCounts = null;
      long[] fileCounts = null;
      LogicalExpression materializedExpr = null;
      boolean countOverflowFound = false;
      while (!countOverflowFound && statsEntryIterator.hasNext()) {
        List<PartitionStatsEntry> entriesInBatch = createRecordBatch(conditionsByColumn, batchIndex);
        int batchSize = entriesInBatch.size();
        if (batchSize == 0) {
          logger.debug("batch: {} is empty as sarg pruned all entries", batchIndex);
          break;
        }

        if (batchIndex == 0) {
          setupVectors(inUseColIdToNameMap, partitionColToIdMap, batchSize);
          materializedExpr = materializePruneExpr(pruneCondition, rowType, cluster);
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

            if (recordCounts[i] < 0 || qualifiedRecordCount < 0 || fileCounts[i] < 0 || qualifiedFileCount < 0) {
              qualifiedRecordCount = tableMetadata.getApproximateRecordCount();
              qualifiedFileCount = tableMetadata.getDatasetConfig().getReadDefinition().getManifestScanStats().getRecordCount();
              countOverflowFound = true;
              logger.info("Record count overflowed while evaluating partition filter. Partition stats file {}. Partition Expression {}", this.fileLocation, materializedExpr.toString());
              break;
            }
          }
        }

        logger.debug("Within batch: {}, qualified records: {}, qualified files: {}", batchIndex, qualifiedRecordCount, qualifiedFileCount);
        batchIndex++;
      }

      if (countOverflowFound) {
        qualifiedRecordCount = tableMetadata.getApproximateRecordCount();
        qualifiedFileCount = tableMetadata.getDatasetConfig().getReadDefinition().getManifestScanStats().getRecordCount();
      }
    }
    qualifiedRecordCount = ( qualifiedRecordCount < 0 ) ? tableMetadata.getApproximateRecordCount() : qualifiedRecordCount;
    qualifiedFileCount = (qualifiedFileCount < 0) ? tableMetadata.getDatasetConfig().getReadDefinition().getManifestScanStats().getRecordCount() : qualifiedFileCount;
    updateCounters(holder);
    return Pair.of(qualifiedRecordCount, qualifiedFileCount * optimizerContext.getPlannerSettings().getNoOfSplitsPerFile());
  }

  @Override
  public void close() {
    AutoCloseables.close(RuntimeException.class, statsEntryIterator, super::close);
  }

  protected void updateCounters(StateHolder holder) {
    // Update test counters only when assertions are enabled, which is the case for test flow
    if (VM.areAssertsEnabled()) {
      boolean hasDecimalCols = hasDecimalCols(holder);
      if (holder.hasConditions() && !hasDecimalCols) {
        TestCounters.incrementRecordCountEvaluatedWithSargCounter(); // expression was partly or wholly evaluated with sarg
      }

      if (holder.hasRemainingExpression() || hasDecimalCols) {
        TestCounters.incrementRecordCountEvaluatedWithEvalCounter();
      }
    }
  }

  private Pair<Long, Long> evaluateWithSarg(ConditionsByColumn conditionsByColumn, TableMetadata tableMetadata) {
    timer.start();
    long qualifiedCount = 0, qualifiedFileCount = 0;
    while (statsEntryIterator.hasNext()) {
      PartitionStatsEntry statsEntry = statsEntryIterator.next();
      StructLike partitionData = statsEntry.getPartition();
      if (isRecordMatch(conditionsByColumn, partitionData)) {
       long statsEntryReocrdCount = statsEntry.getRecordCount();
       long statsEntryFileCount = statsEntry.getFileCount();
       qualifiedCount += statsEntryReocrdCount;
       qualifiedFileCount += statsEntryFileCount;

        if (statsEntryReocrdCount < 0 || qualifiedCount < 0) {
          qualifiedCount = tableMetadata.getApproximateRecordCount();
          logger.info("Record count overflowed while evaluating partition filter. Partition stats file {}. Partition Expression {}", this.fileLocation, conditionsByColumn.toString());
          break;
        }

        if (statsEntryFileCount < 0 || qualifiedFileCount < 0) {
          qualifiedFileCount = tableMetadata.getDatasetConfig().getReadDefinition().getManifestScanStats().getRecordCount();
          logger.info("File count overflowed while evaluating partition filter. Partition stats file {}. Partition Expression {}", this.fileLocation, conditionsByColumn.toString());
          break;
        }
      }
    }
    logger.debug("Elapsed time to find surviving records: {}ms", timer.elapsed(TimeUnit.MILLISECONDS));
    timer.reset();

    return Pair.of(qualifiedCount, qualifiedFileCount);
  }

  private List<PartitionStatsEntry> createRecordBatch(ConditionsByColumn conditionsByColumn, int batchIndex) {
    timer.start();
    List<PartitionStatsEntry> entriesInBatch = new ArrayList<>();
    while (entriesInBatch.size() < PARTITION_BATCH_SIZE && statsEntryIterator.hasNext()) {
      PartitionStatsEntry statsEntry = statsEntryIterator.next();
      StructLike partitionData = statsEntry.getPartition();
      if (!conditionsByColumn.hasConditions() || isRecordMatch(conditionsByColumn, partitionData)) {
        entriesInBatch.add(statsEntry);
      }
    }

    logger.debug("Elapsed time to get list of partition stats entries for the current batch: {}ms within batchIndex: {}",
      timer.elapsed(TimeUnit.MILLISECONDS), batchIndex);
    timer.reset();
    return entriesInBatch;
  }

  private void populateVectors(int batchIndex, long[] recordCounts, long[] fileCounts, List<PartitionStatsEntry> entriesInBatch) {
    timer.start();
    // Loop over partition stats and populate record count, vectors etc.
    for (int i = 0; i < entriesInBatch.size(); i++) {
      PartitionStatsEntry statsEntry = entriesInBatch.get(i);
      StructLike partitionData = statsEntry.getPartition();
      recordCounts[i] = statsEntry.getRecordCount();
      fileCounts[i] = statsEntry.getFileCount();
      writePartitionValues(i, partitionData);
    }
    logger.debug("Elapsed time to populate partition column vectors: {}ms within batchIndex: {}",
      timer.elapsed(TimeUnit.MILLISECONDS), batchIndex);
    timer.reset();
  }

  private boolean canPruneWhollyWithSarg(StateHolder holder, ConditionsByColumn conditionsByColumn) {
    return !holder.hasRemainingExpression() && conditionsByColumn.hasConditions();
  }

  private ConditionsByColumn buildSargPrunableConditions(Function<RexNode, List<Integer>> usedIndexes,
                                                         List<SchemaPath> projectedColumns,
                                                         FindSimpleFilters rexVisitor,
                                                         ImmutableList<RexCall> rexConditions) {
    ConditionsByColumn conditionsByColumn = new ConditionsByColumn();
    for (RexCall condition : rexConditions) {
      ImmutableList<RexNode> ops = condition.operands;
      if (ops.size() != 2) {
        continue; // We're only interested in conditions with two operands
      }

      StateHolder a = ops.get(0).accept(rexVisitor);
      StateHolder b = ops.get(1).accept(rexVisitor);
      if (a.getType() == FindSimpleFilters.Type.LITERAL && b.getType() == FindSimpleFilters.Type.INPUT) {
        // e.g. '2020' = year
        addCondition(usedIndexes, projectedColumns, condition, conditionsByColumn, b, a);
      } else if (a.getType() == FindSimpleFilters.Type.INPUT && b.getType() == FindSimpleFilters.Type.LITERAL) {
        // e.g. year = '2020'
        addCondition(usedIndexes, projectedColumns, condition, conditionsByColumn, a, b);
      }
    }
    return conditionsByColumn;
  }

  private void addCondition(Function<RexNode, List<Integer>> usedIndexes, List<SchemaPath> projectedColumns,
                            RexCall condition, ConditionsByColumn conditionsByColumn,
                            StateHolder input, StateHolder literal) {
    int colIndex = usedIndexes.apply(input.getNode()).stream().findFirst().get();
    String colName = projectedColumns.get(colIndex).getAsUnescapedPath();
    Comparable<?> value = getValueFromFilter(colName, literal.getNode());
    conditionsByColumn.addCondition(colName, new Condition(condition.getKind(), value));
  }

  private boolean isRecordMatch(ConditionsByColumn conditionsByColumn, StructLike partitionData) {
    for (String colName : conditionsByColumn.getColumnNames()) {
      Integer indexInPartitionSpec = partitionColNameToSpecColIdMap.get(colName);
      Comparable<?> valueFromPartitionData = getValueFromPartitionData(indexInPartitionSpec, colName, partitionData);
      if (!conditionsByColumn.satisfiesComparison(colName, valueFromPartitionData)) {
        return false;
      }
    }
    return true;
  }

  protected boolean hasDecimalCols(StateHolder holder) {
    Pointer<Boolean> hasDecimalCols = new Pointer<>(false);
    holder.getConditions().forEach(condition -> condition.getOperands().forEach(
      operand -> {
        if (operand.getKind().equals(SqlKind.INPUT_REF) && operand.getType().getSqlTypeName().equals(SqlTypeName.DECIMAL)) {
          hasDecimalCols.value = true;
        }
      }));
    return hasDecimalCols.value;
  }

  private Comparable getValueFromFilter(String colName, RexNode node) {
    TypeProtos.MinorType minorType = partitionColNameToTypeMap.get(colName).getMinorType();
    RexLiteral literal = (RexLiteral) node;
    switch (minorType) {
      case BIT:
        return literal.getValueAs(Boolean.class);
      case INT:
      case DATE:
      case TIME:
        return literal.getValueAs(Integer.class);
      case BIGINT:
      case TIMESTAMP:
        return literal.getValueAs(Long.class);
      case FLOAT4:
        return literal.getValueAs(Float.class);
      case FLOAT8:
        return literal.getValueAs(Double.class);
      case VARCHAR:
        return literal.getValueAs(String.class);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + minorType);
    }
  }

  private Comparable getValueFromPartitionData(int indexInPartitionSpec, String colName, StructLike partitionData) {
    MajorType majorType = partitionColNameToTypeMap.get(colName);
    switch (majorType.getMinorType()) {
      case BIT:
        return partitionData.get(indexInPartitionSpec, Boolean.class);
      case INT:
      case DATE:
        return partitionData.get(indexInPartitionSpec, Integer.class);
      case BIGINT:
        return partitionData.get(indexInPartitionSpec, Long.class);
      case TIME:
      case TIMESTAMP:
        // Divide by 1000 to convert microseconds to millis
        return partitionData.get(indexInPartitionSpec, Long.class) / 1000;
      case FLOAT4:
        return partitionData.get(indexInPartitionSpec, Float.class);
      case FLOAT8:
        return partitionData.get(indexInPartitionSpec, Double.class);
      case VARBINARY:
        return partitionData.get(indexInPartitionSpec, ByteBuffer.class);
      case FIXEDSIZEBINARY:
      case VARCHAR:
        return partitionData.get(indexInPartitionSpec, String.class);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + majorType);
    }
  }

  private void populateMaps(Map<Integer, String> fieldNameMap) {
    Set<Integer> partitionColIds = partitionColIdToTypeMap.keySet();
    partitionColIdToSpecColIdMap = new HashMap<>(partitionColIds.size());
    partitionColNameToSpecColIdMap = new HashMap<>(partitionColIds.size());
    for (Integer partitionColIndex : partitionColIds) {
      String colName = fieldNameMap.get(partitionColIndex);
      List<PartitionField> fields = partitionSpec.fields();
      for (int i = 0; i < fields.size(); i++) {
        if (fields.get(i).name().equalsIgnoreCase(colName)) {
          partitionColIdToSpecColIdMap.put(partitionColIndex, i);
          partitionColNameToSpecColIdMap.put(colName, partitionColIndex);
          break;
        }
      }
    }
  }

  /**
   * Sets all the vectors for a given entry in the batch
   */
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

  public static OperatorContextImpl newOperatorContext(OptimizerRulesContext optimizerRulesContext, ScanRelBase scan) {
    return new OperatorContextImpl(
      optimizerRulesContext.getPlannerSettings().getSabotConfig(),
      null, // DremioConfig
      null, // FragmentHandle
      null, // popConfig
      optimizerRulesContext.getAllocator().newChildAllocator("p-s-r-" + scan.getTableMetadata().getDatasetConfig().getName(), 0, Long.MAX_VALUE),
      null, // output allocator
      null, // code compiler
      new OperatorStats(new OpProfileDef(0, 0, 0, 0), optimizerRulesContext.getAllocator()), // stats
      null, // execution controls
      null, // fragment executor builder
      null, // executor service
      null, // function lookup context
      null, // context information
      optimizerRulesContext.getFunctionRegistry().getOptionManager(), // option manager
      null, // spill service
      null, // node debug context provider
      1000, // target batch size
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    );
  }

  @WithSpan("prune-partition-stats")
  public static Optional<Pair<Long, Long>> prune(OptimizerRulesContext context, ScanRelBase scan, PruneFilterCondition pruneCondition)
  throws Exception {
    final TableMetadata tableMetadata = scan.getTableMetadata();
    final RelDataType rowType = scan.getRowType();
    final List<SchemaPath> projectedColumns = scan.getProjectedColumns();
    boolean specEvolTransEnabled = context.getPlannerSettings().getOptions().getOption(ENABLE_ICEBERG_SPEC_EVOL_TRANFORMATION);
    Span.current().setAttribute("dremio.table.name", tableMetadata.getName().getSchemaPath());
    BatchSchema batchSchema = tableMetadata.getSchema();
    List<String> partitionColumns = tableMetadata.getReadDefinition().getPartitionColumnsList();
    ImmutableDremioFileAttrs partitionStatsFileInfo = PartitionStatsHelper.getPartitionStatsFileAttrs(scan);
    String partitionStatsFile = partitionStatsFileInfo.fileName();
    Long partionStatsFileLength = partitionStatsFileInfo.fileLength();
    Optional<Pair<Long, Long>> survivingRecords = Optional.empty();
    StoragePluginId storagePluginId = scan.getIcebergStatisticsPluginId(context);
    StoragePluginId tablePluginId = scan.getPluginId();
    if (shouldPrune(pruneCondition, partitionColumns, partitionStatsFile, projectedColumns, specEvolTransEnabled, storagePluginId)) {
      /*
       * Build a mapping between partition column name and partition column ID.
       * The partition columns can have special columns added by Dremio. The ID is artificial,
       * i.e. it has nothing to do with the column ID or order of the column in the table.
       */
      Map<String, Integer> partitionColToIdMap = Maps.newHashMap();
      int index = 0;
      for (String column : partitionColumns) {
        partitionColToIdMap.put(column, index++);
      }

      /*
       * Build a mapping between partition column ID and the partition columns which are used
       * in the query. For example, for
       * partitionColToIdMap = {col2 -> 0, col4 -> 1, $_dremio_ -> 2, col6 -> 3}, and
       * getRowType().getFieldNames() = [col1, col3, col4, col6]
       * inUseColIdToNameMap would be {1 -> col4, 3 -> col6}
       */
      Map<Integer, String> inUseColIdToNameMap = Maps.newHashMap();
      for (String col : rowType.getFieldNames()) {
        Integer partitionIndex = partitionColToIdMap.get(col);
        if (partitionIndex != null) {
          inUseColIdToNameMap.put(partitionIndex, col);
        }
      }

      MutablePlugin plugin = context.getCatalogService().getSource(storagePluginId);
      if (plugin instanceof SupportsIcebergRootPointer) {
        SupportsIcebergRootPointer icebergRootPointerPlugin = (SupportsIcebergRootPointer) plugin;
        PartitionSpec spec = IcebergUtils.getIcebergPartitionSpec(batchSchema, partitionColumns, null);
        try (OperatorContextImpl operatorContext = newOperatorContext(context, scan)) {
          FileSystem fs = icebergRootPointerPlugin.createFS(partitionStatsFile,
              context.getContextInformation().getQueryUser(), operatorContext);
          FileIO io = icebergRootPointerPlugin.createIcebergFileIO(fs, operatorContext, scan.getTableMetadata().getDatasetConfig().getFullPathList(), tablePluginId.getName(), partionStatsFileLength);
          InputFile inputFile = io.newInputFile(partitionStatsFile);
          PartitionStatsReader partitionStatsReader = new PartitionStatsReader(inputFile, spec);
          if(!partitionStatsReader.iterator().hasNext()){
            //handle the case where the stats are empty or not successfully generated
            logger.warn(String.format("Encountered empty partition stats file for table %s during row count estimation. File %s.",
              tableMetadata.getName().toString(), partitionStatsFile));
            //Returning an empty Optional indicates a problem with partition pruning
            //the upper layers handle it by considering all the rows of the table as qualified
            return Optional.empty();
          }
          try (RecordPruner pruner = new PartitionStatsBasedPruner(inputFile.location(), partitionStatsReader, context,
              spec)) {
            RexNode finalPruneCondition = pruneCondition.getPartitionExpression();
            if (specEvolTransEnabled && pruneCondition.getPartitionRange() != null) {
              finalPruneCondition = finalPruneCondition == null ? pruneCondition.getPartitionRange()
                : scan.getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.AND, pruneCondition.getPartitionRange(), finalPruneCondition);
            }
            survivingRecords = Optional.of(
                pruner.prune(inUseColIdToNameMap, partitionColToIdMap, getUsedIndices, projectedColumns,
                    tableMetadata, finalPruneCondition, batchSchema, rowType, scan.getCluster()));

          }
        } catch (RuntimeException | IOException e) {
          logger.error("Encountered exception during row count estimation: ", e);
        }
      }
    }

    return survivingRecords;
  }

  private static boolean shouldPrune(PruneFilterCondition pruneCondition, List<String> partitionColumns, String partitionStatsFile, List<SchemaPath> projectedColumns, boolean specEvolTransEnabled, StoragePluginId storagePluginId) {
    boolean pruneConditionExists = pruneCondition != null && (pruneCondition.getPartitionExpression() != null
      || specEvolTransEnabled && pruneCondition.getPartitionRange() != null);

    return storagePluginId != null && pruneConditionExists && partitionColumns != null && StringUtils.isNotEmpty(partitionStatsFile) && !isConditionOnImplicitCol(pruneCondition, projectedColumns);
  }

  private static boolean isConditionOnImplicitCol(PruneFilterCondition pruneCondition, List<SchemaPath> projectedColumns) {
    if (pruneCondition.getPartitionExpression() == null) {
      return false;
    }

    int updateColIndex = projectedColumns.indexOf(SchemaPath.getSimplePath(IncrementalUpdateUtils.UPDATE_COLUMN));
    final AtomicBoolean isImplicit = new AtomicBoolean(false);
    pruneCondition.getPartitionExpression().accept(new RexVisitorImpl<Void>(true) {
      @Override
      public Void visitInputRef(RexInputRef inputRef) {
        isImplicit.set(updateColIndex==inputRef.getIndex());
        return null;
      }
    });
    return isImplicit.get();
  }

  /**
   * Maintains counters which tests can use to verify the behavior
   */
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

  private static class ConditionsByColumn {
    Multimap<String, Condition> queryConditions = HashMultimap.create();

    void addCondition(String colName, Condition condition) {
      queryConditions.put(colName, condition);
    }

    boolean hasConditions() {
      return !queryConditions.isEmpty();
    }

    boolean satisfiesComparison(String colName, Comparable valueFromPartitionData) {
      for (Condition condition : queryConditions.get(colName)) {
        if (valueFromPartitionData == null || !condition.matches(valueFromPartitionData)) {
          return false;
        }
      }
      return true;
    }

    public Set<String> getColumnNames() {
      return queryConditions.keySet();
    }
  }

  private static class Condition {
    final Predicate<Comparable> matcher;

    Condition(SqlKind sqlKind, Comparable<?> valueFromCondition) {
      switch (sqlKind) {
        case EQUALS:
          matcher = valueFromPartitionData -> valueFromPartitionData.compareTo(valueFromCondition) == 0;
          break;
        case LESS_THAN:
          matcher = valueFromPartitionData -> valueFromPartitionData.compareTo(valueFromCondition) < 0;
          break;
        case LESS_THAN_OR_EQUAL:
          matcher = valueFromPartitionData -> valueFromPartitionData.compareTo(valueFromCondition) <= 0;
          break;
        case GREATER_THAN:
          matcher = valueFromPartitionData -> valueFromPartitionData.compareTo(valueFromCondition) > 0;
          break;
        case GREATER_THAN_OR_EQUAL:
          matcher = valueFromPartitionData -> valueFromPartitionData.compareTo(valueFromCondition) >= 0;
          break;
        default:
          throw new IllegalStateException("Unsupported SQL operator type: " + sqlKind);
      }
    }

    boolean matches(Comparable valueFromPartitionData) {
      return matcher.test(valueFromPartitionData);
    }
  }
}
