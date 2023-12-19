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
import static com.dremio.exec.store.iceberg.IcebergUtils.getCurrentPartitionSpec;
import static com.dremio.exec.store.iceberg.IcebergUtils.getUsedIndices;
import static com.dremio.proto.model.PartitionStats.PartitionStatsKey;
import static com.dremio.proto.model.PartitionStats.PartitionStatsValue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsReader;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.common.ImmutableDremioFileAttrs;
import com.dremio.exec.planner.common.PartitionStatsHelper;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.partitionstats.cache.PartitionStatsCache;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.context.OperatorStats;
import com.google.common.collect.Maps;

import io.opentelemetry.api.trace.Span;

/**
 * Class to utilize cache for partition stats.
 */
public class PartitionStatsBasedPrunerCache implements RecordPrunerCache{

  public static final Logger logger = LoggerFactory.getLogger(PartitionStatsBasedPrunerCache.class);
  private final PartitionStatsCache partitionStatsPredicateCache;
  private final OptimizerRulesContext context;
  private final ScanRelBase scan;
  private final List<SchemaPath> projectedColumns;
  private final TableMetadata tableMetadata;
  private final boolean specEvolTransEnabled;
  private final List<String> partitionColumns;
  private final String partitionStatsFile;
  private final Long partitionStatsFileLength;
  private final StoragePluginId storagePluginId;
  private final MutablePlugin plugin;
  private final PruneFilterCondition pruneCondition;
  private final RelDataType rowType;
  private final BatchSchema batchSchema;

  public PartitionStatsBasedPrunerCache(OptimizerRulesContext context,
                                   ScanRelBase scan,
                                   PruneFilterCondition pruneCondition,
                                   PartitionStatsCache partitionStatsPredicateCache){
    this.scan = scan;
    this.context = context;
    this.partitionStatsPredicateCache = partitionStatsPredicateCache;
    this.tableMetadata = scan.getTableMetadata();
    this.projectedColumns = scan.getProjectedColumns();
    this.rowType = scan.getRowType();
    this.specEvolTransEnabled = context.getPlannerSettings().getOptions().getOption(ENABLE_ICEBERG_SPEC_EVOL_TRANFORMATION);
    Span.current().setAttribute("dremio.table.name", tableMetadata.getName().getSchemaPath());
    this.partitionColumns = tableMetadata.getReadDefinition().getPartitionColumnsList();
    this.batchSchema = tableMetadata.getSchema();

    ImmutableDremioFileAttrs partitionStatsFileInfo = PartitionStatsHelper.getPartitionStatsFileAttrs(scan);
    this.partitionStatsFile = partitionStatsFileInfo.fileName();
    this.partitionStatsFileLength = partitionStatsFileInfo.fileLength();
    this.storagePluginId = scan.getIcebergStatisticsPluginId(context);
    this.plugin = context.getCatalogService().getSource(storagePluginId);
    this.pruneCondition = pruneCondition;
  }

  @Override
  public Optional<PartitionStatsValue> lookupCache() throws Exception {

    Optional<PartitionStatsValue> survivingRecords = Optional.empty();
    if (shouldPrune()) {
      if (plugin instanceof SupportsIcebergRootPointer) {
        RexNode finalPruneCondition = pruneCondition.getPartitionExpression();
        if (specEvolTransEnabled && pruneCondition.getPartitionRange() != null) {
          finalPruneCondition = finalPruneCondition == null ? pruneCondition.getPartitionRange()
            : scan.getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.AND, pruneCondition.getPartitionRange(), finalPruneCondition);
        }
        if(context.getPlannerSettings().isPartitionStatsCacheEnabled()){
          PartitionStatsKey partitionStatsKey = partitionStatsPredicateCache.computeKey(
            scan.getCluster().getRexBuilder(),
            partitionStatsFile,
            finalPruneCondition,
            projectedColumns.stream().map(
              pc -> pc.getAsUnescapedPath()
            ).collect(Collectors.toList()));

          //Partition stats cache could have stats for the given partition stats file and prune condition.
          Optional<PartitionStatsValue> records = Optional.ofNullable(partitionStatsPredicateCache.get(partitionStatsKey));
          if(!records.isPresent()){
            records = computePartitionStats(finalPruneCondition);
            if(records.isPresent()){
              partitionStatsPredicateCache.put(partitionStatsKey, records.get());
            }
          }
          return records;
        }
        return computePartitionStats(finalPruneCondition);
      }
    }
    return survivingRecords;
  }

  private boolean shouldPrune() {
    boolean pruneConditionExists = pruneCondition != null && (pruneCondition.getPartitionExpression() != null
      || specEvolTransEnabled && pruneCondition.getPartitionRange() != null);

    return storagePluginId != null
      && pruneConditionExists
      && partitionColumns != null
      && StringUtils.isNotEmpty(partitionStatsFile)
      && !isConditionOnImplicitCol();
  }

  private boolean isConditionOnImplicitCol() {
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

  private Optional<PartitionStatsValue> computePartitionStats(RexNode finalPruneCondition) throws Exception {

    Optional<PartitionStatsValue> survivingRecords = Optional.empty();

    try (OperatorContextImpl operatorContext = newOperatorContext(context, scan)) {

      PartitionSpec spec = getCurrentPartitionSpec(tableMetadata.getDatasetConfig().getPhysicalDataset());
      if(spec == null){
        spec = IcebergUtils.getIcebergPartitionSpec(batchSchema, partitionColumns, null);
      }
      InputFile inputFile = createInputFile(operatorContext);

      PartitionStatsReader partitionStatsReader = new PartitionStatsReader(inputFile, spec);
      try (CloseableIterator closeableIterator = partitionStatsReader.iterator()) {
        if (!closeableIterator.hasNext()) {
          //handle the case where the stats are empty or not successfully generated
          logger.warn(String.format("Encountered empty partition stats file for table %s during row count estimation. File %s.",
            tableMetadata.getName().toString(), partitionStatsFile));
          //Returning an empty Optional indicates a problem with partition pruning
          //the upper layers handle it by considering all the rows of the table as qualified
          return Optional.empty();
        }
      }
      try (RecordPruner pruner = new PartitionStatsBasedPruner(inputFile.location(), partitionStatsReader, context, spec, scan)) {

        Map<String, Integer> partitionColToIdMap = createColNameToIdMap(partitionColumns);

        survivingRecords = Optional.of(
          pruner.prune(createColIdToNameMap(partitionColToIdMap), partitionColToIdMap, getUsedIndices, projectedColumns,
            tableMetadata, finalPruneCondition, batchSchema, rowType, scan.getCluster()));
      }
    } catch (RuntimeException | IOException e) {
      logger.error("Encountered exception during row count estimation: ", e);
    }
    return survivingRecords;
  }

  private InputFile createInputFile(OperatorContextImpl operatorContext) throws IOException {
    SupportsIcebergRootPointer icebergRootPointerPlugin = (SupportsIcebergRootPointer) plugin;

    return icebergRootPointerPlugin
      .createIcebergFileIO(
        icebergRootPointerPlugin.createFS(
          partitionStatsFile,
          context.getContextInformation().getQueryUser(),
          operatorContext),
        operatorContext,
        scan.getTableMetadata().getDatasetConfig().getFullPathList(),
        scan.getPluginId().getName(),
        partitionStatsFileLength)
      .newInputFile(partitionStatsFile);
  }

  /*
   * Build a mapping between partition column ID and the partition columns which are used
   * in the query. For example, for
   * partitionColToIdMap = {col2 -> 0, col4 -> 1, $_dremio_ -> 2, col6 -> 3}, and
   * getRowType().getFieldNames() = [col1, col3, col4, col6]
   * inUseColIdToNameMap would be {1 -> col4, 3 -> col6}
   */
  private Map<Integer, String> createColIdToNameMap(Map<String, Integer> partitionColToIdMap){
    Map<Integer, String> inUseColIdToNameMap = Maps.newHashMap();
    for (String col : rowType.getFieldNames()) {
      Integer partitionIndex = partitionColToIdMap.get(col);
      if (partitionIndex != null) {
        inUseColIdToNameMap.put(partitionIndex, col);
      }
    }
    return inUseColIdToNameMap;
  }

  /*
   * Build a mapping between partition column name and partition column ID.
   * The partition columns can have special columns added by Dremio. The ID is artificial,
   * i.e. it has nothing to do with the column ID or order of the column in the table.
   */
  private Map<String, Integer> createColNameToIdMap(List<String> partitionColumns){
    Map<String, Integer> partitionColToIdMap = Maps.newHashMap();
    int index = 0;
    for (String column : partitionColumns) {
      partitionColToIdMap.put(column, index++);
    }
    return partitionColToIdMap;
  }

  private OperatorContextImpl newOperatorContext(OptimizerRulesContext optimizerRulesContext, ScanRelBase scan) {
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
      null);
  }

}
