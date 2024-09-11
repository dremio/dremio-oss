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
package com.dremio.exec.store.deltalake;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.RoundRobinExchangePrel;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.metadatarefresh.RefreshExecTableMetadata;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingScanPrel;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

public class DeltaLakeHistoryScanPlanBuilder {
  private final RelOptCluster cluster;
  private final RelTraitSet traitSet;
  private final RelOptTable table;
  private final TableMetadata tableMetadata;
  private final List<SchemaPath> projectedColumns;
  private final double observedRowcountAdjustment;
  private final StoragePluginId pluginId;

  public DeltaLakeHistoryScanPlanBuilder(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata tableMetadata,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment) {
    this.cluster = cluster;
    this.traitSet = traitSet;
    this.table = table;
    this.tableMetadata = tableMetadata;
    this.projectedColumns = projectedColumns;
    this.observedRowcountAdjustment = observedRowcountAdjustment;
    this.pluginId = tableMetadata.getStoragePluginId();
  }

  public static DeltaLakeHistoryScanPlanBuilder fromDrel(ScanRelBase drel) {
    return new DeltaLakeHistoryScanPlanBuilder(
        drel.getCluster(),
        drel.getTraitSet().plus(Prel.PHYSICAL),
        drel.getTable(),
        drel.getTableMetadata(),
        drel.getProjectedColumns(),
        drel.getObservedRowcountAdjustment());
  }

  /* Generates plan to distribute parsing of commit log json files.
   * Implements both table_history and table_snapshot table metadata functions.
   * The functions differ by schema, which is taken care of by Project
   *
   * Project [committed_at, snapshot_id, operation] or [made_current_at, snapshot_id]
   *          ▲
   *          │
   * DeltaLakeHistoryScan (parse json to [timestamp, version, operation])
   *          ▲
   *          │
   * RoundRobinExchange
   *          ▲
   *          │
   * DirListingScan (globPattern: "*.json")
   *
   */
  public RelNode build() {
    Prel listingScan = createDirListingScan();
    Prel distributionScan = createDistributionScan(listingScan);
    Prel historyScan = createHistoryScan(distributionScan);
    return createProjectionScan(historyScan);
  }

  private Prel createDirListingScan() {
    DatasetConfig datasetConfig = tableMetadata.getDatasetConfig();
    String tableLocation = datasetConfig.getPhysicalDataset().getFormatSettings().getLocation();
    Path datasetPath = Path.of(tableLocation).resolve(DeltaConstants.DELTA_LOG_DIR);

    PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
    List<PartitionValue> partitionValue = Collections.emptyList();
    DirListInputSplitProto.DirListInputSplit dirListInputSplit =
        DirListInputSplitProto.DirListInputSplit.newBuilder()
            .setRootPath(datasetPath.toString())
            .setOperatingPath(datasetPath.toString())
            .setReadSignature(Long.MAX_VALUE)
            .setIsFile(false)
            .setHasVersion(false)
            .setGlobPattern("*.json")
            .build();
    DatasetSplit split = DatasetSplit.of(Collections.emptyList(), 1, 1, dirListInputSplit::writeTo);
    partitionChunkListing.put(partitionValue, split);
    partitionChunkListing.computePartitionChunks();
    SplitsPointer splitsPointer =
        MaterializedSplitsPointer.of(
            0,
            AbstractRefreshPlanBuilder.convertToPartitionChunkMetadata(
                partitionChunkListing, datasetConfig),
            1);

    RefreshExecTableMetadata dirScanMetadata =
        new RefreshExecTableMetadata(
            pluginId,
            datasetConfig,
            tableMetadata.getUser(),
            splitsPointer,
            BatchSchema.EMPTY,
            null);
    return new DirListingScanPrel(
        cluster,
        traitSet,
        table,
        pluginId,
        dirScanMetadata,
        observedRowcountAdjustment,
        ImmutableList.of(),
        false,
        x -> estimateRowCount(),
        ImmutableList.of());
  }

  private Prel createDistributionScan(Prel input) {
    return new RoundRobinExchangePrel(cluster, traitSet.plus(DistributionTrait.ROUND_ROBIN), input);
  }

  private Prel createHistoryScan(Prel input) {
    final BatchSchema schema = SystemSchemas.DELTALAKE_HISTORY_SCAN_SCHEMA;
    final TableFunctionConfig historyScanConfig =
        TableFunctionUtil.getTableFunctionConfig(
            TableFunctionConfig.FunctionType.DELTALAKE_HISTORY_SCAN,
            schema,
            tableMetadata.getStoragePluginId());
    final RelDataType rowType =
        ScanPrelBase.getRowTypeFromProjectedColumns(
            schema.getFields().stream()
                .map(f -> SchemaPath.getSimplePath(f.getName()))
                .collect(Collectors.toList()),
            schema,
            cluster);

    return new TableFunctionPrel(
        cluster,
        traitSet,
        input,
        historyScanConfig,
        rowType,
        x -> estimateRowCount(),
        (long) estimateRowCount(),
        tableMetadata.getUser());
  }

  private double estimateRowCount() {
    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(cluster.getPlanner());
    double rowMultiplier =
        ((double) plannerSettings.getSliceTarget()
            / plannerSettings
                .getOptions()
                .getOption(PlannerSettings.DELTALAKE_HISTORY_SCAN_FILES_PER_THREAD));
    return Math.max(tableMetadata.getApproximateRecordCount() * rowMultiplier, 1);
  }

  private Prel createProjectionScan(Prel input) {
    final Set<String> firstLevelPaths = new LinkedHashSet<>();
    for (SchemaPath p : projectedColumns) {
      firstLevelPaths.add(p.getRootSegment().getNameSegment().getPath());
    }

    List<RexNode> projectExpressions = new ArrayList<>();
    tableMetadata
        .getSchema()
        .getFields()
        .forEach(
            field -> {
              if (!firstLevelPaths.contains(field.getName())) {
                return;
              }

              switch (field.getName()) {
                case "committed_at":
                case "made_current_at":
                  projectExpressions.add(createInputRef(input, SystemSchemas.TIMESTAMP));
                  break;

                case "snapshot_id":
                  projectExpressions.add(createInputRef(input, SystemSchemas.VERSION));
                  break;

                case "operation":
                  projectExpressions.add(createInputRef(input, SystemSchemas.OPERATION));
                  break;

                default:
                  projectExpressions.add(
                      cluster.getRexBuilder().makeNullLiteral(convertFieldType(field)));
                  break;
              }
            });
    RelDataType rowType =
        ScanPrelBase.getRowTypeFromProjectedColumns(
            projectedColumns, tableMetadata.getSchema(), cluster);
    return ProjectPrel.create(cluster, traitSet, input, projectExpressions, rowType);
  }

  private RexNode createInputRef(Prel input, String fieldName) {
    Pair<Integer, RelDataTypeField> fieldPair =
        MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), fieldName);
    if (fieldPair == null) {
      throw new RuntimeException(
          String.format("Unable to find field '%s' in the input", fieldName));
    }

    return cluster.getRexBuilder().makeInputRef(fieldPair.right.getType(), fieldPair.left);
  }

  private RelDataType convertFieldType(Field field) {
    return CalciteArrowHelper.wrap(CompleteType.fromField(field))
        .toCalciteType(cluster.getTypeFactory(), true);
  }
}
