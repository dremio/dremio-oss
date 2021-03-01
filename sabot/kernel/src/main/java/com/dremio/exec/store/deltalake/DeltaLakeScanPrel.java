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

import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_JOINER;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_ADD_PATH;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_REMOVE_PATH;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.ComplexSchemaFlattener;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.physical.BroadcastExchangePrel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.JoinPruleBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.handlers.PrelFinalizable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ExpressionInputRewriter;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.dremio.sabot.op.join.JoinUtils;
import com.google.common.collect.ImmutableList;

/**
 * DeltaLake dataset prel
 */
public class DeltaLakeScanPrel extends ScanRelBase implements Prel, PrelFinalizable {
  private final ParquetScanFilter filter;
  private final boolean arrowCachingEnabled;
  private final PruneFilterCondition pruneCondition;

  public DeltaLakeScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                           StoragePluginId pluginId, TableMetadata tableMetadata, List<SchemaPath> projectedColumns,
                           double observedRowcountAdjustment, ParquetScanFilter filter, boolean arrowCachingEnabled,
                           PruneFilterCondition pruneCondition) {
    super(cluster, traitSet, table, pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment);
    this.filter = filter;
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.pruneCondition = pruneCondition;
  }

  private List<ParquetFilterCondition> getConditions() {
    return filter == null ? null : filter.getConditions();
  }

  public ParquetScanFilter getFilter() {
    return filter;
  }

  public boolean isArrowCachingEnabled() {
    return arrowCachingEnabled;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DeltaLakeScanPrel(getCluster(), getTraitSet(), getTable(), getPluginId(), getTableMetadata(),
      getProjectedColumns(), getObservedRowcountAdjustment(), filter, arrowCachingEnabled, pruneCondition);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    return new DeltaLakeScanPrel(getCluster(), getTraitSet(), getTable(), getPluginId(), getTableMetadata(),
      projection, getObservedRowcountAdjustment(), filter, arrowCachingEnabled, pruneCondition);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.DEFAULT;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

  @Override
  public Prel finalizeRel() {
    /*
     *                        +--------------------+
     *                        |    TableFunction   |
     *                        |(Parquet data scan) |
     *                        +--------------------+
     *                                  |
     *                                  |
     *                        +--------------------+
     *                        |      Exchange      |
     *                        +--------------------+
     *                                  |
     *                                  |--> ( Expand DeltaLakeScanPrel here )
     *                        +--------------------+
     *                        |    TableFunction   |
     *                        | (Split generation) |
     *                        +--------------------+
     *                                  |
     *                                  |
     *                        +--------------------+
     *                        |       Filter       |
     *                        | remove_path = null |
     *                        +--------------------+
     *                                  |
     *                                  |
     *                      +------------------------+
     *                      |       HashJoin         |
     *                      | add_path = remove_path |
     *                      +------------------------+
     *                                  |
     *                                  |
     *               |--------------------------------------|
     *               |            Broadcasting?             |
     *               |<----------------|------------------->|
     *               |                 |                    |
     *             | NO |           | YES |               | NO |
     *               |                 |                    |
     *               |                 |                    |
     *               |       (Only broadcast exchange       |
     *               |            on build side)            |
     *               |                 |    |               |
     *               |                 |    |               |
     * +----------------------------+  |    |  +----------------------------+
     * |  HashToRandomExchangePrel  |  |    |  |  HashToRandomExchangePrel  |
     * +----------------------------+  |    |  +----------------------------+
     *               |                 |    |                |
     *               |<----------------|    |                |
     *               |                      |                |
     *               |                      |                |
     *               |    +----------------------------+     |
     *               |    |    BroadcastExchangePrel   |     |
     *               |    +----------------------------+     |
     *               |                   |                   |
     *               |                   |------------------>|
     * +----------------------------+          +----------------------------+
     * |           Filter           |          |           Filter           |
     * |      add_path <> null      |          |     remove_path <> null    |
     * |  Partition & stats prune   |          |                            |
     * +----------------------------+          +----------------------------+
     *               |                                      |
     *               |                                      |
     * +----------------------------+          +----------------------------+
     * |          Project           |          |          Project           |
     * |     (Flatten schema)       |          |      (Flatten schema)      |
     * +----------------------------+          +----------------------------+
     *               |                                      |
     *               |                                      |
     * +----------------------------+          +----------------------------+
     * | DeltaLakeCommitLogScanPrel |          | DeltaLakeCommitLogScanPrel |
     * |     (For added paths)      |          |     (For removed paths)    |
     * +----------------------------+          +----------------------------+
     */


    // Exchange above DeltaLog scan phase
    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
    RelTraitSet relTraitSet = getCluster().getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);
    HashToRandomExchangePrel parquetSplitsExchange = new HashToRandomExchangePrel(getCluster(), relTraitSet,
            expandDeltaLakeScan(), distributionTrait.getFields(),
            HashPrelUtil.DREMIO_SPLIT_DISTRIBUTE_HASH_FUNCTION_NAME);

    // Parquet scan phase
    TableFunctionConfig parquetScanTableFunctionConfig = TableFunctionUtil.getParquetScanTableFunctionConfig(
      tableMetadata, getConditions(), getProjectedColumns(), arrowCachingEnabled);

    return new TableFunctionPrel(getCluster(), getTraitSet().plus(DistributionTrait.ANY), parquetSplitsExchange, tableMetadata,
      ImmutableList.copyOf(getProjectedColumns()), parquetScanTableFunctionConfig, getRowType(), rm -> (double) tableMetadata.getApproximateRecordCount());
  }

  public static RelDataType getSplitRowType(RelOptCluster cluster) {
    final RelDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();
    builder.add(new RelDataTypeFieldImpl(RecordReader.SPLIT_INFORMATION, 0, cluster.getTypeFactory().createSqlType(SqlTypeName.VARBINARY)));
    return builder.build();
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    if (tableMetadata.getReadDefinition().getManifestScanStats() != null) {
      return tableMetadata.getReadDefinition().getManifestScanStats().getRecordCount();
    }
    return tableMetadata.getSplitCount();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    if (filter != null) {
      return pw.item("filters", filter);
    }
    return pw;
  }

  public RelNode expandDeltaLakeScan() {
    RexBuilder rexBuilder = getCluster().getRexBuilder();
    JoinRelType joinRelType = JoinRelType.LEFT;

    // Create DeltaLakeScans for added and removed paths
    RelNode addPathScan = createDeltaLakeCommitLogScan(rexBuilder, true);
    RelNode removePathScan = createDeltaLakeCommitLogScan(rexBuilder, false);

    if (checkBroadcastConditions(joinRelType, addPathScan, removePathScan)) {
      removePathScan = new BroadcastExchangePrel(removePathScan.getCluster(), removePathScan.getTraitSet(), removePathScan);
    } else {
      DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
      DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
      RelTraitSet relTraitSet = getCluster().getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);

      addPathScan = new HashToRandomExchangePrel(addPathScan.getCluster(), relTraitSet,
        addPathScan, distributionTrait.getFields());

      removePathScan = new HashToRandomExchangePrel(removePathScan.getCluster(), relTraitSet,
        removePathScan, distributionTrait.getFields());
    }

    // Join the DeltaLakeScans on add_path = remove_path
    // Find the respective path fields on each side
    Pair<Integer, RelDataTypeField> addPathField = findFieldWithIndex(addPathScan, SCHEMA_ADD_PATH);
    Pair<Integer, RelDataTypeField> removePathField = findFieldWithIndex(removePathScan, SCHEMA_REMOVE_PATH);

    RelDataType joinType = addPathField.right.getType();
    RexNode joinCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeInputRef(joinType, addPathField.left),
      rexBuilder.makeInputRef(joinType, addPathScan.getRowType().getFieldCount() + removePathField.left /* Add the offset */));

    HashJoinPrel hashJoinPrel = HashJoinPrel.create(addPathScan.getCluster(), addPathScan.getTraitSet(), addPathScan, removePathScan,
      joinCondition, joinRelType, JoinUtils.projectAll(addPathScan.getRowType().getFieldCount() + removePathScan.getRowType().getFieldCount()));

    removePathField = findFieldWithIndex(hashJoinPrel, SCHEMA_REMOVE_PATH);
    // Filter removed path from the join
    RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
      rexBuilder.makeInputRef(removePathField.right.getType(), removePathField.left));
    FilterPrel notRemovedFilter = FilterPrel.create(hashJoinPrel.getCluster(), hashJoinPrel.getTraitSet(), hashJoinPrel, filterCondition);
    // Split generation table function

    TableFunctionConfig splitGenTableFunctionConfig = TableFunctionUtil.getSplitGenFunctionConfig(tableMetadata, Collections.EMPTY_LIST);
    return new TableFunctionPrel(getCluster(),
            getTraitSet().plus(DistributionTrait.ANY),
            notRemovedFilter,
            tableMetadata,
            ImmutableList.of(SchemaPath.getSimplePath(RecordReader.SPLIT_INFORMATION)),
            splitGenTableFunctionConfig,
            getSplitRowType(getCluster()),
            rm -> rm.getRowCount(notRemovedFilter));
  }

  private boolean checkBroadcastConditions(JoinRelType joinRelType, RelNode probe, RelNode build) {
    final double probeRowCount = getRowCount(probe);
    final double buildRowCount = getRowCount(build);
    return JoinPruleBase.checkBroadcastConditions(joinRelType, probe, build, probeRowCount, buildRowCount);
  }

  private double getRowCount(RelNode relNode) {
    // Get the row count of commit file scans
    Queue<RelNode> queue = new LinkedList<>();
    queue.add(relNode);
    while (!queue.isEmpty()) {
      RelNode rel = queue.poll();
      if (rel instanceof DeltaLakeCommitLogScanPrel) {
        return rel.estimateRowCount(rel.getCluster().getMetadataQuery());
      } else {
        queue.addAll(rel.getInputs());
      }
    }
    throw new RuntimeException(String.format("Unable to find DeltaLakeCommitLogScanPrel in:\n%s", RelOptUtil.toString(relNode)));
  }

  private RelNode createDeltaLakeCommitLogScan(RexBuilder rexBuilder, boolean scanForAddedPaths) {
    // Create DeltaLake commit log scans
    DeltaLakeCommitLogScanPrel deltaLakeCommitLogScanPrel = new DeltaLakeCommitLogScanPrel(
      getCluster(),
      getTraitSet().plus(DistributionTrait.ANY), /*
                                                  * The broadcast condition depends on the probe side being non-SINGLETON. Since
                                                  * we will only be broadcasting commit log files, it should primarily depend on
                                                  * the number of added and removed files. Making the distribution trait of type
                                                  * ANY here so that the broadcast check logic falls back to row count estimate.
                                                  */
      getTableMetadata(),
      isArrowCachingEnabled(),
      scanForAddedPaths);

    // Flatten the row type
    RelNode flattened = flattenRowType(deltaLakeCommitLogScanPrel, rexBuilder);

    Pair<Integer, RelDataTypeField> flattenedField = findFieldWithIndex(flattened, scanForAddedPaths ? SCHEMA_ADD_PATH : SCHEMA_REMOVE_PATH);

    // Add a Filter on top to filter out null values and also do partition and stats pruning
    // Only partition pruning to be supported in MVP.
    RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
      rexBuilder.makeInputRef(flattenedField.right.getType(), flattenedField.left));
    if (scanForAddedPaths) {
      final RexNode partitionCond = getPartitionCondition(flattened.getCluster().getRexBuilder(), flattened, pruneCondition);
      if (partitionCond != null) {
        cond = rexBuilder.makeCall(SqlStdOperatorTable.AND, cond, partitionCond);
      }
    }
    return FilterPrel.create(flattened.getCluster(), flattened.getTraitSet(), flattened, cond);
  }

  private RexNode getPartitionCondition(RexBuilder builder, RelNode input, PruneFilterCondition pruneCondition) {
    if (pruneCondition == null) {
      return null;
    }
    RexNode partitionExpression = pruneCondition.getPartitionExpression();
    if (partitionExpression == null) {
      return null;
    }
    return partitionExpression.accept(new ExpressionInputRewriter(builder, getRowType(), input, ""));
  }

  private RelNode flattenRowType(RelNode relNode, RexBuilder rexBuilder) {
    RelDataType rowType = relNode.getRowType();
    ComplexSchemaFlattener flattener = new ComplexSchemaFlattener(rexBuilder, DELTA_FIELD_JOINER);
    flattener.flatten(rowType);

    RelDataType newRowType = RexUtil.createStructType(relNode.getCluster().getTypeFactory(), flattener.getExps(), flattener.getFields(), SqlValidatorUtil.F_SUGGESTER);

    return ProjectPrel.create(relNode.getCluster(), relNode.getTraitSet(), relNode,
      flattener.getExps(), newRowType);
  }

  private Pair<Integer, RelDataTypeField> findFieldWithIndex(RelNode relNode, String fieldName) {
    Pair<Integer, RelDataTypeField> fieldPair = MoreRelOptUtil.findFieldWithIndex(relNode.getRowType().getFieldList(), fieldName);

    if (fieldPair == null) {
      throw new RuntimeException(String.format("Unable to find field '%s' in the schema", fieldName));
    }
    return fieldPair;
  }

}
