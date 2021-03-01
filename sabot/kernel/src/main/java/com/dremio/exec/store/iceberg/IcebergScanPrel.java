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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.store.RecordReader.SPLIT_INFORMATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.handlers.PrelFinalizable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ExpressionInputRewriter;
import com.dremio.exec.store.MinMaxRewriter;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.google.common.collect.ImmutableList;

/**
 * Iceberg dataset prel
 */
public class IcebergScanPrel extends ScanRelBase implements Prel, PrelFinalizable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergScanPrel.class);

  private final ParquetScanFilter filter;
  private final boolean arrowCachingEnabled;
  private final PruneFilterCondition pruneCondition;

  private static Function<RexNode, List<Integer>> getUsedIndices = (cond) -> {
    Set<Integer> usedIndices = new HashSet<>();
    cond.accept(new RexVisitorImpl<Void>(true) {
      @Override
      public Void visitInputRef(RexInputRef inputRef) {
        usedIndices.add(inputRef.getIndex());
        return null;
      }
    });
    return usedIndices.stream().sorted().collect(Collectors.toList());
  };

  public IcebergScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId, TableMetadata dataset,
                         List<SchemaPath> projectedColumns, double observedRowcountAdjustment, ParquetScanFilter filter, boolean arrowCachingEnabled, PruneFilterCondition pruneCondition) {
    super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment);
    this.filter = filter;
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.pruneCondition = pruneCondition;
  }

  private List<ParquetFilterCondition> getConditions() {
    return filter == null ? null : filter.getConditions();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IcebergScanPrel(getCluster(), traitSet, getTable(), pluginId, tableMetadata, getProjectedColumns(),
      observedRowcountAdjustment, this.filter, this.arrowCachingEnabled, this.pruneCondition);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    return new IcebergScanPrel(getCluster(), getTraitSet(), table, pluginId, tableMetadata, projection, observedRowcountAdjustment, this.filter, this.arrowCachingEnabled, this.pruneCondition);
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
    List<SchemaPath> manifestListReaderColumns = new ArrayList<>(Collections.singleton(SchemaPath.getSimplePath(SPLIT_INFORMATION)));
    BatchSchema manifestListReaderSchema = RecordReader.SPLIT_GEN_SCAN_SCHEMA;

    List<SchemaPath> manifestFileReaderColumns = new ArrayList<>(Collections.singleton(SchemaPath.getSimplePath(SPLIT_INFORMATION)));
    BatchSchema manifestFileReaderSchema = RecordReader.SPLIT_GEN_SCAN_SCHEMA;

    if (pruneCondition != null) {
      manifestListReaderSchema = getSchemaWithMinMaxUsedColumns(pruneCondition.getPartitionRange(), manifestListReaderColumns, manifestListReaderSchema);
      manifestFileReaderSchema = getSchemaWithUsedColumns(pruneCondition.getPartitionExpression(), manifestFileReaderColumns, manifestFileReaderSchema);
      manifestFileReaderSchema = getSchemaWithMinMaxUsedColumns(pruneCondition.getNonPartitionRange(), manifestFileReaderColumns, manifestFileReaderSchema);
    }

    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
    RelTraitSet relTraitSet = getCluster().getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);
    IcebergManifestListPrel manifestListPrel = new IcebergManifestListPrel(getCluster(), getTraitSet(), tableMetadata, manifestListReaderSchema, manifestListReaderColumns,
      getRowTypeFromProjectedColumns(manifestListReaderColumns, manifestListReaderSchema, getCluster()));

    RelNode input = manifestListPrel;
    RexNode manifestListCondition = getManifestListFilter(getCluster().getRexBuilder(), manifestListPrel);
    if (manifestListCondition != null) {
      // Manifest list filter
      input = new FilterPrel(getCluster(), getTraitSet(), manifestListPrel, manifestListCondition);
    }

    // exchange above manifest list scan, which is a leaf level easy scan
    HashToRandomExchangePrel manifestSplitsExchange = new HashToRandomExchangePrel(getCluster(), relTraitSet,
            input, distributionTrait.getFields(),
            HashPrelUtil.DREMIO_SPLIT_DISTRIBUTE_HASH_FUNCTION_NAME);

    // Manifest scan phase
    TableFunctionConfig manifestScanTableFunctionConfig =  TableFunctionUtil.getManifestScanTableFunctionConfig(tableMetadata, manifestFileReaderColumns, manifestFileReaderSchema, null);

    TableFunctionPrel manifestScanTF = new TableFunctionPrel(getCluster(), getTraitSet().plus(DistributionTrait.ANY), manifestSplitsExchange, tableMetadata,
      ImmutableList.copyOf(manifestFileReaderColumns), manifestScanTableFunctionConfig, getRowTypeFromProjectedColumns(manifestFileReaderColumns, manifestFileReaderSchema, getCluster()));

    RelNode input2 = manifestScanTF;
    final RexNode manifestFileCondition = getManifestFileFilter(getCluster().getRexBuilder(), manifestScanTF);
    if (manifestFileCondition != null) {
      // Manifest file filter
      input2 = new FilterPrel(getCluster(), getTraitSet(), manifestScanTF, manifestFileCondition);
    }

    // Exchange above manifest scan phase
    HashToRandomExchangePrel parquetSplitsExchange = new HashToRandomExchangePrel(getCluster(), relTraitSet,
            input2, distributionTrait.getFields(),
            HashPrelUtil.DREMIO_SPLIT_DISTRIBUTE_HASH_FUNCTION_NAME);

    // Parquet scan phase
    TableFunctionConfig parquetScanTableFunctionConfig = TableFunctionUtil.getParquetScanTableFunctionConfig(
      tableMetadata, getConditions(), getProjectedColumns(), arrowCachingEnabled);
    return new TableFunctionPrel(getCluster(), getTraitSet().plus(DistributionTrait.ANY), parquetSplitsExchange, tableMetadata,
      ImmutableList.copyOf(getProjectedColumns()), parquetScanTableFunctionConfig, getRowType());
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    return 1;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    if(filter != null){
      return pw.item("filters",  filter);
    }
    return pw;
  }

  public RexNode getManifestListFilter(RexBuilder builder, RelNode input) {
    if (pruneCondition == null) {
      return null;
    }
    RexNode partitionRange = pruneCondition.getPartitionRange();
    if (partitionRange == null) {
      return null;
    }
    return pruneCondition.getPartitionRange().accept(new MinMaxRewriter(builder, getRowType(), input));
  }

  public RexNode getManifestFileFilter(RexBuilder builder, RelNode input) {
    if (pruneCondition == null) {
      return null;
    }
    List<RexNode> filters = new ArrayList<>();
    RexNode nonPartitionRange = pruneCondition.getNonPartitionRange();
    RelDataType rowType = getRowType();
    if (nonPartitionRange != null) {
      filters.add(nonPartitionRange.accept(new MinMaxRewriter(builder, rowType, input)));
    }
    RexNode partitionExpression = pruneCondition.getPartitionExpression();
    if (partitionExpression != null) {
      filters.add(partitionExpression.accept(new ExpressionInputRewriter(builder, rowType, input, "_val")));
    }
    return filters.size() == 0 ? null : (filters.size()  == 1 ? filters.get(0) : builder.makeCall(SqlStdOperatorTable.AND, filters));
  }


  private BatchSchema getSchemaWithMinMaxUsedColumns(RexNode cond, List<SchemaPath> outputColumns, BatchSchema schema) {
    if (cond == null) {
      return schema;
    }

    List<SchemaPath> usedColumns = getUsedIndices.apply(cond).stream().map(projectedColumns::get).collect(Collectors.toList());
    usedColumns.forEach(c -> {
      List<String> nameSegments = c.getNameSegments();
      Preconditions.checkArgument(nameSegments.size() == 1);
      outputColumns.add(SchemaPath.getSimplePath(nameSegments.get(0) + "_min"));
      outputColumns.add(SchemaPath.getSimplePath(nameSegments.get(0) + "_max"));
    });

    List<Field> fields = tableMetadata.getSchema().maskAndReorder(usedColumns).getFields().stream()
      .flatMap(f -> Stream.of(new Field(f.getName() + "_min", f.getFieldType(), f.getChildren()), new Field(f.getName() + "_max", f.getFieldType(), f.getChildren())))
      .collect(Collectors.toList());
    return schema.cloneWithFields(fields);
  }

  private BatchSchema getSchemaWithUsedColumns(RexNode cond, List<SchemaPath> outputColumns, BatchSchema schema) {
    if (cond == null) {
      return schema;
    }

    List<SchemaPath> usedColumns = getUsedIndices.apply(cond).stream().map(projectedColumns::get).collect(Collectors.toList());
    usedColumns.forEach(c -> {
      List<String> nameSegments = c.getNameSegments();
      Preconditions.checkArgument(nameSegments.size() == 1);
      outputColumns.add(SchemaPath.getSimplePath(nameSegments.get(0) + "_val"));
    });

    List<Field> fields = tableMetadata.getSchema().maskAndReorder(usedColumns).getFields().stream()
      .map(f -> new Field(f.getName() + "_val", f.getFieldType(), f.getChildren()))
      .collect(Collectors.toList());
    return schema.cloneWithFields(fields);
  }
}
