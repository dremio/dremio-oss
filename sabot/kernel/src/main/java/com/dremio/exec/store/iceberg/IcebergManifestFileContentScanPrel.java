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

import static com.dremio.exec.store.RecordReader.COL_IDS;
import static com.dremio.exec.store.RecordReader.SPLIT_IDENTITY;
import static com.dremio.exec.store.RecordReader.SPLIT_INFORMATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.handlers.PrelFinalizable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.TableMetadata;
import com.google.common.collect.ImmutableList;

/**
 * table_files Metadata Functions use IcebergManifestFileContentScanPrel to fetch data file content from manifest file.
 */
public class IcebergManifestFileContentScanPrel extends ScanPrelBase implements PrelFinalizable {

  public IcebergManifestFileContentScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment, List<RelHint> hints) {
    super(cluster, traitSet, table, dataset.getStoragePluginId(), dataset, projectedColumns, observedRowcountAdjustment, hints, ImmutableList.of());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IcebergManifestFileContentScanPrel(getCluster(), traitSet, getTable(), tableMetadata, getProjectedColumns(),
      observedRowcountAdjustment, hints);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    return new IcebergManifestFileContentScanPrel(getCluster(), getTraitSet(), table, tableMetadata, projection,
      observedRowcountAdjustment, hints);
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
    List<SchemaPath> manifestListReaderColumns = new ArrayList<>(Arrays.asList(SchemaPath.getSimplePath(SPLIT_IDENTITY), SchemaPath.getSimplePath(SPLIT_INFORMATION), SchemaPath.getSimplePath(COL_IDS)));
    BatchSchema manifestListReaderSchema = RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;

    BatchSchema manifestFileReaderSchema = tableMetadata.getSchema();
    List<SchemaPath> manifestFileReaderColumns = new ArrayList<>();

    for (Field field:manifestFileReaderSchema.getFields()) {
      manifestFileReaderColumns.add(SchemaPath.getSimplePath(field.getName()));
    }

    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
    RelTraitSet relTraitSet = getCluster().getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);

    IcebergManifestListPrel manifestListPrel = new IcebergManifestListPrel(getCluster(), getTraitSet(), tableMetadata, manifestListReaderSchema, manifestListReaderColumns,
      getRowTypeFromProjectedColumns(manifestListReaderColumns, manifestListReaderSchema, getCluster()), null, ManifestContentType.ALL);


    // exchange above manifest list scan, which is a leaf level easy scan
    HashToRandomExchangePrel manifestSplitsExchange = new HashToRandomExchangePrel(getCluster(), relTraitSet,
      manifestListPrel, distributionTrait.getFields(), TableFunctionUtil.getHashExchangeTableFunctionCreator(tableMetadata, true));

    // Manifest scan phase
    TableFunctionConfig manifestScanTableFunctionConfig = TableFunctionUtil.getMetadataManifestScanTableFunctionConfig(
      tableMetadata, manifestFileReaderColumns, manifestFileReaderSchema, null);
    RelDataType rowTypeFromProjectedColumns = getRowTypeFromProjectedColumns(projectedColumns, manifestFileReaderSchema, getCluster());

    return new TableFunctionPrel(getCluster(), getTraitSet().plus(DistributionTrait.ANY),
      getTable(), manifestSplitsExchange, tableMetadata, manifestScanTableFunctionConfig, rowTypeFromProjectedColumns);
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }
}
