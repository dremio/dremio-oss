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
package com.dremio.exec.planner.physical;

import static com.dremio.exec.planner.physical.PlannerSettings.ICEBERG_MANIFEST_SCAN_RECORDS_PER_THREAD;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.PartitionTransformTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.filter.RuntimeFilteredRel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.RowCountEstimator;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

/**
 * Prel for TableFunction operator
 */
@Options
public class TableFunctionPrel extends SinglePrel implements RuntimeFilteredRel, RowCountEstimator {
  public static final TypeValidators.LongValidator RESERVE = new TypeValidators.PositiveLongValidator("planner.op.tablefunction.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator LIMIT = new TypeValidators.PositiveLongValidator("planner.op.tablefunction.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  protected final TableMetadata tableMetadata;
  private final TableFunctionConfig functionConfig;
  private final RelOptTable table;
  private final Long survivingRecords;
  final Function<RelMetadataQuery, Double> estimateRowCountFn;
  private List<RuntimeFilteredRel.Info> runtimeFilters ;

  public TableFunctionPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RelNode child,
                           TableMetadata tableMetadata,
                           TableFunctionConfig functionConfig,
                           RelDataType rowType) {
    this(cluster, traits, table, child, tableMetadata, functionConfig, rowType,
      null, null, ImmutableList.of());
  }

  public TableFunctionPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RelNode child,
                           TableMetadata tableMetadata,
                           TableFunctionConfig functionConfig,
                           RelDataType rowType,
                           Long survivingRecords) {
    this(cluster, traits, table, child, tableMetadata, functionConfig, rowType, null, survivingRecords, ImmutableList.of());
  }

  public TableFunctionPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RelNode child,
                           TableMetadata tableMetadata,
                           TableFunctionConfig functionConfig,
                           RelDataType rowType,
                           Function<RelMetadataQuery, Double> estimateRowCountFn) {
    this(cluster, traits, table, child, tableMetadata, functionConfig, rowType,
      estimateRowCountFn, null, ImmutableList.of());
  }

  public TableFunctionPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RelNode child,
                           TableMetadata tableMetadata,
                           TableFunctionConfig functionConfig,
                           RelDataType rowType,
                           Function<RelMetadataQuery, Double> estimateRowCountFn,
                           Long survivingRecords,
                           List<RuntimeFilteredRel.Info> runtimeFilteredRels) {
    super(cluster, traits, child);
    this.tableMetadata = tableMetadata;
    this.functionConfig = functionConfig;
    this.rowType = rowType;
    this.estimateRowCountFn = estimateRowCountFn == null ?
            mq -> defaultEstimateRowCount(functionConfig, mq) : estimateRowCountFn;
    this.table = table;
    this.survivingRecords = survivingRecords;
    this.runtimeFilters = runtimeFilteredRels;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    validateSchema(childPOP.getProps().getSchema(), functionConfig.getOutputSchema());
    return new TableFunctionPOP(
      creator.props(this, tableMetadata != null ? tableMetadata.getUser() : null,
        functionConfig.getOutputSchema(), RESERVE, LIMIT),
      childPOP, functionConfig);
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TableFunctionPrel(getCluster(), traitSet, table, sole(inputs), tableMetadata,
            functionConfig, rowType, estimateRowCountFn, survivingRecords, runtimeFilters);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    if (functionConfig.getFunctionContext().getScanFilter() != null) {
      pw.item("filters", functionConfig.getFunctionContext().getScanFilter().toString());
    }
    if(functionConfig.getFunctionContext().getColumns() != null){
      pw.item("columns",
        functionConfig.getFunctionContext().getColumns().stream()
          .map(Object::toString)
          .collect(Collectors.joining(", ")));
    }
    if(!runtimeFilters.isEmpty()) {
      pw.item("runtimeFilters", runtimeFilters.stream()
        .map(Object::toString)
        .collect(Collectors.joining(", ")));
    }
    return explainTableFunction(pw);
  }

  public RelWriter explainTableFunction(RelWriter pw) {
    pw.item("Table Function Type", functionConfig.getType().name());
    switch (functionConfig.getType()) {
      case DATA_FILE_SCAN:
        pw.item("table", PathUtils.constructFullPath(functionConfig.getFunctionContext().getTablePath().get(0)));
        break;
      case SPLIT_GEN_MANIFEST_SCAN:
        Expression icebergAnyColExpression;
        try {
          icebergAnyColExpression = IcebergSerDe.deserializeFromByteArray(((ManifestScanTableFunctionContext) functionConfig.getFunctionContext()).getIcebergAnyColExpression());
        } catch (IOException e) {
          throw new RuntimeIOException(e, "failed to deserialize ManifestFile Filter AnyColExpression");
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("failed to deserialize ManifestFile Filter AnyColExpression" , e);
        }
        if(icebergAnyColExpression != null) {
          pw.item("ManifestFile Filter AnyColExpression", icebergAnyColExpression.toString());
        }
        break;
    }
    return pw;
  }

  protected double defaultEstimateRowCount(TableFunctionConfig functionConfig, RelMetadataQuery mq) {
    switch (functionConfig.getType()) {
      case FOOTER_READER:
        return DremioCost.LARGE_ROW_COUNT;
      case SPLIT_GENERATION:
        // TODO: This should estimate the number of splits that generated
        break;
      case DATA_FILE_SCAN:
        double selectivityEstimateFactor = 1.0;
        if (functionConfig.getFunctionContext().getScanFilter() != null) {
          final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(getCluster().getPlanner());
          selectivityEstimateFactor = plannerSettings.getFilterMinSelectivityEstimateFactor();
        }
        return survivingDataFileRecords() * selectivityEstimateFactor;
      case SPLIT_GEN_MANIFEST_SCAN:
      case METADATA_MANIFEST_FILE_SCAN:
        if(survivingRecords == null) {
          //In case this is not an iceberg table we should rely on old methods of estimation
          final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(getCluster().getPlanner());
          double sliceTarget = ((double)plannerSettings.getSliceTarget() /
          plannerSettings.getOptions().getOption(ICEBERG_MANIFEST_SCAN_RECORDS_PER_THREAD));
          return Math.max(mq.getRowCount(input) * sliceTarget, 1);
        }
        return (double)survivingRecords;
      case SPLIT_ASSIGNMENT:
        return mq.getRowCount(this.input);
    }

    return 1;
  }

  private long survivingDataFileRecords() {
    return survivingRecords == null ? tableMetadata.getApproximateRecordCount() : survivingRecords;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitTableFunction(this, value);
  }

  @Override
  public RelOptTable getTable() {
    return table;
  }

  public boolean isDataScan() {
    return TableFunctionConfig.FunctionType.DATA_FILE_SCAN.equals(functionConfig.getType());
  }

  public boolean hasFilter() {
    return functionConfig.getFunctionContext().getScanFilter() != null;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public TableFunctionConfig getTableFunctionConfig() {
    return functionConfig;
  }

  @Override
  public List<RuntimeFilteredRel.Info> getRuntimeFilters() {
    return runtimeFilters;
  }

  public Long getSurvivingRecords() {
    return survivingRecords;
  }

  @Override
  public void addRuntimeFilter(RuntimeFilteredRel.Info filterInfo) {
    runtimeFilters = ImmutableList.<Info>builder()
      .addAll(runtimeFilters)
      .add(filterInfo)
      .build();
    recomputeDigest();
  }

  private void validateSchema(BatchSchema inputSchema, BatchSchema tableFunctionSchema) {
    switch (functionConfig.getType()) {
      case ICEBERG_PARTITION_TRANSFORM:
        ByteString partitionSpecBytes = ((PartitionTransformTableFunctionContext) functionConfig.getFunctionContext()).getPartitionSpec();
        String icebergSchema = ((PartitionTransformTableFunctionContext) functionConfig.getFunctionContext()).getIcebergSchema();
        PartitionSpec partitionSpec = IcebergSerDe.deserializePartitionSpec(IcebergSerDe.deserializedJsonAsSchema(icebergSchema), partitionSpecBytes.toByteArray());
        SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
        Set<String> extraFields = partitionSpec.fields().stream()
          .map(IcebergUtils::getPartitionFieldName)
          .map(String::toLowerCase)
          .collect(Collectors.toSet());
        for (Field field : tableFunctionSchema) {
          if (extraFields.contains(field.getName().toLowerCase())) {
            continue;
          }
          schemaBuilder.addField(field);
        }
        BatchSchema expectedInputSchema = schemaBuilder.build();
        if (!inputSchema.equalsIgnoreCase(expectedInputSchema)) {
          throw UserException.validationError().message("Table schema %s doesn't match with query schema %s.",
            expectedInputSchema, inputSchema).buildSilently();
        }
        return;

      default:
        return;
    }
  }

  public Function<RelMetadataQuery, Double> getEstimateRowCountFn() {
    return estimateRowCountFn;
  }
}
