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

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.model.ImmutableManifestScanOptions;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

/** Builds optimized plans for Iceberg datasets when only aggregating on partition fields. */
public class IcebergPartitionAggregationPlanBuilder extends IcebergScanPlanBuilder {

  private static final String errorTemplate = "%s field expected in RelNode: %s";
  private static final String partitionFieldSuffix = "_val";

  public IcebergPartitionAggregationPlanBuilder(final IcebergScanPrel scanPrel) {
    super(scanPrel);
  }

  public Prel buildManifestScanPlanForPartitionAggregation() {
    Preconditions.checkState(
        getIcebergScanPrel().isPartitionValuesEnabled(),
        "Cannot call this plan builder unless optimization flag is set");
    Preconditions.checkState(
        !hasDeleteFiles(), "Cannot call this plan builder if delete files are present");
    return buildPlanForSimpleAggregation();
  }

  /**
   *
   *
   * <pre>
   * Builds manifest scan plan that aggregates distinct partition column values.
   *
   * Project(partition columns)
   * |
   * Filter(record_count > 0)
   * |
   * ManifestScan(manifest fields + partition column values)
   * |
   * Exchange on split identity
   * |
   * ManifestListScan
   * </pre>
   */
  public Prel buildPlanForSimpleAggregation() {
    // Build data manifest scan
    final BatchSchema manifestScanSchema =
        BatchSchema.newBuilder()
            .addFields(SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA)
            .addField(SystemSchemas.RECORD_COUNT_FIELD)
            .addFields(buildPartitionFields())
            .build();

    final RelNode manifestScan =
        buildManifestScan(manifestScanSchema, getDataManifestRecordCount());

    // Filter out rows where recordCount = 0
    final RelNode filter = buildFilterOnRecordCount(manifestScan);

    final List<Field> projectedSchema = getIcebergScanPrel().getProjectedSchema().getFields();
    final List<RexNode> projectedColumns = new ArrayList<>(projectedSchema.size());
    final List<String> columnNames = new ArrayList<>(projectedSchema.size());

    // Project original projected fields. Remember that partition
    // field names were modified for manifest scan, so map back
    // to original names.
    projectColumnsAndNames(
        projectedColumns, columnNames, filter, name -> name + partitionFieldSuffix);

    return buildProject(filter, projectedColumns, columnNames);
  }

  private List<Field> buildPartitionFields() {
    // To output partition fields from manifest scan, we must
    // append '_val' suffix (see PathGeneratingManifestEntryProcessor).
    return getIcebergScanPrel().getProjectedSchema().getFields().stream()
        .map(
            field ->
                new Field(
                    field.getName() + partitionFieldSuffix,
                    field.getFieldType(),
                    field.getChildren()))
        .collect(Collectors.toList());
  }

  private Prel buildManifestScan(final BatchSchema schema, final long recordCount) {
    final List<SchemaPath> columns =
        schema.getFields().stream()
            .map(field -> SchemaPath.getSimplePath(field.getName()))
            .collect(Collectors.toList());

    // No need for split generation as we are only interested
    // in manifest entry partition field values.
    final ImmutableManifestScanOptions options =
        new ImmutableManifestScanOptions.Builder()
            .setIncludesSplitGen(false)
            .setManifestContentType(ManifestContentType.DATA)
            .build();

    return getIcebergScanPrel().buildManifestScan(recordCount, options, columns, schema);
  }

  private FilterPrel buildFilterOnRecordCount(final RelNode input) {
    final RelDataTypeField recordCountField =
        Preconditions.checkNotNull(
            input.getRowType().getField(SystemSchemas.RECORD_COUNT, true, false),
            errorTemplate,
            SystemSchemas.RECORD_COUNT,
            input);

    final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    final RexNode recordCountRef =
        rexBuilder.makeInputRef(recordCountField.getType(), recordCountField.getIndex());
    final RexNode zero = rexBuilder.makeZeroLiteral(recordCountField.getType());
    final RexNode filterCondition =
        rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, recordCountRef, zero);

    return FilterPrel.create(input.getCluster(), input.getTraitSet(), input, filterCondition);
  }

  private ProjectPrel buildProject(
      final RelNode input, final List<RexNode> projectedColumns, final List<String> columnNames) {
    final RexBuilder rexBuilder = input.getCluster().getRexBuilder();

    final RelDataType type =
        RexUtil.createStructType(
            rexBuilder.getTypeFactory(),
            projectedColumns,
            columnNames,
            SqlValidatorUtil.F_SUGGESTER);

    return ProjectPrel.create(
        input.getCluster(), input.getTraitSet(), input, projectedColumns, type);
  }

  private void projectColumnsAndNames(
      final List<RexNode> projectedColumns,
      final List<String> columnNames,
      final RelNode input,
      final Function<String, String> nameMapper) {
    final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    getIcebergScanPrel()
        .getProjectedSchema()
        .forEach(
            field -> {
              final String name = field.getName();
              final String mappedName = nameMapper.apply(name);
              final RelDataTypeField typeField =
                  Preconditions.checkNotNull(
                      input.getRowType().getField(mappedName, true, false),
                      errorTemplate,
                      mappedName,
                      input);
              final RexNode ref =
                  rexBuilder.makeInputRef(typeField.getType(), typeField.getIndex());
              projectedColumns.add(ref);
              columnNames.add(name);
            });
  }
}
