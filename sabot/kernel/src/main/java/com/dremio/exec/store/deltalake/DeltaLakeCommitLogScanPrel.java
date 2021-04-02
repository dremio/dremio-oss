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

import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_REMOVE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_DATA_CHANGE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_DELETION_TIMESTAMP;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_KEY;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_KEY_VALUE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_MAX_VALUES;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_MIN_VALUES;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_MODIFICATION_TIME;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_NULL_COUNT;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_NUM_RECORDS;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES_PARSED;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PATH;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_SIZE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STATS;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STATS_PARSED;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_TAGS;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_VALUE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.TableMetadata;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * DeltaLake commit log reader prel for added and removed paths
 */
@Options
public class DeltaLakeCommitLogScanPrel extends AbstractRelNode implements LeafPrel {
  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.scan.deltalake.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.scan.deltalake.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  private static final Logger logger = LoggerFactory.getLogger(DeltaLakeCommitLogScanPrel.class);

  private final TableMetadata tableMetadata;
  private final boolean arrowCachingEnabled;
  private final boolean scanForAddedPaths;
  private final List<SchemaPath> columns;
  private final BatchSchema deltaCommitLogSchema;

  public DeltaLakeCommitLogScanPrel(RelOptCluster cluster, RelTraitSet traitSet, TableMetadata tableMetadata,
                                    boolean arrowCachingEnabled, boolean scanForAddedPaths) {
    super(cluster, traitSet);
    Preconditions.checkNotNull(tableMetadata);
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.scanForAddedPaths = scanForAddedPaths;
    this.tableMetadata = DeltaLakeScanTableMetadata.createWithTableMetadata(tableMetadata, scanForAddedPaths);
    final List<String> partitionCols = Optional.ofNullable(tableMetadata.getReadDefinition().getPartitionColumnsList()).orElse(Collections.EMPTY_LIST);
    this.deltaCommitLogSchema = getCommitLogSchema(tableMetadata, partitionCols);
    this.columns = getPathScanColumns(partitionCols);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    if (scanForAddedPaths && tableMetadata.getReadDefinition().getManifestScanStats() != null) {
      return tableMetadata.getReadDefinition().getManifestScanStats().getRecordCount();
    } else if (!scanForAddedPaths) {
      return estimateRowCountForRemovedPath();
    }
    return tableMetadata.getSplitCount();
  }

  private long estimateRowCountForRemovedPath() {
    try {
      DeltaLakeProtobuf.DeltaLakeDatasetXAttr xAttrs = DeltaLakeProtobuf.DeltaLakeDatasetXAttr
              .parseFrom(tableMetadata.getReadDefinition().getExtendedProperty().toByteArray());
      return xAttrs.getNumCommitJsonDataFileCount();
    } catch (InvalidProtocolBufferException e) {
      logger.error("Error while estimating dataset size", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  @Nonnull
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new DeltaLakeGroupScan(
      creator.props(this, tableMetadata.getUser(), deltaCommitLogSchema, RESERVE, LIMIT),
      tableMetadata,
      deltaCommitLogSchema,
      columns,
      scanForAddedPaths);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DeltaLakeCommitLogScanPrel(getCluster(), getTraitSet(), tableMetadata, arrowCachingEnabled, scanForAddedPaths);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitLeaf(this, value);
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
    return false;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return tableMetadata.getSplitCount();
  }

  @Override
  public int getMinParallelizationWidth() {
    return 1;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.NONE;
  }

  @Override
  public RelDataType deriveRowType() {
    final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    final List<String> names = new ArrayList<>();
    final List<RelDataType> typeList = new ArrayList<>();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();

    if (scanForAddedPaths) {
      // Schema for DeltaLakeScan for added paths
      typeList.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
      typeList.add(typeFactory.createSqlType(SqlTypeName.BIGINT));
      typeList.add(typeFactory.createSqlType(SqlTypeName.BIGINT));
      names.add(SCHEMA_PATH);
      names.add(SCHEMA_SIZE);
      names.add(SCHEMA_MODIFICATION_TIME);
      builder.add(new RelDataTypeFieldImpl(DELTA_FIELD_ADD, 0, typeFactory.createStructType(typeList, names)));

      // Add all partition col fields
      List<String> partitionCols = tableMetadata.getReadDefinition().getPartitionColumnsList();
      if (partitionCols != null) {
        Set<String> partitionColsSet = new HashSet<>(partitionCols);
        AtomicInteger i = new AtomicInteger(1);
        deltaCommitLogSchema.getFields().stream()
          .filter(f -> partitionColsSet.contains(f.getName()))
          .forEach(field -> builder.add(new RelDataTypeFieldImpl(field.getName(), i.getAndIncrement(),
            CalciteArrowHelper.toCalciteType(field, typeFactory, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal()))));
      }
    } else {
      // Schema for DeltaLakeScan for removed paths
      typeList.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
      names.add(SCHEMA_PATH);

      builder.add(new RelDataTypeFieldImpl(DELTA_FIELD_REMOVE, 0, typeFactory.createStructType(typeList, names)));
    }
    return builder.build();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    pw.item("table", tableMetadata.getName());
    pw.item("columns", columns.stream().map(SchemaPath::toString).collect(Collectors.joining(",")));
    return pw;
  }

  private BatchSchema getCommitLogSchema(TableMetadata dataset, List<String> partitionColumnsList) {
    final BatchSchema dataSchema = BatchSchema.deserialize(dataset.getDatasetConfig().getRecordSchema());
    final SchemaBuilder outputSchema = BatchSchema.newBuilder();

    final Field path = Field.nullablePrimitive(SCHEMA_PATH, new ArrowType.PrimitiveType.Utf8());
    final Field size = Field.nullablePrimitive(SCHEMA_SIZE, new ArrowType.PrimitiveType.Int(64, true));
    final Field modificationTime = Field.nullablePrimitive(SCHEMA_MODIFICATION_TIME,
      new ArrowType.PrimitiveType.Int(64, true));
    final Field dataChange = Field.nullablePrimitive(SCHEMA_DATA_CHANGE, new ArrowType.Bool());

    final Field tagsKey = Field.nullablePrimitive(SCHEMA_KEY, new ArrowType.Utf8());
    final Field tagsVal = Field.nullablePrimitive(SCHEMA_VALUE, new ArrowType.Utf8());
    final Field tagsEntry = new Field("$data$", FieldType.nullable(new ArrowType.Struct()), ImmutableList.of(tagsKey, tagsVal));
    final Field tagsKeyVal = new Field(SCHEMA_KEY_VALUE, FieldType.nullable(new ArrowType.List()), ImmutableList.of(tagsEntry));
    final Field tags = new Field(SCHEMA_TAGS, FieldType.nullable(new ArrowType.Struct()), ImmutableList.of(tagsKeyVal)); // Map type is currently not supported

    final Field stats = Field.nullablePrimitive(SCHEMA_STATS, new ArrowType.Utf8());

    final List<Field> partitionColFields = new ArrayList<>(partitionColumnsList.size());
    final List<Field> minValueFields = new ArrayList<>(dataSchema.getFieldCount());
    final List<Field> maxValueFields = new ArrayList<>(dataSchema.getFieldCount());
    final List<Field> nullCountFields = new ArrayList<>(dataSchema.getFieldCount());

    for (Field field : dataSchema.getFields()) {
      if (partitionColumnsList.contains(field.getName())) {
        partitionColFields.add(field);
      } else {
        minValueFields.add(field);
        maxValueFields.add(field);
        nullCountFields.add(field);
      }
    }

    final Field partitionValuesParsed = new Field(SCHEMA_PARTITION_VALUES_PARSED, FieldType.nullable(new ArrowType.Struct()), partitionColFields);
    final Field partitionValues = new Field(SCHEMA_PARTITION_VALUES, FieldType.nullable(new ArrowType.Struct()),
        partitionColFields.stream()
          .map(f -> Field.nullable(f.getName(), new ArrowType.Utf8())) // partition values in json log are strings
          .collect(Collectors.toList())
      );

    final Field minValues = new Field(SCHEMA_MIN_VALUES, FieldType.nullable(new ArrowType.Struct()), minValueFields);
    final Field maxValues = new Field(SCHEMA_MAX_VALUES, FieldType.nullable(new ArrowType.Struct()), maxValueFields);
    final Field nullCount = new Field(SCHEMA_NULL_COUNT, FieldType.nullable(new ArrowType.Struct()), nullCountFields);
    final Field numRecords = Field.nullablePrimitive(SCHEMA_NUM_RECORDS, new ArrowType.PrimitiveType.Int(64, true));
    final Field statsParsed = new Field(SCHEMA_STATS_PARSED, FieldType.nullable(new ArrowType.Struct()),
      ImmutableList.of(numRecords, minValues, maxValues, nullCount));

    final Field add = new Field(DELTA_FIELD_ADD, true, new ArrowType.Struct(),
      ImmutableList.of(path, partitionValues, size, modificationTime, dataChange, tags, stats, partitionValuesParsed, statsParsed));
    outputSchema.addField(add);

    final Field removePath = Field.nullablePrimitive(SCHEMA_PATH, new ArrowType.PrimitiveType.Utf8());
    final Field deletionTimestamp = Field.nullablePrimitive(SCHEMA_DELETION_TIMESTAMP, new ArrowType.PrimitiveType.Int(64, true));
    final Field removeDataChange = Field.nullablePrimitive(SCHEMA_DATA_CHANGE, new ArrowType.Bool());

    final Field remove = new Field(DELTA_FIELD_REMOVE, true, new ArrowType.Struct(), ImmutableList.of(removePath, deletionTimestamp, removeDataChange));
    outputSchema.addField(remove);

    // Partition cols to be populated explicitly
    outputSchema.addFields(partitionValuesParsed.getChildren());
    return outputSchema.build();
  }

  private List<SchemaPath> getPathScanColumns(List<String> partitionCols) {
    if (scanForAddedPaths) {
      final List<SchemaPath> cols = new ArrayList<>();
      cols.add(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PATH));
      cols.add(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_SIZE));
      cols.add(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_MODIFICATION_TIME));
      cols.add(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES_PARSED));
      cols.add(SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES));

      if (partitionCols != null) {
        Set<String> partitionColsSet = new HashSet<>(partitionCols);
        // Add all partition col fields
        deltaCommitLogSchema.getFields().stream()
          .filter(f -> partitionColsSet.contains(f.getName()))
          .forEach(f -> cols.add(SchemaPath.getSimplePath(f.getName())));
      }
      return cols;
    } else {
      // Not projecting complete `remove` so RecordReader can adjust when internal structure has differences.
      return Collections.singletonList(SchemaPath.getCompoundPath(DELTA_FIELD_REMOVE, SCHEMA_PATH));
    }
  }
}
