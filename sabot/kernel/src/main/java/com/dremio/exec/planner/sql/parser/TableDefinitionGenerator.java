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
package com.dremio.exec.planner.sql.parser;

import static com.dremio.exec.calcite.SqlNodes.DREMIO_DIALECT;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.TableProperties;
import io.protostuff.ByteString;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.transforms.Transform;

/*
 * Generating the table definition
 */
public class TableDefinitionGenerator {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(TableDefinitionGenerator.class);
  private static final String SCRATCH_DIR = "$scratch";
  final DatasetConfig datasetConfig;
  final NamespaceKey resolvedPath;
  final List<RelDataTypeField> fields;
  final boolean isVersioned;
  final OptionManager optionManager;

  public TableDefinitionGenerator(
      DatasetConfig datasetConfig,
      NamespaceKey resolvedPath,
      List<RelDataTypeField> fields,
      boolean isVersioned,
      OptionManager optionManager) {
    this.datasetConfig = datasetConfig;
    this.resolvedPath = resolvedPath;
    this.fields = fields;
    this.isVersioned = isVersioned;
    this.optionManager = optionManager;
  }

  @Nullable
  public String generateTableDefinition() {
    if (!shouldReturnTableDefinition(resolvedPath)) {
      return null;
    }

    if (CollectionUtils.isEmpty(fields)) {
      throw UserException.validationError()
          .message("Table [%s] has no columns.", resolvedPath)
          .build(LOGGER);
    }

    if (datasetConfig.getPhysicalDataset() == null) {
      throw UserException.validationError()
          .message("Table at [%s] is corrupted", resolvedPath)
          .build(LOGGER);
    }

    SqlWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);
    doGenerateTableDefinition(writer);
    return writer.toString();
  }

  private boolean shouldReturnTableDefinition(NamespaceKey path) {
    String sourceName = path.getRoot();
    boolean isTableInScratchDir = sourceName.equals(SCRATCH_DIR);
    return isVersioned || isTableInScratchDir;
  }

  protected void doGenerateTableDefinition(SqlWriter writer) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    writer.literal(resolvedPath.getSchemaPath());

    generateFields(writer, fields);

    generatePartitionColumns(writer, datasetConfig);
    generateSortColumns(writer);
    generateTableProperties(writer);
  }

  private void generateFields(SqlWriter writer, List<RelDataTypeField> fields) {
    writer.keyword("(");
    generateOneField(writer, fields.get(0));
    for (int i = 1; i < fields.size(); ++i) {
      writer.keyword(",");
      generateOneField(writer, fields.get(i));
    }
    writer.keyword(")");
  }

  protected void generateOneField(SqlWriter writer, RelDataTypeField field) {
    writer.identifier(field.getName());
    RelDataType type = field.getType();
    generateOneType(writer, type);
  }

  private void generateOneType(SqlWriter writer, RelDataType type) {
    writer.keyword(type.getSqlTypeName().getName());
    if (type.isStruct()) {
      generateFields(writer, type.getFieldList());
    } else if (type.getComponentType() != null) {
      writer.keyword("(");
      generateOneType(writer, type.getComponentType());
      writer.keyword(")");
    }

    if (!type.isNullable()) {
      writer.keyword("NOT NULL");
    }
  }

  private static void generatePartitionColumns(SqlWriter writer, DatasetConfig datasetConfig) {
    List<String> partitionColumns = datasetConfig.getReadDefinition().getPartitionColumnsList();
    PartitionSpec partitionSpec =
        IcebergUtils.getCurrentPartitionSpec(datasetConfig.getPhysicalDataset());
    List<PartitionField> partitionFields = partitionSpec == null ? null : partitionSpec.fields();
    if (CollectionUtils.isEmpty(partitionFields)) {
      return;
    }

    writer.keyword("PARTITION BY");
    writer.keyword("(");
    generateOnePartitionField(writer, partitionFields.get(0), partitionColumns.get(0));
    for (int i = 1; i < partitionFields.size(); ++i) {
      writer.keyword(",");
      generateOnePartitionField(writer, partitionFields.get(i), partitionColumns.get(i));
    }
    writer.keyword(")");
  }

  private static void generateOnePartitionField(
      SqlWriter writer, PartitionField field, String fieldName) {
    Transform transform = field.transform();
    if (transform.isIdentity()) {
      writer.identifier(fieldName);
    } else {
      generateOneTransformedPartitionField(writer, transform, fieldName);
    }
  }

  private static void generateOneTransformedPartitionField(
      SqlWriter writer, Transform transform, String fieldName) {
    if (transform
        .toString()
        .toUpperCase()
        .startsWith(PartitionProtobuf.IcebergTransformType.BUCKET.toString())) {
      writer.literal(PartitionProtobuf.IcebergTransformType.BUCKET.toString().toLowerCase());
      writer.keyword("(");
      writer.print(getBucketNumber(transform.toString()));
      writer.keyword(",");
    } else if (transform
        .toString()
        .toUpperCase()
        .startsWith(PartitionProtobuf.IcebergTransformType.TRUNCATE.toString())) {
      writer.literal(PartitionProtobuf.IcebergTransformType.TRUNCATE.toString().toLowerCase());
      writer.keyword("(");
      writer.print(getTruncateWidth(transform.toString()));
      writer.keyword(",");
    } else { // all other transforms without additional param(s)
      writer.literal(transform.toString());
      writer.keyword("(");
    }
    writer.identifier(fieldName);
    writer.keyword(")");
  }

  private static int getBucketNumber(String bucket) {
    return Integer.parseInt(
        bucket.substring(
            PartitionProtobuf.IcebergTransformType.BUCKET.toString().length() + 1,
            bucket.length() - 1));
  }

  private static int getTruncateWidth(String truncate) {
    return Integer.parseInt(
        truncate.substring(
            PartitionProtobuf.IcebergTransformType.TRUNCATE.toString().length() + 1,
            truncate.length() - 1));
  }

  private void generateSortColumns(SqlWriter writer) {
    List<String> sortColumns = getSortColumnListFromIcebergMetadata();
    if (CollectionUtils.isEmpty(sortColumns)) {
      return;
    }

    writer.keyword("LOCALSORT BY");
    writer.keyword("(");
    writer.identifier(sortColumns.get(0));
    for (int i = 1; i < sortColumns.size(); ++i) {
      writer.keyword(",");
      writer.identifier(sortColumns.get(i));
    }
    writer.keyword(")");
  }

  private List<String> getSortColumnListFromIcebergMetadata() {
    IcebergMetadata icebergMetadata = datasetConfig.getPhysicalDataset().getIcebergMetadata();
    if (icebergMetadata == null || icebergMetadata.getSortOrder() == null) {
      return Collections.EMPTY_LIST;
    }
    String sortOrder = icebergMetadata.getSortOrder();

    ByteString recordSchema = datasetConfig.getRecordSchema();
    if (recordSchema == null || ArrayUtils.isEmpty(recordSchema.toByteArray())) {
      return Collections.EMPTY_LIST;
    }

    Schema icebergSchema =
        SchemaConverter.getBuilder()
            .build()
            .toIcebergSchema(BatchSchema.deserialize(recordSchema.toByteArray()));

    SortOrder deserializedSortOrder =
        IcebergSerDe.deserializeSortOrderFromJson(icebergSchema, sortOrder);
    return IcebergUtils.getColumnsFromSortOrder(deserializedSortOrder, optionManager);
  }

  private void generateTableProperties(SqlWriter writer) {
    if (!optionManager.getOption(ExecConstants.ENABLE_ICEBERG_TABLE_PROPERTIES)) {
      return;
    }

    IcebergMetadata icebergMetadata = datasetConfig.getPhysicalDataset().getIcebergMetadata();
    if (icebergMetadata == null || icebergMetadata.getTablePropertiesList() == null) {
      return;
    }

    List<TableProperties> tableProperties = icebergMetadata.getTablePropertiesList();
    if (CollectionUtils.isEmpty(tableProperties)) {
      return;
    }

    writer.keyword("TBLPROPERTIES");
    writer.keyword("(");
    generateOneTableProperty(
        writer,
        tableProperties.get(0).getTablePropertyName(),
        tableProperties.get(0).getTablePropertyValue());

    for (int i = 1; i < tableProperties.size(); ++i) {
      writer.keyword(",");
      generateOneTableProperty(
          writer,
          tableProperties.get(i).getTablePropertyName(),
          tableProperties.get(i).getTablePropertyValue());
    }
    writer.keyword(")");
  }

  private void generateOneTableProperty(
      SqlWriter writer, String tablePropertyName, String tablePropertyValue) {
    SqlLiteral.createCharString(tablePropertyName, SqlParserPos.ZERO).unparse(writer, 0, 0);
    writer.keyword("=");
    SqlLiteral.createCharString(tablePropertyValue, SqlParserPos.ZERO).unparse(writer, 0, 0);
  }
}
