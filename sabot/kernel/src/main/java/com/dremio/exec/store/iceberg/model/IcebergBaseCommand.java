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
package com.dremio.exec.store.iceberg.model;

import static org.apache.iceberg.Transactions.createTableTransaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.ColumnOperations;
import com.dremio.exec.store.iceberg.FieldIdBroker;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.io.file.FileSystem;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Base Iceberg catalog
 */
public class IcebergBaseCommand implements IcebergCommand {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergBaseCommand.class);
    private static final String MANIFEST_FILE_DEFAULT_SIZE = "153600";
    private Transaction transaction;
    protected TableOperations tableOperations;
    private AppendFiles appendFiles;
    private DeleteFiles deleteFiles;
    private final Configuration configuration;
    protected final Path fsPath;
    private final FileSystem fs;
    private Snapshot currentSnapshot;

    public IcebergBaseCommand(Configuration configuration,
                                 String tableFolder,
                                 FileSystem fs,
                                 TableOperations tableOperations) {
        this.configuration = configuration;
        transaction = null;
        currentSnapshot = null;
        fsPath = new Path(tableFolder);
        this.fs = fs;
        this.tableOperations = tableOperations;
    }

    public void beginCreateTableTransaction(String tableName, BatchSchema writerSchema,
                                            List<String> partitionColumns, Map<String, String> tableParameters) {
        Preconditions.checkState(transaction == null, "Unexpected state - transaction should be null");
        Preconditions.checkNotNull(tableOperations);
        Schema schema;
        try {
            SchemaConverter schemaConverter = new SchemaConverter(tableName);
            schema = schemaConverter.toIcebergSchema(writerSchema);
        } catch (Exception ex) {
            throw UserException.validationError(ex).buildSilently();
        }
        PartitionSpec partitionSpec = IcebergUtils.getIcebergPartitionSpec(writerSchema, partitionColumns, null);
        HashMap<String, String> tableProp = new HashMap<>(tableParameters);
        tableProp.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true");
        tableProp.put(TableProperties.MANIFEST_TARGET_SIZE_BYTES,  MANIFEST_FILE_DEFAULT_SIZE);
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, partitionSpec, getTableLocation(), tableProp);

        if ( tableOperations.current() != null ) {
          throw UserException.validationError().message("A table with the given name already exists").buildSilently();
        }
        transaction = createTableTransaction(tableName, tableOperations, metadata);
        transaction.table();
    }

    @Override
    public Snapshot endCreateTableTransaction() {
      transaction.commitTransaction();
      Snapshot s = transaction.table().currentSnapshot();
      transaction = null;
      return s;
    }

    @Override
    public void beginInsertTableTransaction() {
      Preconditions.checkState(transaction == null, "Unexpected state");
      Table table = loadTable();
      transaction = table.newTransaction();
    }

    @Override
    public Snapshot endInsertTableTransaction() {
      transaction.commitTransaction();
      Snapshot snapshot = transaction.table().currentSnapshot();
      transaction = null;
      return snapshot;
    }

    @Override
    public void beginMetadataRefreshTransaction() {
      Preconditions.checkState(transaction == null, "Unexpected state");
      Table table = loadTable();
      transaction = table.newTransaction();
    }

    @Override
    public Table endMetadataRefreshTransaction() {
      transaction.commitTransaction();
      Table table = transaction.table();
      transaction = null;
      return table;
    }

    @Override
    public void beginDelete() {
      Preconditions.checkState(transaction != null, "Unexpected state");
      deleteFiles = transaction.newDelete();
    }

    @Override
    public void finishDelete() {
      deleteFiles.commit();
    }

    @Override
    public void consumeUpdatedColumns(List<Types.NestedField> columns) {
      consumeDroppedColumns(columns);
      consumeAddedColumns(columns);
    }

    @Override
    public void consumeDroppedColumns(List<Types.NestedField> columns) {
      UpdateSchema updateSchema = transaction.updateSchema();
      for (Types.NestedField col : columns) {
        updateSchema.deleteColumn(col.name());
      }
      updateSchema.commit();
    }


    @Override
    public void consumeAddedColumns(List<Types.NestedField> columns) {
      UpdateSchema updateSchema = transaction.updateSchema();
      columns.forEach(c -> updateSchema.addColumn(c.name(), c.type()));
      updateSchema.commit();
    }

    @Override
    public void beginInsert() {
        Preconditions.checkState(transaction != null, "Unexpected state");
        appendFiles = transaction.newAppend();
    }

    @Override
    public void finishInsert() {
        appendFiles.commit();
    }

    @Override
    public void consumeManifestFiles(List<ManifestFile> filesList) {
      Preconditions.checkState(transaction != null, "Transaction was not started");
      Preconditions.checkState(appendFiles != null, "Transaction was not started");
      filesList.forEach(x -> appendFiles.appendManifest(x));
    }

    @Override
    public void consumeDeleteDataFiles(List<DataFile> filesList) {
      Preconditions.checkState(transaction != null, "Transaction was not started");
      Preconditions.checkState(deleteFiles != null, "Transaction was not started");
      filesList.forEach(x -> deleteFiles.deleteFile(x.path()));
    }

    public void truncateTable() {
        Preconditions.checkState(transaction == null, "Unexpected state");
        Table table = loadTable();
        transaction = table.newTransaction();
        transaction.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
        transaction.commitTransaction();
        transaction = null;
    }

  @Override
  public void setIsReadModifyWriteTransaction(long snapshotId) {
    transaction.newOverwrite()
      .validateFromSnapshot(snapshotId)
      .validateNoConflictingData()
      .commit();
  }

  @Override
  public void addColumns(List<Types.NestedField> columnsToAdd) {
    Table table = loadTable();
    UpdateSchema updateSchema = table.updateSchema();
    columnsToAdd.stream().forEach(x -> {
      updateSchema.addColumn(x.name(), x.type());
    });
    updateSchema.commit();
  }

  public void deleteTable() {
      try {
        com.dremio.io.file.Path p = com.dremio.io.file.Path.of(fsPath.toString());
        fs.delete(p, true);
      } catch (IOException e) {
        String message = String.format("The dataset is now forgotten by dremio, but there was an error while cleaning up respective metadata files residing at %s.", fsPath.toString());
        logger.error(message);
        throw new RuntimeException(e);
      }
    }


  @Override
  public void deleteTableRootPointer() {

  }

  @Override
  public void beginAlterTableTransaction() {
    Preconditions.checkState(transaction == null, "Unexpected state");
    Table table = loadTable();
    transaction = table.newTransaction();
  }

  @Override
  public Table endAlterTableTransaction() {
    transaction.commitTransaction();
    return transaction.table();
  }

  @Override
  public void addColumnsInternalTable(List<Field> columnsToAdd) {
    UpdateSchema updateSchema = transaction.updateSchema();
    SchemaConverter schemaConverter = new SchemaConverter();
    List<Types.NestedField> icebergFields = schemaConverter.toIcebergFields(columnsToAdd);
    icebergFields.forEach(c -> updateSchema.addColumn(c.name(), c.type()));
    updateSchema.commit();
  }

  @Override
  public void dropColumnInternalTable(String columnToDrop) {
    dropColumn(columnToDrop, transaction.table(), transaction.updateSchema(), true);
  }

  public void changeColumnForInternalTable(String columnToChange, Field batchField) {
    UpdateSchema schema = transaction.updateSchema();
    dropColumn(columnToChange, transaction.table(), schema, false);
    SchemaConverter converter = new SchemaConverter();
    List<Types.NestedField> nestedFields = converter.toIcebergFields(ImmutableList.of(batchField));
    schema.addColumn(nestedFields.get(0).name(),nestedFields.get(0).type());
    schema.commit();
  }

  public void updatePropertiesMap(BatchSchema droppedColumns, BatchSchema updatedColumns) {
    UpdateProperties properties = transaction.table().updateProperties();
    com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    String droppedColumnJson = "", updateColumnJson = "";
    try {
      updateColumnJson = mapper.writeValueAsString(updatedColumns);
      droppedColumnJson = mapper.writeValueAsString(droppedColumns);
    } catch (JsonProcessingException e) {
      String error = "Unexpected error occurerd while serialising dropped and updatedColumn in json string. " + e.getMessage();
      logger.error(error);
      throw new RuntimeException(error);
    }
    properties.set(ColumnOperations.DREMIO_DROPPED_COLUMNS, droppedColumnJson);
    properties.set(ColumnOperations.DREMIO_UPDATE_COLUMNS, updateColumnJson);
    properties.commit();
  }

  public void dropColumn(String columnToDrop) {
    Table table = loadTable();
    dropColumn(columnToDrop, table, table.updateSchema(), true);
  }

  public void dropColumn(String columnToDrop, Table table, UpdateSchema updateSchema, boolean isCommit) {
    Types.NestedField columnInIceberg = table.schema().caseInsensitiveFindField(columnToDrop);
    if (!table.spec().getFieldsBySourceId(columnInIceberg.fieldId()).isEmpty()) { // column is part of partitionspec
      throw UserException.unsupportedError().message("[%s] is a partition column. Partition spec change is not supported.",
        columnInIceberg.name()).buildSilently();
    }
    updateSchema = updateSchema.deleteColumn(table.schema().findColumnName(columnInIceberg.fieldId()));
    if (isCommit) {
      updateSchema.commit();
    }
  }

  public void changeColumn(String columnToChange, Field batchField) {
    Table table = loadTable();
    UpdateSchema updateSchema = table.updateSchema();
    changeColumn(columnToChange, batchField, table, new SchemaConverter(table.name()), updateSchema, false);
    updateSchema.commit();
  }

    /**
     * TODO: currently this function is called from unit tests only. need to revisit it when we implement alter table rename column command
     * renames an existing column name.
     * @param name existing name in the table
     * @param newName new name for the column
     */
    public void renameColumn(String name, String newName) {
        Table table = loadTable();
        table.updateSchema().renameColumn(name, newName).commit();
    }

    private String sqlTypeNameWithPrecisionAndScale(org.apache.iceberg.types.Type type) {
        SchemaConverter schemaConverter = new SchemaConverter(getTableName());
        CompleteType completeType = schemaConverter.fromIcebergType(type);
        SqlTypeName calciteTypeFromMinorType = CalciteArrowHelper.getCalciteTypeFromMinorType(completeType.toMinorType());
        if (calciteTypeFromMinorType == SqlTypeName.DECIMAL) {
            return calciteTypeFromMinorType + "(" + completeType.getPrecision() + ", " + completeType.getScale() + ")";
        }
        return calciteTypeFromMinorType.toString();
    }

    public String getTableName() {
        return fsPath.getName();
    }

    public String getTableLocation() {
      return IcebergUtils.getValidIcebergPath(fsPath, configuration, DremioHadoopUtils.getHadoopFSScheme(fsPath, configuration));
    }

    @Override
    public Table loadTable() {
        Table table = new BaseTable(getTableOps(), getTableName());
        table.refresh();
        if (getTableOps().current() == null) {
            throw UserException.ioExceptionError(new IOException("Failed to load the Iceberg table. Please make sure to use correct Iceberg catalog and retry.")).buildSilently();
        }
        this.currentSnapshot = table.currentSnapshot();
        return table;
    }

  private void changeColumn(String columnToChange, Field batchField, Table table, SchemaConverter schemaConverter, UpdateSchema updateSchema, boolean isInternalField) {
    Types.NestedField columnToChangeInIceberg = table.schema().caseInsensitiveFindField(columnToChange);
    if (!table.spec().getFieldsBySourceId(columnToChangeInIceberg.fieldId()).isEmpty()) { // column is part of partitionspec
      throw UserException.unsupportedError().message("[%s] is a partition column. Partition spec change is not supported.",
        columnToChangeInIceberg.name()).buildSilently();
    }
    boolean isColumnToChangePrimitive = columnToChangeInIceberg.type().isPrimitiveType();
    boolean isNewDefComplex = batchField.getType().isComplex();
    if (isColumnToChangePrimitive && !isNewDefComplex) {
      changePrimitiveColumn(columnToChange, batchField, updateSchema, columnToChangeInIceberg, schemaConverter, table, isInternalField);
    }
    if (isColumnToChangePrimitive && isNewDefComplex) {
      throw UserException.unsupportedError().message("Cannot convert a primitive field [%s] to a complex type",
        columnToChange).buildSilently();
    }
    if (!isColumnToChangePrimitive && !isNewDefComplex) {
      throw UserException.unsupportedError().message("Cannot convert a complex field [%s] to a primitive type",
        columnToChange).buildSilently();
    }
    if (!isColumnToChangePrimitive && isNewDefComplex) {
      if ((columnToChangeInIceberg.type().isListType() && batchField.getType().getTypeID() != ArrowType.ArrowTypeID.List)
        || (columnToChangeInIceberg.type().isStructType() && batchField.getType().getTypeID() != ArrowType.ArrowTypeID.Struct)) {
        throw UserException.unsupportedError().message("Cannot convert complex field [%s] from [%s] to [%s]",
          columnToChange, columnToChangeInIceberg.type().toString(), batchField.getType().getTypeID().name()).buildSilently();
      } else {
        changeComplexColumn(columnToChangeInIceberg, batchField, schemaConverter, columnToChange, table, updateSchema, isInternalField);
      }
    }
  }

  private void changeComplexColumn(Types.NestedField currentColumn, Field newFieldDef, SchemaConverter schemaConverter, String dottedParentColumnName, Table table, UpdateSchema updateSchema, boolean isInternalField) {
    List<Types.NestedField> currentChildren;
    if (currentColumn.type().isStructType()) {
      currentChildren = new ArrayList<>(currentColumn.type().asStructType().fields());
    } else if (currentColumn.type().isListType()) {
      currentChildren = new ArrayList<>(currentColumn.type().asListType().fields());
    } else {
      throw UserException.unsupportedError().message("Cannot convert a complex field [%s] of type [%s]",
        dottedParentColumnName, currentColumn.type().toString()).buildSilently();
    }
    List<Field> newChildren = newFieldDef.getChildren();
    for (Field newChild : newChildren) {
      if (currentChildren.size() == 1 && currentChildren.get(0).name().equals("element") && newChild.getName().equalsIgnoreCase("$data$")) {
        changeColumn(dottedParentColumnName.concat(".").concat("element"), newChild, table, schemaConverter, updateSchema, true);
        currentChildren.clear();
      } else if (currentChildren.stream().anyMatch(c -> c.name().equalsIgnoreCase(newChild.getName()))) {
        changeColumn(dottedParentColumnName.concat(".").concat(newChild.getName()), newChild, table, schemaConverter, updateSchema, true);
        currentChildren.removeAll(currentChildren.stream().filter(c -> c.name().equalsIgnoreCase(newChild.getName())).collect(Collectors.toList()));
      } else {
        updateSchema
          .addColumn(
            dottedParentColumnName,
            newChild.getName(),
            schemaConverter.toIcebergType(CompleteType.fromField(newChild), dottedParentColumnName.concat(".").concat(newChild.getName()), new FieldIdBroker.UnboundedFieldIdBroker())
          );
      }
    }
    for (Types.NestedField dropChild : currentChildren) {
      dropColumn(dottedParentColumnName.concat(".").concat(dropChild.name()), table, updateSchema, false);
    }
    //Only helpful to rename the actual column in table. For fields inside a complex root column, old field is dropped and new field is added.
    if (!isInternalField && !currentColumn.name().equalsIgnoreCase(newFieldDef.getName())) {
      updateSchema.renameColumn(currentColumn.name(), newFieldDef.getName());
    }
  }

  private void changePrimitiveColumn(String columnToChange, Field batchField, UpdateSchema updateSchema, Types.NestedField columnToChangeInIceberg, SchemaConverter schemaConverter, Table table, boolean isInternalField) {
    Types.NestedField newDef = schemaConverter.changeIcebergColumn(batchField, columnToChangeInIceberg);

    if (!TypeUtil.isPromotionAllowed(columnToChangeInIceberg.type(), newDef.type()
      .asPrimitiveType())) {
      throw UserException.validationError()
        .message("Cannot change data type of column [%s] from %s to %s",
          columnToChange,
          sqlTypeNameWithPrecisionAndScale(columnToChangeInIceberg.type()),
          sqlTypeNameWithPrecisionAndScale(newDef.type()))
        .buildSilently();
    }
    if (isInternalField) {
      //We are processing a field inside a complex column.Only update is possible here. Rename happens via drop and add.
      updateSchema
        .updateColumn(table.schema().findColumnName(columnToChangeInIceberg.fieldId()), newDef.type().asPrimitiveType());
    } else {
      updateSchema
        .renameColumn(columnToChangeInIceberg.name(), newDef.name())
        .updateColumn(columnToChangeInIceberg.name(), newDef.type().asPrimitiveType());
    }
  }

  @Override
  public Snapshot getCurrentSnapshot() {
    Preconditions.checkArgument(transaction != null, "Fetching current snapshot supported only after starting a transaction");
    return this.currentSnapshot;
  }

  @Override
  public String getRootPointer() {
    TableMetadata metadata = getTableOps().current();
    if (metadata == null) {
      throw UserException.dataReadError().message("Failed to get iceberg metadata").buildSilently();
    }
    return metadata.metadataFileLocation();
  }

  @Override
  public Map<Integer, PartitionSpec> getPartitionSpecMap() {
    return getTableOps().current().specsById();
  }

  private TableOperations getTableOps() {
    return tableOperations;
  }
}
