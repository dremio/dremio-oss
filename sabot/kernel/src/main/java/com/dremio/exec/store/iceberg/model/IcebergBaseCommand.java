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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

/**
 * Base Iceberg catalog
 */
public abstract class IcebergBaseCommand implements IcebergCommand {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergBaseCommand.class);
    private static final String MANIFEST_FILE_DEFAULT_SIZE = "153600";
    private Transaction transaction;
    private TableOperations tableOperations;
    private AppendFiles appendFiles;
    private DeleteFiles deleteFiles;
    protected final Configuration configuration;
    protected final Path fsPath;
    protected final FileSystem fs;
    protected final OperatorContext context;
    protected final List<String> dataset;

    protected IcebergBaseCommand(Configuration configuration, String tableFolder, FileSystem fs, OperatorContext context, List<String> dataset) {
        this.configuration = configuration;
        transaction = null;
        fsPath = new Path(tableFolder);
        this.fs = fs;
        this.context = context;
        this.dataset = dataset;
    }

    protected abstract TableOperations getTableOperations();
    protected  abstract void deleteRootPointerStoreKey();

    public void beginCreateTableTransaction(String tableName, BatchSchema writerSchema,
                                            List<String> partitionColumns, Map<String, String> tableParameters) {
        Preconditions.checkState(transaction == null, "Unexpected state");
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
        transaction = createTableTransaction(tableName, getTableOps(), metadata);
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
    public Snapshot endMetadataRefreshTransaction() {
      transaction.commitTransaction();
      Snapshot snapshot = transaction.table().currentSnapshot();
      transaction = null;
      return snapshot;
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

    public void deleteTable() {
      try {
        com.dremio.io.file.Path p = com.dremio.io.file.Path.of(fsPath.toString());
        fs.delete(p, true);
        deleteRootPointerStoreKey();
      } catch (IOException e) {
        String message = String.format("The dataset is now forgotten by dremio, but there was an error while cleaning up respective metadata files residing at %s.", fsPath.toString());
        logger.error(message);
        throw new RuntimeException(e);
      }
    }

    public void addColumns(List<Types.NestedField> columnsToAdd) {
        Table table = loadTable();
        UpdateSchema updateSchema = table.updateSchema();
        columnsToAdd.forEach(c -> updateSchema.addColumn(c.name(), c.type()));
        updateSchema.commit();
    }

    public void dropColumn(String columnToDrop) {
        Table table = loadTable();
        Types.NestedField columnInIceberg = table.schema().caseInsensitiveFindField(columnToDrop);
        if (!table.spec().getFieldsBySourceId(columnInIceberg.fieldId()).isEmpty()) { // column is part of partitionspec
            throw UserException.unsupportedError().message("[%s] is a partition column. Partition spec change is not supported.",
                    columnInIceberg.name()).buildSilently();
        }
        table.updateSchema().deleteColumn(columnInIceberg.name()).commit();
    }

    public void changeColumn(String columnToChange, Field batchField) {
        Table table = loadTable();
        Types.NestedField columnToChangeInIceberg = table.schema().caseInsensitiveFindField(columnToChange);
        if (!table.spec().getFieldsBySourceId(columnToChangeInIceberg.fieldId()).isEmpty()) { // column is part of partitionspec
            throw UserException.unsupportedError().message("[%s] is a partition column. Partition spec change is not supported.",
                    columnToChangeInIceberg.name()).buildSilently();
        }

        SchemaConverter schemaConverter = new SchemaConverter(table.name());
        Types.NestedField newDef = schemaConverter.changeIcebergColumn(batchField, columnToChangeInIceberg);

        if (!TypeUtil.isPromotionAllowed(columnToChangeInIceberg.type(), newDef.type()
                .asPrimitiveType())) {
            throw UserException.validationError()
                    .message("Cannot change data type of column [%s] from %s to %s",
                            columnToChangeInIceberg.name(),
                            sqlTypeNameWithPrecisionAndScale(columnToChangeInIceberg.type()),
                            sqlTypeNameWithPrecisionAndScale(newDef.type()))
                    .buildSilently();
        }

        table.updateSchema()
                .renameColumn(columnToChangeInIceberg.name(), newDef.name())
                .updateColumn(columnToChangeInIceberg.name(), newDef.type().asPrimitiveType())
                .commit();
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
        return table;
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
        if (tableOperations == null) {
            tableOperations = getTableOperations();
        }
        return tableOperations;
    }
}
