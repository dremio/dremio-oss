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

import static org.apache.iceberg.Transactions.createTableTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;

/**
 * This class interacts with Iceberg libraries and helps mutation commands
 * to manage a transaction on a table. It requires configuration instance from
 * file system plugins, so that it can read and write from file system sources
 */
public class IcebergCatalog {
  private Transaction transaction;
  private AppendFiles appendFiles;
  private Configuration configuration;
  private Path fsPath;
  private Table table;

  public IcebergCatalog(String path, Configuration configuration) {
    Preconditions.checkArgument(path != null && !path.isEmpty(), "Table path must be provided");
    Preconditions.checkArgument(configuration != null, "Configuration must be provided");
    fsPath = new Path(path);
    this.configuration = configuration;
    transaction = null;
    table = null;
  }

  public static PartitionSpec getIcebergPartitionSpec(BatchSchema batchSchema,
                                                      List<String> partitionColumns) {
    SchemaConverter schemaConverter = new SchemaConverter();
    Schema schema;

    // match partition column name with name in schema
    List<String> partitionColumnsInSchemaCase = new ArrayList<>();
    if (partitionColumns != null) {
      List<String> invalidPartitionColumns = new ArrayList<>();
      for (String partitionColumn : partitionColumns) {
        Optional<Field> fieldFromSchema = batchSchema.findFieldIgnoreCase(partitionColumn);
        if (fieldFromSchema.isPresent()) {
          if (fieldFromSchema.get().getType().getTypeID() == ArrowType.ArrowTypeID.Time) {
            throw UserException.validationError().message("Partition column %s of type time is not supported", fieldFromSchema.get().getName()).buildSilently();
          }
          partitionColumnsInSchemaCase.add(fieldFromSchema.get().getName());
        } else {
          invalidPartitionColumns.add(partitionColumn);
        }
      }
      if (!invalidPartitionColumns.isEmpty()) {
        throw UserException.validationError().message("Partition column(s) %s are not found in table.", invalidPartitionColumns).buildSilently();
      }
    }

    try {
      schema = schemaConverter.toIceberg(batchSchema);
      PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
        for (String column : partitionColumnsInSchemaCase) {
          partitionSpecBuilder.identity(column);
      }
      return partitionSpecBuilder.build();
    } catch (Exception ex) {
      throw UserException.validationError(ex).buildSilently();
    }
  }

  public void beginCreateTable(BatchSchema writerSchema, List<String> partitionColumns) {
    Preconditions.checkState(transaction == null, "Unexpected state");
    IcebergTableOperations tableOperations = new IcebergTableOperations(fsPath, configuration);
    SchemaConverter schemaConverter = new SchemaConverter();
    Schema schema;
    try {
      schema = schemaConverter.toIceberg(writerSchema);
    } catch (Exception ex) {
      throw UserException.validationError(ex).buildSilently();
    }
    PartitionSpec partitionSpec = getIcebergPartitionSpec(writerSchema, partitionColumns);
    TableMetadata metadata = TableMetadata.newTableMetadata(tableOperations, schema, partitionSpec, fsPath.toString());
    transaction = createTableTransaction(tableOperations, metadata);
    table = transaction.table();
    beginInsert();
  }

  public void beginInsert() {
    Preconditions.checkState(transaction != null, "Unexpected state");
    appendFiles = transaction.newAppend();
  }

  public void endCreateTable() {
    finishInsert();
  }

  private void finishInsert() {
    appendFiles.commit();
    transaction.commitTransaction();
    transaction = null;
  }

  public void consumeData(List<DataFile> filesList) {
    Preconditions.checkState(transaction != null, "Transaction was not started");
    Preconditions.checkState(appendFiles != null, "Transaction was not started");


    filesList
      .stream()
      .forEach(x -> appendFiles.appendFile(x));

    // adds the current update to the transaction. It will be marked as
    // pending commit inside transaction. Final commit on transaction in end method
    // makes these files become part of the table

  }

  void beginInsertTable() {
    Preconditions.checkState(transaction == null, "Unexpected state");
    IcebergTableOperations tableOperations = new IcebergTableOperations(fsPath, configuration);
    table = new BaseTable(tableOperations, fsPath.getName());
    transaction = table.newTransaction();
    beginInsert();
  }

  void endInsertTable() {
    finishInsert();
  }

  void truncateTable() {
    Preconditions.checkState(transaction == null, "Unexpected state");
    IcebergTableOperations tableOperations = new IcebergTableOperations(fsPath, configuration);
    table = new BaseTable(tableOperations, fsPath.getName());
    transaction = table.newTransaction();
    transaction.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
    transaction.commitTransaction();
    transaction = null;
  }

  void addColumns(List<Types.NestedField> columnsToAdd) {
    IcebergTableOperations tableOperations = new IcebergTableOperations(fsPath, configuration);
    table = new BaseTable(tableOperations, fsPath.getName());
    UpdateSchema updateSchema = table.updateSchema();
    columnsToAdd.forEach(c -> updateSchema.addColumn(c.name(), c.type()));
    updateSchema.commit();
  }

  public void dropColumn(String columnToDrop) {
    IcebergTableOperations tableOperations = new IcebergTableOperations(fsPath, configuration);
    table = new BaseTable(tableOperations, fsPath.getName());
    Types.NestedField columnInIceberg = table.schema().caseInsensitiveFindField(columnToDrop);
    if (!table.spec().getFieldsBySourceId(columnInIceberg.fieldId()).isEmpty()) { // column is part of partitionspec
      throw UserException.unsupportedError().message("[%s] is a partition column. Partition spec change is not supported.",
          columnInIceberg.name()).buildSilently();
    }
    table.updateSchema().deleteColumn(columnInIceberg.name()).commit();
  }

  public void changeColumn(String columnToChange, Types.NestedField newDef) {
    IcebergTableOperations tableOperations = new IcebergTableOperations(fsPath, configuration);
    table = new BaseTable(tableOperations, fsPath.getName());
    Types.NestedField columnToChangeInIceberg = table.schema().caseInsensitiveFindField(columnToChange);
    if (!table.spec().getFieldsBySourceId(columnToChangeInIceberg.fieldId()).isEmpty()) { // column is part of partitionspec
      throw UserException.unsupportedError().message("[%s] is a partition column. Partition spec change is not supported.",
          columnToChangeInIceberg.name()).buildSilently();
    }

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
    IcebergTableOperations tableOperations = new IcebergTableOperations(fsPath, configuration);
    table = new BaseTable(tableOperations, fsPath.getName());
    table.updateSchema().renameColumn(name, newName).commit();
  }

  private String sqlTypeNameWithPrecisionAndScale(Type type) {
    CompleteType completeType = SchemaConverter.fromIcebergType(type);
    SqlTypeName calciteTypeFromMinorType = CalciteArrowHelper.getCalciteTypeFromMinorType(completeType.toMinorType());
    if (calciteTypeFromMinorType == SqlTypeName.DECIMAL) {
      return calciteTypeFromMinorType + "(" + completeType.getPrecision() + ", " + completeType.getScale() + ")";
    }
    return calciteTypeFromMinorType.toString();
  }

}
