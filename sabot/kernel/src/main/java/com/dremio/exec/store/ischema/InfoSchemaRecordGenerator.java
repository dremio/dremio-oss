/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.ischema;

import static com.dremio.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_NAME;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.COLS_COL_COLUMN_NAME;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_CONNECT;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_DESCR;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.SCHS_COL_SCHEMA_NAME;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_NAME;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_SCHEMA;
import static com.dremio.exec.store.ischema.InfoSchemaConstants.TBLS_COL_TABLE_TYPE;

import java.util.List;
import java.util.Map;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import com.dremio.exec.store.AbstractSchema;
import com.dremio.exec.store.dfs.SchemaMutability.MutationType;
import com.dremio.exec.store.ischema.InfoSchemaFilter.Result;
import com.dremio.exec.store.pojo.PojoRecordReader;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Generates records for POJO RecordReader by scanning the given schema. At every level (catalog, schema, table, field),
 * level specific object is visited and decision is taken to visit the contents of the object. Object here is catalog,
 * schema, table or field.
 */
public abstract class InfoSchemaRecordGenerator<S> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaRecordGenerator.class);

  protected InfoSchemaFilter filter;
  protected final String catalogName;

  public InfoSchemaRecordGenerator(String catalogName){
    this.catalogName = catalogName;
  }

  public void setInfoSchemaFilter(InfoSchemaFilter filter) {
    this.filter = filter;
  }

  /**
   * Visit the catalog. Dremio has only one catalog.
   *
   * @return Whether to continue exploring the contents of the catalog or not. Contents are schema/schema tree.
   */
  public boolean visitCatalog() {
    return true;
  }

  /**
   * Visit the given schema.
   *
   * @param schemaName Name of the schema
   * @param schema Schema object
   * @return Whether to continue exploring the contents of the schema or not. Contents are tables within the schema.
   */
  public boolean visitSchema(String schemaName, SchemaPlus schema) {
    return true;
  }

  /**
   * Visit the given table.
   *
   * @param schemaName Name of the schema where the table is present
   * @param tableName Name of the table
   * @param table Table object
   * @return Whether to continue exploring the contents of the table or not. Contents are fields within the table.
   */
  public boolean visitTable(String schemaName, String tableName, TableInfo table) {
    return true;
  }

  /**
   * Visit the given field.
   *
   * @param schemaName Schema where the table of the field is present
   * @param tableName Table name
   * @param field Field object
   */
  public void visitField(String schemaName, String tableName, RelDataTypeField field) {
  }

  protected boolean shouldVisitCatalog() {
    if (filter == null) {
      return true;
    }

    final Map<String, String> recordValues = ImmutableMap.of(CATS_COL_CATALOG_NAME, catalogName);

    // If the filter evaluates to false then we don't need to visit the catalog.
    // For other two results (TRUE, INCONCLUSIVE) continue to visit the catalog.
    return filter.evaluate(recordValues) != Result.FALSE;
  }

  protected boolean shouldVisitSchema(String schemaName, SchemaPlus schema) {
    try {
      // if the schema path is null or empty (try for root schema)
      if (schemaName == null || schemaName.isEmpty()) {
        return false;
      }

      AbstractSchema schemaInstance = schema.unwrap(AbstractSchema.class);
      if (!schemaInstance.showInInformationSchema()) {
        return false;
      }

      if (filter == null) {
        return true;
      }

      final Map<String, String> recordValues =
        ImmutableMap.of(
          CATS_COL_CATALOG_NAME, catalogName,
          SHRD_COL_TABLE_SCHEMA, schemaName,
          SCHS_COL_SCHEMA_NAME, schemaName);

      // If the filter evaluates to false then we don't need to visit the schema.
      // For other two results (TRUE, INCONCLUSIVE) continue to visit the schema.
      return filter.evaluate(recordValues) != Result.FALSE;
    } catch(ClassCastException e) {
      // ignore and return true as this is not a Dremio schema
    }
    return true;
  }

  protected boolean shouldVisitTable(String schemaName, String tableName, TableType tableType) {
    if (filter == null) {
      return true;
    }

    final Map<String, String> recordValues =
      ImmutableMap.of(
        CATS_COL_CATALOG_NAME, catalogName,
        SHRD_COL_TABLE_SCHEMA, schemaName,
        SCHS_COL_SCHEMA_NAME, schemaName,
        SHRD_COL_TABLE_NAME, tableName,
        TBLS_COL_TABLE_TYPE, tableType.toString());

    // If the filter evaluates to false then we don't need to visit the table.
    // For other two results (TRUE, INCONCLUSIVE) continue to visit the table.
    return filter.evaluate(recordValues) != Result.FALSE;
  }

  protected boolean shouldVisitColumn(String schemaName, String tableName, String columnName) {
    if (filter == null) {
      return true;
    }

    final Map<String, String> recordValues =
      ImmutableMap.of(
        CATS_COL_CATALOG_NAME, catalogName,
        SHRD_COL_TABLE_SCHEMA, schemaName,
        SCHS_COL_SCHEMA_NAME, schemaName,
        SHRD_COL_TABLE_NAME, tableName,
        COLS_COL_COLUMN_NAME, columnName);

    // If the filter evaluates to false then we don't need to visit the column.
    // For other two results (TRUE, INCONCLUSIVE) continue to visit the column.
    return filter.evaluate(recordValues) != Result.FALSE;
  }

  public abstract PojoRecordReader<S> getRecordReader();

  /**
   * Scan the schame tree, invoking the visitor as appropriate.
   * @param  rootSchema  the given schema
   */
  public void scanSchema(SchemaPlus rootSchema) {
    if (!shouldVisitCatalog() || !visitCatalog()) {
      return;
    }

    // Visit this schema and if requested ...
    for (String subSchemaName: rootSchema.getSubSchemaNames()) {
      final SchemaPlus firstLevelSchema = rootSchema.getSubSchema(subSchemaName);

      if (shouldVisitSchema(subSchemaName, firstLevelSchema) && visitSchema(subSchemaName, firstLevelSchema)) {

        final AbstractSchema schemaInstance;
        try{
          schemaInstance = (AbstractSchema) firstLevelSchema.unwrap(AbstractSchema.class).getDefaultSchema();
        }catch(Exception ex){
          logger.warn("Failure reading schema {}. Skipping inclusion in INFORMATION_SCHEMA.", subSchemaName, ex);
          continue;
        }

        // ... do for each of the schema's tables.
        for (String tableName : firstLevelSchema.getTableNames()) {
          try {
            final TableInfo tableInfo = schemaInstance.getTableInfo(tableName);

            if (tableInfo == null) {
              // Schema may return NULL for table if the query user doesn't have permissions to load the table. Ignore such
              // tables as INFO SCHEMA is about showing tables which the user has access to query.
              continue;
            }
            final Table table;
            if (tableInfo.getTable() == null) {
              table = schemaInstance.getTable(tableName);
            } else {
              table = tableInfo.getTable();
            }
            final TableType tableType = table.getJdbcTableType();
            // Visit the table, and if requested ...
            if (shouldVisitTable(subSchemaName, tableName, tableType) && visitTable(subSchemaName, tableName, tableInfo)) {
              // ... do for each of the table's fields.
              RelDataType tableRow = table.getRowType(new JavaTypeFactoryImpl());
              for (RelDataTypeField field : tableRow.getFieldList()) {
                if (shouldVisitColumn(subSchemaName, tableName, field.getName())) {
                  visitField(subSchemaName, tableName, field);
                }
              }
            }
          } catch (Exception e) {
            Joiner joiner = Joiner.on('.');
            String path = joiner.join(joiner.join(firstLevelSchema.getTableNames()), tableName);
            logger.warn("Failure while trying to read schema for table {}. Skipping inclusion in INFORMATION_SCHEMA.", path, e);;
            continue;
          }

        }
      }
    }
  }

  public static class Catalogs extends InfoSchemaRecordGenerator<Records.Catalog> {
    List<Records.Catalog> records = ImmutableList.of();

    public Catalogs(String catalogName){
      super(catalogName);
    }

    @Override
    public PojoRecordReader<Records.Catalog> getRecordReader() {
      return new PojoRecordReader<>(Records.Catalog.class, records.iterator());
    }

    @Override
    public boolean visitCatalog() {
      records = ImmutableList.of(new Records.Catalog(catalogName, IS_CATALOG_DESCR, IS_CATALOG_CONNECT));
      return false;
    }
  }

  public static class Schemata extends InfoSchemaRecordGenerator<Records.Schema> {
    List<Records.Schema> records = Lists.newArrayList();

    public Schemata(String catalogName){
      super(catalogName);
    }

    @Override
    public PojoRecordReader<Records.Schema> getRecordReader() {
      return new PojoRecordReader<>(Records.Schema.class, records.iterator());
    }

    @Override
    public boolean visitSchema(String schemaName, SchemaPlus schema) {
      AbstractSchema as = schema.unwrap(AbstractSchema.class);
      records.add(new Records.Schema(catalogName, schemaName, "<owner>",
        as.getTypeName(), as.getMutability().hasMutationCapability(MutationType.TABLE, false)));
      return false;
    }
  }

  public static class Tables extends InfoSchemaRecordGenerator<Records.Table> {
    List<Records.Table> records = Lists.newArrayList();

    public Tables(String catalogName){
      super(catalogName);
    }

    @Override
    public PojoRecordReader<Records.Table> getRecordReader() {
      return new PojoRecordReader<>(Records.Table.class, records.iterator());
    }

    @Override
    public boolean visitTable(String schemaName, String tableName, TableInfo tableInfo) {
      Preconditions.checkNotNull(tableInfo, "Error. Table %s.%s provided is null.", schemaName, tableName);

      // skip over unknown table types
      if (tableInfo.getJdbcTableType() != null) {
        records.add(new Records.Table(catalogName, schemaName, tableName,
          tableInfo.getJdbcTableType().toString()));
      }

      return false;
    }
  }

  public static class Views extends InfoSchemaRecordGenerator<Records.View> {
    List<Records.View> records = Lists.newArrayList();

    public Views(String catalogName){
      super(catalogName);
    }

    @Override
    public PojoRecordReader<Records.View> getRecordReader() {
      return new PojoRecordReader<>(Records.View.class, records.iterator());
    }

    @Override
    public boolean visitTable(String schemaName, String tableName, TableInfo tableInfo) {
      Preconditions.checkNotNull(tableInfo, "Error. Table %s.%s provided is null.", schemaName, tableName);

      if (tableInfo.getJdbcTableType() == TableType.VIEW) {
        records.add(new Records.View(catalogName, schemaName, tableName, tableInfo.getViewSQL()));
      }
      return false;
    }
  }

  public static class Columns extends InfoSchemaRecordGenerator<Records.Column> {
    List<Records.Column> records = Lists.newArrayList();

    public Columns(String catalogName){
      super(catalogName);
    }

    @Override
    public PojoRecordReader<Records.Column> getRecordReader() {
      return new PojoRecordReader<>(Records.Column.class, records.iterator());
    }

    @Override
    public void visitField(String schemaName, String tableName, RelDataTypeField field) {
      records.add(new Records.Column(catalogName, schemaName, tableName, field));
    }
  }
}
