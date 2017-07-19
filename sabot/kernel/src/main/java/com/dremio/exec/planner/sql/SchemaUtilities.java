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
package com.dremio.exec.planner.sql;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.planner.sql.parser.SqlAddLayout.NameAndGranularity;
import com.dremio.exec.store.AbstractSchema;
import com.dremio.exec.store.dfs.SchemaMutability.MutationType;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

public class SchemaUtilities {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaUtilities.class);
  public static final Joiner SCHEMA_PATH_JOINER = Joiner.on(".").skipNulls();

  /**
   * Search and return schema with given schemaPath. First search in schema tree starting from defaultSchema,
   * if not found search starting from rootSchema. Root schema tree is derived from the defaultSchema reference.
   *
   * @param defaultSchema Reference to the default schema in complete schema tree.
   * @param schemaPath Schema path to search.
   * @return SchemaPlus object.
   */
  public static SchemaPlus findSchema(final SchemaPlus defaultSchema, final List<String> schemaPath) {
    if (schemaPath.size() == 0) {
      return defaultSchema;
    }

    SchemaPlus schema;
    if ((schema = searchSchemaTree(defaultSchema, schemaPath)) != null) {
      return schema;
    }

    SchemaPlus rootSchema = defaultSchema;
    while(rootSchema.getParentSchema() != null) {
      rootSchema = rootSchema.getParentSchema();
    }

    if (rootSchema != defaultSchema &&
        (schema = searchSchemaTree(rootSchema, schemaPath)) != null) {
      return schema;
    }

    return null;
  }

  public static String toSchemaPath(List<String> names){
    return Joiner.on('.').join(names);
  }

  /**
   * Same utility as {@link #findSchema(SchemaPlus, List)} except the search schema path given here is complete path
   * instead of list. Use "." separator to divided the schema into nested schema names.
   * @param defaultSchema
   * @param schemaPath
   * @return
   * @throws ValidationException
   */
  public static SchemaPlus findSchema(final SchemaPlus defaultSchema, final String schemaPath) {
    final List<String> schemaPathAsList = SqlUtils.parseSchemaPath(schemaPath);
    return findSchema(defaultSchema, schemaPathAsList);
  }

  /** Utility method to search for schema path starting from the given <i>schema</i> reference */
  private static SchemaPlus searchSchemaTree(SchemaPlus schema, final List<String> schemaPath) {
    for (String schemaName : schemaPath) {
      SchemaPlus oldSchema = schema;
      schema = schema.getSubSchema(schemaName);
      if (schema == null) {
        schema = oldSchema.getSubSchema(schemaName.toLowerCase());
        if(schema == null){
          return null;
        }
      }
    }
    return schema;
  }

  /**
   * Returns true if the given <i>schema</i> is root schema. False otherwise.
   * @param schema
   * @return
   */
  public static boolean isRootSchema(SchemaPlus schema) {
    return schema.getParentSchema() == null;
  }

  /**
   * Unwrap given <i>SchemaPlus</i> instance as Dremio schema instance (<i>AbstractSchema</i>). Once unwrapped, return
   * default schema from <i>AbstractSchema</i>. If the given schema is not an instance of <i>AbstractSchema</i> a
   * {@link UserException} is thrown.
   */
  public static AbstractSchema unwrapAsSchemaInstance(SchemaPlus schemaPlus)  {
    try {
      return schemaPlus.unwrap(AbstractSchema.class);
    } catch (ClassCastException e) {
      throw UserException.validationError(e)
          .message("Schema [%s] is not a Dremio schema.", getSchemaPath(schemaPlus))
          .build(logger);
    }
  }

  /** Utility method to get the schema path for given schema instance. */
  public static String getSchemaPath(SchemaPlus schema) {
    return SCHEMA_PATH_JOINER.join(getSchemaPathAsList(schema));
  }

  /** Utility method to get the schema path as list for given schema instance. */
  public static List<String> getSchemaPathAsList(SchemaPlus schema) {
    if (isRootSchema(schema)) {
      return Collections.EMPTY_LIST;
    }

    List<String> path = Lists.newArrayListWithCapacity(5);
    while(schema != null) {
      final String name = schema.getName();
      if (!Strings.isNullOrEmpty(name)) {
        path.add(schema.getName());
      }
      schema = schema.getParentSchema();
    }

    return Lists.reverse(path);
  }

  /** Utility method to throw {@link UserException} with context information */
  public static void throwSchemaNotFoundException(final SchemaPlus defaultSchema, final String givenSchemaPath) {
    throw UserException.validationError()
        .message("Schema [%s] is not valid with respect to either root schema or current default schema.",
            givenSchemaPath)
        .addContext("Current default schema: ",
            isRootSchema(defaultSchema) ? "No default schema selected" : getSchemaPath(defaultSchema))
        .build(logger);
  }

  /**
   * Given reference to default schema in schema tree, search for schema with given <i>schemaPath</i>. Once a schema is
   * found resolve it into a mutable <i>AbstractSchema</i> instance. A {@link UserException} is throws when:
   *   1. No schema for given <i>schemaPath</i> is found,
   *   2. Schema found for given <i>schemaPath</i> is a root schema
   *   3. Resolved schema is not a mutable schema.
   * @param defaultSchema
   * @param schemaPath
   * @return
   */
  public static AbstractSchema resolveToMutableSchemaInstance(final SchemaPlus defaultSchema, List<String> schemaPath, boolean isSystemUser, MutationType type) {
    final SchemaPlus schema = findSchema(defaultSchema, schemaPath);

    if (schema == null) {
      throwSchemaNotFoundException(defaultSchema, SCHEMA_PATH_JOINER.join(schemaPath));
    }

    if (isRootSchema(schema)) {
      throw UserException.parseError()
          .message("Root schema is immutable. Creating or dropping tables/views is not allowed in root schema " + schema.getName() + ". " +
              "Select a schema using 'USE schema' command.")
          .build(logger);
    }

    final AbstractSchema schemaInstance = unwrapAsSchemaInstance(schema);
    if(schemaInstance.getMutability().hasMutationCapability(type, isSystemUser)){
      return schemaInstance;
    }

    throw UserException.parseError()
        .message("Unable to create or drop tables/views. Schema [%s] is immutable for this user.", getSchemaPath(schema))
        .build(logger);
  }

  public static TableWithPath verify(final SchemaPlus defaultSchema, SqlIdentifier identifier){
    if(identifier.isSimple()){
      Table table = defaultSchema.getTable(identifier.getSimple());
      if(table != null){
        return new TableWithPath(table, qualify(defaultSchema, identifier.getSimple()));
      }
    } else {
      SchemaPlus plus = findSchema(defaultSchema, identifier.names.subList(0, identifier.names.size() - 1));
      if(plus != null){
        String tableName = identifier.names.get(identifier.names.size() - 1);

        Table table = plus.getTable(tableName);
        if(table != null){
          return new TableWithPath(table, qualify(plus, tableName));
        }
      }
    }

    throw UserException.parseError().message("Unable to find table.").build(logger);

  }

  public static List<String> qualify(SchemaPlus schema, String tableName){
    LinkedList<String> names = new LinkedList<>();
    while(schema != null && schema.getParentSchema() != null){
      names.addFirst(schema.getName());
      schema = schema.getParentSchema();
    }
    names.add(tableName);
    return names;
  }

  public static class TableWithPath {

    private final Table table;
    private final List<String> path;

    public TableWithPath(Table table, List<String> path) {
      super();
      this.table = table;
      this.path = path;
    }
    public Table getTable() {
      return table;
    }
    public List<String> getPath() {
      return path;
    }

    public List<String> qualifyColumns(List<String> strings){
      final RelDataType type = table.getRowType(new JavaTypeFactoryImpl());
      return FluentIterable.from(strings).transform(new Function<String, String>(){

        @Override
        public String apply(String input) {
          RelDataTypeField field = type.getField(input, false, false);
          if(field == null){
            throw UserException.validationError()
              .message("Unable to find field %s in table %s. Available fields were: %s.",
                  input,
                  SqlUtils.quotedCompound(path),
                  FluentIterable.from(type.getFieldNames()).transform(SqlUtils.QUOTER).join(Joiner.on(", "))
                ).build(logger);
          }

          return field.getName();
        }

      }).toList();

    }

    public List<NameAndGranularity> qualifyColumnsWithGranularity(List<NameAndGranularity> strings){
      final RelDataType type = table.getRowType(new JavaTypeFactoryImpl());
      return FluentIterable.from(strings).transform(new Function<NameAndGranularity, NameAndGranularity>(){

        @Override
        public NameAndGranularity apply(NameAndGranularity input) {
          RelDataTypeField field = type.getField(input.getName(), false, false);
          if(field == null){
            throw UserException.validationError()
              .message("Unable to find field %s in table %s. Available fields were: %s.",
                  input.getName(),
                  SqlUtils.quotedCompound(path),
                  FluentIterable.from(type.getFieldNames()).transform(SqlUtils.QUOTER).join(Joiner.on(", "))
                ).build(logger);
          }

          return new NameAndGranularity(field.getName(), input.getGranularity());
        }

      }).toList();

    }

  }
}
