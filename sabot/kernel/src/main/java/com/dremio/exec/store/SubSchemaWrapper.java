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
package com.dremio.exec.store;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang3.text.StrTokenizer;

import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.google.common.collect.ImmutableList;

public class SubSchemaWrapper extends AbstractSchema {

  private final AbstractSchema innerSchema;

  public SubSchemaWrapper(AbstractSchema innerSchema) {
    super(ImmutableList.<String>of(), toTopLevelSchemaName(innerSchema.getSchemaPath()));
    this.innerSchema = innerSchema;
  }

  @Override
  public Iterable<String> getSubPartitions(String table,
                                           List<String> partitionColumns,
                                           List<String> partitionValues
  ) throws PartitionNotFoundException {
    Schema defaultSchema = getDefaultSchema();
    if (defaultSchema instanceof AbstractSchema) {
      return ((AbstractSchema) defaultSchema).getSubPartitions(table, partitionColumns, partitionValues);
    } else {
      return Collections.EMPTY_LIST;
    }

  }


  @Override
  public SchemaMutability getMutability() {
    return innerSchema.getMutability();
  }

  @Override
  public boolean showInInformationSchema() {
    return innerSchema.showInInformationSchema();
  }

  @Override
  public Schema getDefaultSchema() {
    return innerSchema.getDefaultSchema();
  }

  @Override
  public CreateTableEntry createNewTable(
      String tableName,
      WriterOptions options,
      Map<String, Object> storageOptions) {
    return innerSchema.createNewTable(tableName, options, storageOptions);
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return innerSchema.getFunctions(name);
  }

  @Override
  public Set<String> getFunctionNames() {
    return innerSchema.getFunctionNames();
  }

  @Override
  public Schema getSubSchema(String name) {
    return innerSchema.getSubSchema(name);
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return innerSchema.getSubSchemaNames();
  }

  @Override
  public Table getTable(String name) {
    return innerSchema.getTable(name);
  }

  @Override
  public Set<String> getTableNames() {
    return innerSchema.getTableNames();
  }

  @Override
  public String getTypeName() {
    return innerSchema.getTypeName();
  }

  /**
   * Given subschema path, create a top level schema name, that can be parsed back to subschema path using
   * {@link #toSubSchemaPath(String)}
   * @param schemaPath
   * @return
   */
  public static String toTopLevelSchemaName(List<String> schemaPath) {
    final StringBuilder stringBuilder = new StringBuilder();

    boolean first = true;
    for(String schema : schemaPath) {
      if (first) {
        first = false;
      } else {
        stringBuilder.append(".");
      }

      if (schema.contains(".") || schema.contains("'")) {
        stringBuilder.append("'");
        stringBuilder.append(schema.replace("'", "''"));
        stringBuilder.append("'");
      } else {
        stringBuilder.append(schema);
      }
    }

    return stringBuilder.toString();
  }

  /**
   * Parse the top level schema name back into subschema path.
   * @param topLevelSchemaName
   * @return
   */
  public static List<String> toSubSchemaPath(String topLevelSchemaName) {
    return new StrTokenizer(topLevelSchemaName, '.', '\'')
        .setIgnoreEmptyTokens(true)
        .getTokenList();
  }
}
