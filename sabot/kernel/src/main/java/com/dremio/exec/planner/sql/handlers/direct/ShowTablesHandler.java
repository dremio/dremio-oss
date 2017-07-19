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

package com.dremio.exec.planner.sql.handlers.direct;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.parser.SqlShowTables;
import com.dremio.exec.store.AbstractSchema;
import com.google.common.base.Joiner;

public class ShowTablesHandler implements SqlDirectHandler<ShowTablesHandler.ShowTableResult> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShowTablesHandler.class);

  private final SchemaPlus defaultSchema;

  public ShowTablesHandler(SchemaPlus defaultSchema){
    this.defaultSchema = defaultSchema;
  }

  @Override
  public List<ShowTableResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlShowTables node = SqlNodeUtil.unwrap(sqlNode, SqlShowTables.class);
    final SqlIdentifier db = node.getDb();
    final SchemaPlus schema;
    final List<String> names;

    if (db == null) {
      final AbstractSchema schemaInstance = SchemaUtilities.unwrapAsSchemaInstance(defaultSchema);
      names = schemaInstance.getSchemaPath();
      schema = defaultSchema;

    } else {
      names = db.names;
      schema = SchemaUtilities.findSchema(defaultSchema, names);
      if(schema == null){
        throw UserException.validationError()
            .message("Unable to find schema [%s] requested.", SchemaUtilities.toSchemaPath(names))
            .build(logger);
      }
    }

    if (SchemaUtilities.isRootSchema(schema)) {
      // If the default schema is a root schema, throw an error to select a default schema
      throw UserException.validationError()
          .message("No default schema selected. Select a schema using 'USE schema' command")
          .build(logger);
    }

    final Pattern likePattern = SqlNodeUtil.getPattern(node.getLikePattern());
    final Matcher m = likePattern.matcher("");

    final String concatenatedSchemaName = Joiner.on('.').join(names);
    final List<ShowTableResult> results = new ArrayList<>();
    for(String table : schema.getTableNames()){
      m.reset(table);
      if(m.matches()){
        results.add(new ShowTableResult(concatenatedSchemaName, table));
      }
    }

    return results;
  }

  @Override
  public Class<ShowTableResult> getResultType() {
    return ShowTableResult.class;
  }

  public static class ShowTableResult {
    public final String TABLE_SCHEMA;
    public final String TABLE_NAME;

    public ShowTableResult(String tABLE_SCHEMA, String tABLE_NAME) {
      super();
      TABLE_SCHEMA = tABLE_SCHEMA;
      TABLE_NAME = tABLE_NAME;
    }

  }

}
