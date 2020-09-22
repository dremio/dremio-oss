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

package com.dremio.exec.planner.sql.handlers.direct;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.parser.SqlShowTables;
import com.dremio.service.catalog.Table;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

public class ShowTablesHandler implements SqlDirectHandler<ShowTablesHandler.ShowTableResult> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShowTablesHandler.class);

  private final Catalog catalog;

  public ShowTablesHandler(Catalog catalog){
    this.catalog = catalog;
  }

  @Override
  public List<ShowTableResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlShowTables node = SqlNodeUtil.unwrap(sqlNode, SqlShowTables.class);
    final NamespaceKey path = node.getDb() != null ? new NamespaceKey(node.getDb().names) : catalog.getDefaultSchema();

    if(path == null) {
      // If the default schema is a root schema, throw an error to select a default schema
      throw UserException.validationError()
        .message("No default schema selected. Select a schema using 'USE schema' command")
        .build(logger);
    }

    if(!catalog.containerExists(path)) {
      throw UserException.validationError()
        .message("Invalid schema %s.", path)
        .build(logger);
    }

    final Pattern likePattern = SqlNodeUtil.getPattern(node.getLikePattern());
    final Matcher m = likePattern.matcher("");

    return FluentIterable.from(catalog.listDatasets(path))
        .filter(new Predicate<Table>() {

      @Override
      public boolean apply(Table input) {
        m.reset(input.getTableName());
        return m.matches();
      }}).transform(new Function<Table, ShowTableResult>(){

      @Override
      public ShowTableResult apply(Table input) {
        return new ShowTableResult(input.getSchemaName(), input.getTableName());
      }}).toList();
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
