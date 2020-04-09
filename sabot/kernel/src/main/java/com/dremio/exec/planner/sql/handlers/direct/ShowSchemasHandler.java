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

import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.planner.sql.parser.SqlShowSchemas;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Handles both SHOW DATABASES and SHOW SCHEMAS (synonyms).
 */
public class ShowSchemasHandler implements SqlDirectHandler<ShowSchemasHandler.SchemaResult> {

  private final EntityExplorer entityExplorer;

  public ShowSchemasHandler(EntityExplorer entityExplorer) {
    super();
    this.entityExplorer = entityExplorer;
  }

  @Override
  public List<SchemaResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlShowSchemas node = SqlNodeUtil.unwrap(sqlNode, SqlShowSchemas.class);
    final Pattern likePattern = SqlNodeUtil.getPattern(node.getLikePattern());
    final Matcher m = likePattern.matcher("");

    return FluentIterable.from(entityExplorer.listSchemas(new NamespaceKey(ImmutableList.<String>of())))
        .filter(new Predicate<String>() {
          @Override
          public boolean apply(String input) {
            m.reset(input);
            return m.matches();
          }})
        .transform(new Function<String, SchemaResult>() {
          @Override
          public SchemaResult apply(String input) {
            return new SchemaResult(input);
          }})
        .toList();
  }

  @Override
  public Class<SchemaResult> getResultType() {
    return SchemaResult.class;
  }

  public static class SchemaResult {
    public final String SCHEMA_NAME;

    public SchemaResult(String sCHEMA_NAME) {
      super();
      SCHEMA_NAME = sCHEMA_NAME;
    }

  }

}
