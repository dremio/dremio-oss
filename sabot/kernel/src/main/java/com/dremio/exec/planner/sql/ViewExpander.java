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

import java.util.List;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class ViewExpander implements RelOptTable.ViewExpander {
  private static final Logger logger = LoggerFactory.getLogger(ViewExpander.class);

  private final SqlConverter parent;

  ViewExpander(final SqlConverter parent) {
    this.parent = Preconditions.checkNotNull(parent, "parent is required");
  }

  @Override
  public RelRoot expandView(
      RelDataType rowType,
      String queryString,
      List<String> schemaPath,
      List<String> viewPath) {
    final SqlConverter parser = new SqlConverter(parent, parent.getDefaultSchema(), parent.getRootSchema(),
        parent.getCatalog().withSchemaPath(schemaPath));
    return expandView(queryString, parser);
  }

  @Override
  public RelRoot expandView(
      RelDataType rowType,
      String queryString,
      SchemaPlus rootSchema, // new root schema
      List<String> schemaPath,
      List<String> viewPath) {

    final CalciteCatalogReader catalogReader = new CalciteCatalogReader(
        CalciteSchema.from(rootSchema),
        parent.getParserConfig().caseSensitive(),
        schemaPath,
        parent.getTypeFactory());

    SchemaPlus schema = rootSchema;
    for (final String path : schemaPath) {
      SchemaPlus newSchema = schema.getSubSchema(path);

      if (newSchema == null) {
        // If the view context is not found, make the root schema as the default schema. If the view is referring to
        // any tables not found according to rootSchema, a table not found exception is thrown. It is valid for a view
        // context to become invalid at some point.
        schema = rootSchema;
        break;
      }

      schema = newSchema;
    }
    final SqlConverter parser = new SqlConverter(parent, schema, rootSchema, catalogReader);
    final RelRoot expanded = expandView(queryString, parser);
    parser.getObserver().planExpandView(expanded, schemaPath, parent.getNestingLevel(), queryString);
    return expanded;
  }

  private RelRoot expandView(final String queryString, final SqlConverter converter) {
    final SqlNode parsedNode = converter.parse(queryString);
    final SqlNode validatedNode = converter.validate(parsedNode);
    final RelRoot root = converter.toConvertibleRelRoot(validatedNode, true);
    return root;
  }

  public static ViewExpander of(final SqlConverter parent) {
    return new ViewExpander(parent);
  }
}
