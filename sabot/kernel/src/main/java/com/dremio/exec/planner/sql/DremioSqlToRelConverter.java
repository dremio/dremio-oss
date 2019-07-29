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
package com.dremio.exec.planner.sql;

import java.util.List;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelStructuredTypeFlattener;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.ops.ViewExpansionContext.ViewExpansionToken;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.sql.SqlConverter.RelRootPlus;
import com.dremio.service.namespace.NamespaceKey;

/**
 * An overridden implementation of SqlToRelConverter that redefines view expansion behavior.
 */
public class DremioSqlToRelConverter extends SqlToRelConverter {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioSqlToRelConverter.class);

  private final SqlConverter sqlConverter;

  public DremioSqlToRelConverter(
      SqlConverter sqlConverter,
      SqlValidator validator,
      SqlRexConvertletTable convertletTable,
      Config config) {
    super(new NoOpExpander(), validator, sqlConverter.getCatalogReader(), sqlConverter.getCluster(), convertletTable, config);
    this.sqlConverter = sqlConverter;
  }

  @Override
  public RelNode toRel(RelOptTable table) {
    return table.toRel(createToRelContext());
  }

  public ToRelContext createToRelContext() {
    return new ExtendedToRelContext(sqlConverter);
  }

  @Override
  public RelNode flattenTypes(
      RelNode rootRel,
      boolean restructure) {
    RelStructuredTypeFlattener typeFlattener =
        new RelStructuredTypeFlattener(rexBuilder, createToRelContext(), restructure);
    return typeFlattener.rewrite(rootRel);
  }


  public static RelRoot expandView(NamespaceKey path, final String viewOwner, final String queryString, final List<String> context, final SqlConverter sqlConverter) {
    ViewExpansionToken token = null;

    try {
      //RelDataType rowType = view.getRowType(cluster.getTypeFactory());
      token = sqlConverter.getViewExpansionContext().reserveViewExpansionToken(viewOwner);
      final DremioCatalogReader catalog;
      if(viewOwner != null) {
        catalog = sqlConverter.getCatalogReader().withSchemaPathAndUser(viewOwner, context);
      } else {
        catalog = sqlConverter.getCatalogReader().withSchemaPath(context);
      }
      final SqlConverter newConverter = new SqlConverter(sqlConverter, catalog);
      final SqlNode parsedNode = newConverter.parse(queryString);
      final SqlNode validatedNode = newConverter.validate(parsedNode);
      final RelRootPlus root = newConverter.toConvertibleRelRoot(validatedNode, true);
      if(path == null) {
        return root;
      }

      // we need to make sure that if a inner expansion is context sensitive, we consider the current
      // expansion context sensitive even if it isn't locally.
      final boolean contextSensitive = root.isContextSensitive() || ExpansionNode.isContextSensitive(root.rel);

      return new RelRoot(ExpansionNode.wrap(path, root.rel, root.validatedRowType, contextSensitive), root.validatedRowType, root.kind, root.fields, root.collation);

    } finally {
      if (token != null) {
        token.release();
      }
    }
  }

  static class NoOpExpander implements RelOptTable.ViewExpander {
    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
      throw new IllegalStateException("This expander should not be used.");
    }
  }

}
