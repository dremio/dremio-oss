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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.SqlNode;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlDropView;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableMap;

/** Handler for Drop View [If Exists] DDL command. */
public class DropViewHandler implements SqlDirectHandler<SimpleCommandResult> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropViewHandler.class);

  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public DropViewHandler(SqlHandlerConfig config) {
    this.config = config;
    this.catalog = config.getContext().getCatalog();
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlDropView dropView = SqlNodeUtil.unwrap(sqlNode, SqlDropView.class);
    NamespaceKey path = catalog.resolveSingle(dropView.getPath());
    catalog.validatePrivilege(path, SqlGrant.Privilege.ALTER);

    final DremioTable table;
    final String sourceName = path.getRoot();
    VersionContext statementSourceVersion =
      ReferenceTypeUtils.map(dropView.getRefType(), dropView.getRefValue(), null);
    final VersionContext sessionVersion = config.getContext().getSession().getSessionVersionForSource(sourceName);
    VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);
    final ResolvedVersionContext version = CatalogUtil.resolveVersionContext(catalog, sourceName, sourceVersion);

    if(isVersioned(path)) {
      final Catalog catalog = getCatalog().resolveCatalogResetContext(path.getRoot(), sourceVersion);
      final Map<String, VersionContext> contextMap = ImmutableMap.of(sourceName, sourceVersion);
      table = catalog.resolveCatalog(contextMap).getTableNoColumnCount(path);
    } else {
      table = catalog.getTableNoColumnCount(path);
    }

    if (dropView.shouldErrorIfViewDoesNotExist()) {
      if(table == null) {
        throw UserException.validationError()
          .message("Unknown view [%s].", path)
          .buildSilently();
      } else if(table.getJdbcTableType() != TableType.VIEW) {
        throw UserException.validationError()
            .message("[%s] is not a VIEW", table.getPath())
            .buildSilently();
      }
    } else if(table == null) {
      return Collections.singletonList(new SimpleCommandResult(true, String.format("View [%s] not found.", path)));
    } else if(table.getJdbcTableType() != TableType.VIEW) {
      return Collections.singletonList(new SimpleCommandResult(true, String.format("View [%s] not found.", path)));
    }
    if(isVersioned(path)){
      catalog.dropView(path, getViewOptions(version));
    } else {
      catalog.dropView(path, null);
    }
    return Collections.singletonList(SimpleCommandResult.successful("View [%s] deleted successfully.", path));
  }

  protected ViewOptions getViewOptions(ResolvedVersionContext resolvedVersionContext){
    ViewOptions viewOptions = new ViewOptions.ViewOptionsBuilder()
      .version(resolvedVersionContext)
      .build();

    return viewOptions;
  }

  protected boolean isVersioned(NamespaceKey path){
    return CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog);
  }

  @Override
  public Class<SimpleCommandResult> getResultType() {
    return SimpleCommandResult.class;
  }

  protected Catalog getCatalog() {
    return catalog;
  }

}
