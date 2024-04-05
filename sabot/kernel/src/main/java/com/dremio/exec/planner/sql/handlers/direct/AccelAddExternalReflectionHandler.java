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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.SchemaUtilities.TableWithPath;
import com.dremio.exec.planner.sql.parser.SqlAddExternalReflection;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

public class AccelAddExternalReflectionHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AccelAddExternalReflectionHandler.class);

  private final Catalog catalog;
  private final AccelerationManager accel;
  private final ReflectionContext reflectionContext;
  private UserSession userSession;
  private OptionManager optionManager;

  public AccelAddExternalReflectionHandler(
      Catalog catalog, QueryContext queryContext, ReflectionContext reflectionContext) {
    this.catalog = catalog;
    this.accel = queryContext.getAccelerationManager();
    this.reflectionContext = reflectionContext;
    this.userSession = queryContext.getSession();
    this.optionManager = queryContext.getOptions();
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlAddExternalReflection addExternalReflection =
        SqlNodeUtil.unwrap(sqlNode, SqlAddExternalReflection.class);
    final NamespaceKey queryPath =
        catalog.resolveSingle(new NamespaceKey(addExternalReflection.getTblName().names));
    if (CatalogUtil.requestedPluginSupportsVersionedTables(queryPath.getRoot(), catalog)) {
      throw UserException.unsupportedError()
          .message(
              "External reflections are not supported on versioned source %s", queryPath.getRoot())
          .build(logger);
    }
    final NamespaceKey targetPath =
        catalog.resolveSingle(new NamespaceKey(addExternalReflection.getTargetTable().names));
    if (CatalogUtil.requestedPluginSupportsVersionedTables(targetPath.getRoot(), catalog)) {
      throw UserException.unsupportedError()
          .message(
              "External reflections are not supported on versioned source %s", targetPath.getRoot())
          .build(logger);
    }
    final SqlIdentifier name = addExternalReflection.getName();
    final TableWithPath table =
        SchemaUtilities.verify(
            catalog,
            addExternalReflection.getTblName(),
            userSession,
            SqlTableVersionSpec.NOT_SPECIFIED,
            optionManager);
    final TableWithPath targetTable =
        SchemaUtilities.verify(
            catalog,
            addExternalReflection.getTargetTable(),
            userSession,
            SqlTableVersionSpec.NOT_SPECIFIED,
            optionManager);
    accel.addExternalReflection(
        name.getSimple(), table.getPath(), targetTable.getPath(), reflectionContext);
    return Collections.singletonList(SimpleCommandResult.successful("External reflection added."));
  }
}
