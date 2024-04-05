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

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.parser.SqlAccelToggle;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.accel.LayoutDefinition;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlNode;

public class AccelToggleHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AccelToggleHandler.class);

  private final Catalog catalog;
  private final AccelerationManager accel;
  private final ReflectionContext reflectionContext;
  private final OptionManager optionManager;
  private final UserSession userSession;

  public AccelToggleHandler(
      Catalog catalog, QueryContext queryContext, ReflectionContext reflectionContext) {
    this.catalog = catalog;
    this.accel = queryContext.getAccelerationManager();
    this.reflectionContext = reflectionContext;
    this.optionManager = queryContext.getOptions();
    this.userSession = queryContext.getSession();
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlAccelToggle toggle = SqlNodeUtil.unwrap(sqlNode, SqlAccelToggle.class);
    SchemaUtilities.TableWithPath tableWithPath =
        SchemaUtilities.verify(
            catalog,
            toggle.getTblName(),
            userSession,
            toggle.getSqlTableVersionSpec(),
            optionManager);

    accel.toggleAcceleration(
        tableWithPath,
        toggle.isRaw() ? LayoutDefinition.Type.RAW : LayoutDefinition.Type.AGGREGATE,
        toggle.isEnable(),
        reflectionContext);
    return Collections.singletonList(
        SimpleCommandResult.successful(
            toggle.isEnable() ? "Acceleration enabled." : "Acceleration disabled."));
  }
}
