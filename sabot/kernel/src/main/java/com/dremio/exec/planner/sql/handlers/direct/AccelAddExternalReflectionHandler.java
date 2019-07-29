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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.SchemaUtilities.TableWithPath;
import com.dremio.exec.planner.sql.parser.SqlAddExternalReflection;
import com.dremio.exec.store.sys.accel.AccelerationManager;

public class AccelAddExternalReflectionHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelAddExternalReflectionHandler.class);

  private final Catalog catalog;
  private final AccelerationManager accel;

  public AccelAddExternalReflectionHandler(Catalog catalog, AccelerationManager accel) {
    this.catalog = catalog;
    this.accel = accel;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlAddExternalReflection addExternalReflection = SqlNodeUtil.unwrap(sqlNode, SqlAddExternalReflection.class);
    final SqlIdentifier name = addExternalReflection.getName();
    final TableWithPath table = SchemaUtilities.verify(catalog, addExternalReflection.getTblName());
    final TableWithPath targetTable = SchemaUtilities.verify(catalog, addExternalReflection.getTargetTable());
    accel.addExternalReflection(name.getSimple(), table.getPath(), targetTable.getPath());
    return Collections.singletonList(SimpleCommandResult.successful("External reflection added."));
  }

}
