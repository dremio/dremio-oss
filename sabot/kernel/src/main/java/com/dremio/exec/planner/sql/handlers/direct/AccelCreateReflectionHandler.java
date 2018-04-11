/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.concurrent.ThreadLocalRandom;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.SchemaUtilities.TableWithPath;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.accel.LayoutDefinition;

public class AccelCreateReflectionHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelCreateReflectionHandler.class);

  private final Catalog catalog;
  private final AccelerationManager accel;

  public AccelCreateReflectionHandler(Catalog catalog, AccelerationManager accel) {
    this.catalog = catalog;
    this.accel = accel;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlCreateReflection addLayout = SqlNodeUtil.unwrap(sqlNode, SqlCreateReflection.class);
    final TableWithPath table = SchemaUtilities.verify(catalog, addLayout.getTblName());
    SqlIdentifier identifier = addLayout.getName();
    String name = null;
    if(identifier != null) {
      name = identifier.toString();
    } else {
      name = "Unnamed-" + ThreadLocalRandom.current().nextLong();
    }

    final LayoutDefinition layout = new LayoutDefinition(name,
        addLayout.isRaw() ? LayoutDefinition.Type.RAW : LayoutDefinition.Type.AGGREGATE,
        table.qualifyColumns(addLayout.getDisplayList()),
        table.qualifyColumnsWithGranularity(addLayout.getDimensionList()),
        table.qualifyColumns(addLayout.getMeasureList()),
        table.qualifyColumns(addLayout.getSortList()),
        table.qualifyColumns(addLayout.getDistributionList()),
        table.qualifyColumns(addLayout.getPartitionList()),
        addLayout.getPartitionDistributionStrategy()
    );
    accel.addLayout(table.getPath(), layout);
    return Collections.singletonList(SimpleCommandResult.successful("Layout added."));
  }

}
