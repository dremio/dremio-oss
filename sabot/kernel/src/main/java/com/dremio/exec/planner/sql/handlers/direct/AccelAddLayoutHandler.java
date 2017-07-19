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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.SchemaUtilities.TableWithPath;
import com.dremio.exec.planner.sql.parser.SqlAddLayout;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.accel.LayoutDefinition;

public class AccelAddLayoutHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelAddLayoutHandler.class);

  private final SchemaPlus defaultSchema;
  private final AccelerationManager accel;

  public AccelAddLayoutHandler(SchemaPlus defaultSchema, AccelerationManager accel) {
    this.defaultSchema = defaultSchema;
    this.accel = accel;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlAddLayout addLayout = SqlNodeUtil.unwrap(sqlNode, SqlAddLayout.class);
    final TableWithPath table = SchemaUtilities.verify(defaultSchema, addLayout.getTblName());
    final LayoutDefinition layout = new LayoutDefinition(
        addLayout.isRaw() ? LayoutDefinition.Type.RAW : LayoutDefinition.Type.AGGREGATE,
            table.qualifyColumns(addLayout.getDisplayList()),
            table.qualifyColumnsWithGranularity(addLayout.getDimensionList()),
            table.qualifyColumns(addLayout.getMeasureList()),
            table.qualifyColumns(addLayout.getSortList()),
            table.qualifyColumns(addLayout.getDistributionList()),
            table.qualifyColumns(addLayout.getPartitionList())
        );
    accel.addLayout(table.getPath(), layout);
    return Collections.singletonList(SimpleCommandResult.successful("Layout added."));
  }

}
