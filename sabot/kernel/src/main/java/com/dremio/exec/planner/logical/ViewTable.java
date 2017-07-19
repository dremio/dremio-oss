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
package com.dremio.exec.planner.logical;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;

import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.ViewExpansionContext;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;

public class ViewTable implements TranslatableTable, ViewInfoProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ViewTable.class);

  private final View view;
  private final String viewOwner;
  private final ViewExpansionContext viewExpansionContext;

  public ViewTable(View view, String viewOwner, ViewExpansionContext viewExpansionContext){
    this.view = view;
    this.viewOwner = viewOwner;
    this.viewExpansionContext = viewExpansionContext;
  }

  protected View getView() {
    return view;
  }

  public ViewExpansionContext getViewExpansionContext() {
    return viewExpansionContext;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return view.getRowType(typeFactory);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    ViewExpansionContext.ViewExpansionToken token = null;
    try {
      RelDataType rowType = relOptTable.getRowType();
      token = viewExpansionContext.reserveViewExpansionToken(viewOwner);
      SchemaPlus schemaPlus = token.getSchemaTree();
      return context.expandView(rowType, getView().getSql(), schemaPlus, getView().getWorkspaceSchemaPath(), null).rel;
    } finally {
      if (token != null) {
        token.release();
      }
    }
  }

  @Override
  public TableType getJdbcTableType() {
    return TableType.VIEW;
  }

  @Override
  public String getViewSql() {
    return view.getSql();
  }
}
