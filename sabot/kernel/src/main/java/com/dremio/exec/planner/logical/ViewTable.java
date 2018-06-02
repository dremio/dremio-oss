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
package com.dremio.exec.planner.logical;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.planner.sql.ExtendedToRelContext;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

public class ViewTable implements DremioTable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ViewTable.class);

  private final View view;
  private final String viewOwner;
  private final NamespaceKey path;
  private final DatasetConfig config;

  private BatchSchema schema;

  public ViewTable(
      NamespaceKey path,
      View view,
      String viewOwner,
      BatchSchema schema
  ) {
    this(path, view, viewOwner, null, schema);
  }

  public ViewTable(
      NamespaceKey path,
      View view,
      DatasetConfig config,
      BatchSchema schema
  ) {
    this(path, view, config.getOwner(), config, schema);
  }

  private ViewTable(
      NamespaceKey path,
      View view,
      String viewOwner,
      DatasetConfig config,
      BatchSchema schema
  ) {
    this.view = view;
    this.path = path;
    this.viewOwner = viewOwner;
    this.config = config;

    this.schema = schema;
  }

  @Override
  public NamespaceKey getPath() {
    return path;
  }

  public View getView() {
    return view;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return view.getRowType(typeFactory);
  }

  @Override
  public BatchSchema getSchema() {
    if (schema == null) {
      schema = BatchSchema.fromCalciteRowType(getRowType(JavaTypeFactoryImpl.INSTANCE));
    }
    return schema;
  }

  @Override
  public DatasetConfig getDatasetConfig() {
    return config;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public String getViewOwner() {
    return viewOwner;
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    return ((ExtendedToRelContext) context).expandView(this).rel;
  }

  @Override
  public TableType getJdbcTableType() {
    return TableType.VIEW;
  }

  @Override
  public long getVersion() {
    return -1;
  }
}
