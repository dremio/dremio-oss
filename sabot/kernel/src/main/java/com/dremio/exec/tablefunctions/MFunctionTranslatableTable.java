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
package com.dremio.exec.tablefunctions;

import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.store.MFunctionCatalogMetadata;
import com.dremio.exec.store.NamespaceTable;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

/** A Translatable table inherits by all the metadata functions */
public abstract class MFunctionTranslatableTable implements TranslatableTable {

  protected final String user;
  protected final boolean complexTypeSupport;
  protected final MFunctionCatalogMetadata catalogMetadata;

  public MFunctionTranslatableTable(
      MFunctionCatalogMetadata catalogMetadata, String user, boolean complexTypeSupport) {
    this.user = user;
    this.complexTypeSupport = complexTypeSupport;
    this.catalogMetadata = catalogMetadata;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return CalciteArrowHelper.wrap(catalogMetadata.getBatchSchema())
        .toCalciteRecordType(
            typeFactory,
            (Field f) -> !NamespaceTable.SYSTEM_COLUMNS.contains(f.getName()),
            complexTypeSupport);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(
      String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return false;
  }
}
