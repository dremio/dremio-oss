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
package com.dremio.plugins.sysflight;

import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.proto.FlightProtos.SysTableFunction;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.iceberg.SchemaConverter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.types.Types;

/** TranslatableTable implementation of reflection_lineage */
public final class ReflectionLineageTable implements TranslatableTable {
  private final String user;
  private final ManagedStoragePlugin storagePlugin;
  private static final String BATCH_NUMBER = "batch_number";
  private static final String REFLECTION_ID = "reflection_id";
  private static final String REFLECTION_NAME = "reflection_name";
  private static final String DATASET_NAME = "dataset_name";

  private static final BatchSchema RESULT_SCHEMA =
      SchemaConverter.getBuilder()
          .build()
          .fromIceberg(
              new org.apache.iceberg.Schema(
                  Types.NestedField.required(1, BATCH_NUMBER, Types.IntegerType.get()),
                  Types.NestedField.required(2, REFLECTION_ID, Types.StringType.get()),
                  Types.NestedField.required(3, REFLECTION_NAME, Types.StringType.get()),
                  Types.NestedField.required(4, DATASET_NAME, Types.StringType.get())));

  private final String reflectionId;

  private ReflectionLineageTable(
      ManagedStoragePlugin storagePlugin, String reflectionId, String user) {
    this.storagePlugin = storagePlugin;
    this.reflectionId = reflectionId;
    this.user = user;
  }

  public static ReflectionLineageTable create(
      ManagedStoragePlugin storagePlugin, String reflectionId, String user) {
    return new ReflectionLineageTable(storagePlugin, reflectionId, user);
  }

  @Override
  public RelNode toRel(ToRelContext toRelContext, RelOptTable relOptTable) {
    SysTableFunction.Builder sysTableFunctionBuilder = SysTableFunction.newBuilder();
    sysTableFunctionBuilder.setName(ReflectionLineageTableMacro.NAME);
    sysTableFunctionBuilder
        .getParametersBuilder()
        .getReflectionLineageParametersBuilder()
        .setReflectionId(reflectionId);
    SysTableFunctionCatalogMetadata sysTableFunctionCatalogMetadata =
        new SysTableFunctionCatalogMetadata(
            RESULT_SCHEMA,
            storagePlugin.getId(),
            ReflectionLineageTableMacro.REFLECTION_LINEAGE_PATH,
            sysTableFunctionBuilder.build());
    return new SysTableFunctionQueryScanCrel(
        toRelContext.getCluster(),
        toRelContext.getCluster().traitSetOf(Convention.NONE),
        getRowType(toRelContext.getCluster().getTypeFactory()),
        sysTableFunctionCatalogMetadata,
        user);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return CalciteArrowHelper.wrap(RESULT_SCHEMA)
        .toCalciteRecordType(
            typeFactory, (Field f) -> !NamespaceTable.SYSTEM_COLUMNS.contains(f.getName()), false);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(
      String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return true;
  }
}
