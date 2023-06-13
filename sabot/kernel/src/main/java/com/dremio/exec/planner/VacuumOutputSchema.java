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
package com.dremio.exec.planner;


import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;

/**
 * Static schema from VACUUM TABLE.
 */
public class VacuumOutputSchema {

  private VacuumOutputSchema() {
  }

  public static final String DELETE_DATA_FILE_COUNT = "deleted_data_files_count";
  public static final String DELETE_POSITION_DELETE_FILES_COUNT = "deleted_position_delete_files_count";
  public static final String DELETE_EQUALITY_DELETE_FILES_COUNT = "deleted_equality_delete_files_count";
  public static final String DELETE_MANIFEST_FILES_COUNT = "deleted_manifest_files_count";
  public static final String DELETE_MANIFEST_LISTS_COUNT = "deleted_manifest_lists_count";
  public static final String DELETE_PARTITION_STATS_FILES_COUNT = "deleted_partition_stats_files_count";

  public static final BatchSchema OUTPUT_SCHEMA = BatchSchema.newBuilder()
    .addField(Field.nullable(DELETE_DATA_FILE_COUNT, Types.MinorType.BIGINT.getType()))
    .addField(Field.nullable(DELETE_POSITION_DELETE_FILES_COUNT, Types.MinorType.BIGINT.getType()))
    .addField(Field.nullable(DELETE_EQUALITY_DELETE_FILES_COUNT, Types.MinorType.BIGINT.getType()))
    .addField(Field.nullable(DELETE_MANIFEST_FILES_COUNT, Types.MinorType.BIGINT.getType()))
    .addField(Field.nullable(DELETE_MANIFEST_LISTS_COUNT, Types.MinorType.BIGINT.getType()))
    .addField(Field.nullable(DELETE_PARTITION_STATS_FILES_COUNT, Types.MinorType.BIGINT.getType()))
    .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
    .build();

  public static RelDataType getRelDataType(RelDataTypeFactory typeFactory) {
    return getRowType(OUTPUT_SCHEMA, typeFactory);
  }

  public static RelDataType getRowType(BatchSchema schema, RelDataTypeFactory factory) {
    final RelDataTypeFactory.FieldInfoBuilder builder = new RelDataTypeFactory.FieldInfoBuilder(factory);
    for(Field field : schema){
      builder.add(field.getName(), CalciteArrowHelper.wrap(CompleteType.fromField(field)).toCalciteType(factory, true));
    }
    return builder.build();
  }
}
