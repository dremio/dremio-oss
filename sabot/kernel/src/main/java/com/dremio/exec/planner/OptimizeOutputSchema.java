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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Static schema from OPTIMIZE TABLE.
 */
public final class OptimizeOutputSchema {

  private OptimizeOutputSchema() {
  }

  public static final String REWRITTEN_DATA_FILE_COUNT = "rewritten_data_files_count";
  public static final String REWRITTEN_DELETE_FILE_COUNT = "rewritten_delete_files_count";
  public static final String NEW_DATA_FILES_COUNT = "new_data_files_count";
  public static final String OPTIMIZE_OUTPUT_SUMMARY = "summary";

  public static RelDataType getRelDataType(RelDataTypeFactory typeFactory, boolean onlyOptimizeManifests) {
    if (onlyOptimizeManifests) {
      return typeFactory.builder()
        .add(OptimizeOutputSchema.OPTIMIZE_OUTPUT_SUMMARY, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
        .build();
    }

    return typeFactory.builder()
      .add(OptimizeOutputSchema.REWRITTEN_DATA_FILE_COUNT, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true))
      .add(OptimizeOutputSchema.REWRITTEN_DELETE_FILE_COUNT, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true))
      .add(OptimizeOutputSchema.NEW_DATA_FILES_COUNT, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true))
      .build();
  }

}
