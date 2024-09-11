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
package com.dremio.exec.util;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.MetadataColumns;

public final class ColumnUtils {
  public static final SchemaPath STAR_COLUMN = SchemaPath.getSimplePath("*");

  public static final List<Field> DELETE_FILE_SYSTEM_COLUMN_FIELDS =
      getSystemColumnDeleteFileFields();

  /** "System" column name used to return the file path of the data file. */
  public static final String FILE_PATH_COLUMN_NAME = "D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H";

  /** "System" column name used to return the row index of the row in the data file. */
  public static final String ROW_INDEX_COLUMN_NAME = "D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X";

  /** column name used to return the row count */
  public static final String ROW_COUNT_COLUMN_NAME = "D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_C_O_U_N_T";

  /** column name used for copy into queries with on_error 'continue'/'skip' options */
  public static final String COPY_HISTORY_COLUMN_NAME = "D_R_E_M_I_O_C_O_P_Y_H_I_S_T_O_R_Y";

  /** prefix used for renaming columns in copy into queries with inner select */
  public static final String VIRTUAL_COLUMN_PREFIX = "D_R_E_M_I_O_V_I_R_T_U_A_L_C_O_L_U_M_N_";

  public static final String FILE_PATH_COLUMN = MetadataColumns.DELETE_FILE_PATH.name();
  public static final String POS_COLUMN = MetadataColumns.DELETE_FILE_POS.name();
  public static final BatchSchema DELETE_FILE_SCHEMA =
      new BatchSchema(
          ImmutableList.of(
              Field.notNullable(FILE_PATH_COLUMN, new ArrowType.Utf8()),
              Field.notNullable(POS_COLUMN, new ArrowType.Int(64, true))));

  public static final BatchSchema DELETE_FILE_SCHEMA_INTERNAL =
      new BatchSchema(
          ImmutableList.of(
              Field.notNullable(FILE_PATH_COLUMN_NAME, new ArrowType.Utf8()),
              Field.notNullable(ROW_INDEX_COLUMN_NAME, new ArrowType.Int(64, true))));

  private static final Set<String> SYSTEM_COLUMNS =
      new HashSet<String>() {
        {
          add(FILE_PATH_COLUMN_NAME);
          add(ROW_INDEX_COLUMN_NAME);
          add(ROW_COUNT_COLUMN_NAME);
        }
      };

  public static final Set<String> DML_SYSTEM_COLUMNS =
      new HashSet<String>() {
        {
          add(FILE_PATH_COLUMN_NAME);
          add(ROW_INDEX_COLUMN_NAME);
        }
      };

  private ColumnUtils() {}

  public static boolean isStarQuery(Collection<SchemaPath> projected) {
    if (projected == null) {
      return false;
    }
    return projected.stream()
        .anyMatch(path -> Preconditions.checkNotNull(path, "path is required").equals(STAR_COLUMN));
  }

  public static List<Field> getSystemColumnDeleteFileFields() {
    List<Field> deleteFileSchemaFields = new ArrayList<>();
    deleteFileSchemaFields.add(
        new Field(
            FILE_PATH_COLUMN_NAME,
            FieldType.nullable(new ArrowType.Utf8()),
            Collections.unmodifiableList(Collections.EMPTY_LIST)));
    deleteFileSchemaFields.add(
        new Field(
            ROW_INDEX_COLUMN_NAME,
            FieldType.nullable(new ArrowType.Int(64, true)),
            Collections.unmodifiableList(Collections.EMPTY_LIST)));

    return Collections.unmodifiableList(deleteFileSchemaFields);
  }

  public static boolean isSystemColumn(String fieldName) {
    return SYSTEM_COLUMNS.contains(fieldName);
  }

  public static boolean isDmlSystemColumn(String fieldName) {
    return DML_SYSTEM_COLUMNS.contains(fieldName);
  }
}
