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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.dremio.common.expression.SchemaPath;
import com.google.common.base.Preconditions;

public final class ColumnUtils {
  public static final SchemaPath STAR_COLUMN = SchemaPath.getSimplePath("*");

  /**
   * "System" column name used to return the file path of the data file.
   */
  public static final String FILE_PATH_COLUMN_NAME = "D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H";
  /**
   * "System" column name used to return the row index of the row in the data file.
   */
  public static final String ROW_INDEX_COLUMN_NAME = "D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X";
  /**
   *  column name used to return the row count
   */
  public static final String ROW_COUNT_COLUMN_NAME = "D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_C_O_U_N_T";

  public static final String COPY_INTO_ERROR_COLUMN_NAME = "D_R_E_M_I_O_C_O_P_Y_I_N_T_O_E_R_R_O_R";

  private static final Set<String> SYSTEM_COLUMNS = new HashSet<String>() {{
    add(FILE_PATH_COLUMN_NAME);
    add(ROW_INDEX_COLUMN_NAME);
    add(ROW_COUNT_COLUMN_NAME);
  }};

  private ColumnUtils() { }

  public static boolean isStarQuery(Collection<SchemaPath> projected) {
    if (projected == null) {
      return false;
    }
    return projected.stream().anyMatch(path -> Preconditions.checkNotNull(path, "path is required").equals(STAR_COLUMN));
  }

  public static boolean isSystemColumn(String fieldName) {
    return SYSTEM_COLUMNS.contains(fieldName);
  }
}
