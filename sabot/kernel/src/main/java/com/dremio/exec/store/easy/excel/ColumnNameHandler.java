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
package com.dremio.exec.store.easy.excel;

import java.util.Map;

import org.apache.poi.hssf.util.CellReference;

import com.google.common.collect.Maps;

/**
 * Keeps track of all column names in an Excel sheet. Replaces missing columns using
 * {@link CellReference#convertNumToColString(int)} and automatically resolves duplicated column names
 */
public class ColumnNameHandler {

  /** maps each column with it's name */
  private final Map<Integer, String> headersMap = Maps.newHashMap();

  /** keeps track of column cardinality */
  private final Map<String, Integer> columnCardinality = Maps.newHashMap();

  /**
   * Retrieves the column name associated with a given column index.<br>
   * If no column name is associated, generates a new column name using
   * {@link CellReference#convertNumToColString(int)} and caches that name

   * @param colIndex 0-based column index
   * @return column name
   */
  public String getColumnName(Integer colIndex) {
    if (headersMap.containsKey(colIndex)) {
      return headersMap.get(colIndex);
    } else {
      final String columnName = CellReference.convertNumToColString(colIndex);
      return setColumnName(colIndex, columnName);
    }
  }

  /**
   * associates a column name with a given column index. If the column name is already taken
   * takes care of resolving this by adding incrementing digits to the name. The "new" name
   * will be checked again for duplication before it's saved.
   * @param colIndex 0-based column index
   * @param columnName column name
   * @return final, non duplicated, column name saved for colIndex
   */
  public String setColumnName(Integer colIndex, String columnName) {
    assert !headersMap.containsKey(colIndex);

    // check if the column name is unique
    final Integer count = columnCardinality.get(columnName);

    if (count == null) { // it's a unique column name
      columnCardinality.put(columnName, 0);
      headersMap.put(colIndex, columnName);
      return columnName;
    }

    // it's a duplicated column name
    columnCardinality.put(columnName, count + 1);
    columnName += count; // alter the column name to remove duplication
    return setColumnName(colIndex, columnName); // check if the altered named isn't also duplicated
  }

}
