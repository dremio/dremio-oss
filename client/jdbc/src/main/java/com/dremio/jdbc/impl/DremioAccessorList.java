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
package com.dremio.jdbc.impl;

import java.sql.SQLException;

import org.apache.arrow.vector.ValueVector;
import org.apache.calcite.avatica.util.Cursor.Accessor;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.vector.accessor.BoundCheckingAccessor;
import com.dremio.exec.vector.accessor.SqlAccessor;
import com.dremio.jdbc.JdbcApiSqlException;


class DremioAccessorList extends BasicList<Accessor> {

  @SuppressWarnings("unused")
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DremioAccessorList.class);
  /** "None" value for rowLastColumnOffset. */
  // (Not -1, since -1 can result from 0 (bad 1-based index) minus 1 (offset
  // from 1-based to 0-based indexing.)
  private static final int NULL_LAST_COLUMN_INDEX = -2;

  private SqlAccessorWrapper[] accessors = new SqlAccessorWrapper[0];

  /** Zero-based offset of last column referenced in current row.
   *  For {@link #wasNull()}. */
  private int rowLastColumnOffset = NULL_LAST_COLUMN_INDEX;


  /**
   * Resets last-column-referenced information for {@link #wasNull}.
   * Must be called whenever row is advanced (when {@link ResultSet#next()}
   * is called).
   */
  void clearLastColumnIndexedInRow() {
    rowLastColumnOffset = NULL_LAST_COLUMN_INDEX;
  }

  void generateAccessors(DremioCursor cursor, RecordBatchLoader currentBatch) {
    int cnt = currentBatch.getSchema().getFieldCount();
    accessors = new SqlAccessorWrapper[cnt];
    for(int i =0; i < cnt; i++){
      final ValueVector vector = currentBatch.getValueAccessorById(null, i).getValueVector();
      final SqlAccessor acc =
          new TypeConvertingSqlAccessor(
              new BoundCheckingAccessor(vector, TypeHelper.getSqlAccessor(vector))
              );
      accessors[i] = new SqlAccessorWrapper(acc, cursor);
    }
    clearLastColumnIndexedInRow();
  }

  /**
   * @param  accessorOffset  0-based index of accessor array (not 1-based SQL
   *           column index/ordinal value)
   */
  @Override
  public SqlAccessorWrapper get(final int accessorOffset) {
    final SqlAccessorWrapper accessor = accessors[accessorOffset];
    // Update lastColumnIndexedInRow after indexing accessors to not touch
    // lastColumnIndexedInRow in case of out-of-bounds exception.
    rowLastColumnOffset = accessorOffset;
    return accessor;
  }

  boolean wasNull() throws SQLException{
    if (NULL_LAST_COLUMN_INDEX == rowLastColumnOffset) {
      throw new JdbcApiSqlException(
          "ResultSet.wasNull() called without a preceding call to a column"
          + " getter method since the last call to ResultSet.next()");
    }
    return accessors[rowLastColumnOffset].wasNull();
  }

  @Override
  public int size() {
    return accessors.length;
  }

}
