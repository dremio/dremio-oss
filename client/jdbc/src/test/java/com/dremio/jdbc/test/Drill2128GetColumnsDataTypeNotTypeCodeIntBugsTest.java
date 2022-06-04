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
package com.dremio.jdbc.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.jdbc.JdbcWithServerTestBase;

/**
 * Basic (spot-check/incomplete) tests for DRILL-2128 bugs (many DatabaseMetaData.getColumns(...)
 * result table problems).
 */
public class Drill2128GetColumnsDataTypeNotTypeCodeIntBugsTest extends JdbcWithServerTestBase {

  private static DatabaseMetaData dbMetadata;

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(120, TimeUnit.SECONDS);

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    JdbcWithServerTestBase.setUpConnection();
    dbMetadata = getConnection().getMetaData();
  }

  /**
   * Basic test that column DATA_TYPE is integer type codes (not strings such as "VARCHAR" or
   * "INTEGER").
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testColumn_DATA_TYPE_isInteger() throws Exception {
    // Get metadata for some column(s).
    final ResultSet columns = dbMetadata.getColumns(null, null, null, null);
    assertThat(columns.next()).isTrue();

    do {
      // DATA_TYPE should be INTEGER, so getInt( "DATA_TYPE" ) should succeed:
      final int typeCode1 = columns.getInt("DATA_TYPE");

      // DATA_TYPE should be at ordinal position 5 (seemingly):
      assertThat(columns.getMetaData().getColumnLabel(5)).isEqualTo("DATA_TYPE");

      // Also, getInt( 5 ) should succeed and return the same type code as above:
      final int typeCode2 = columns.getInt(5);
      assertThat(typeCode2).isEqualTo(typeCode1);

      // Type code should be one of java.sql.Types.*:
      List<Integer> typeCodes = Arrays.asList(Types.ARRAY,
        Types.BIGINT,
        Types.BINARY,
        Types.BIT,
        Types.BLOB,
        Types.BOOLEAN,
        Types.CHAR,
        Types.CLOB,
        Types.DATALINK,
        Types.DATE,
        Types.DECIMAL,
        Types.DISTINCT,
        Types.DOUBLE,
        Types.FLOAT,
        Types.INTEGER,
        Types.JAVA_OBJECT,
        Types.LONGNVARCHAR,
        Types.LONGVARBINARY,
        Types.LONGVARCHAR,
        Types.NCHAR,
        Types.NCLOB,
        // TODO:  Resolve:  Is it not clear whether Types.NULL can re-
        // present a type (e.g., the type of NULL), or whether a column
        // can ever have that type, and therefore whether Types.NULL
        // can appear.  Currently, exclude NULL so we'll notice if it
        // does appear:
        // NoTypes.NULL.
        Types.NUMERIC,
        Types.NVARCHAR,
        Types.OTHER,
        Types.REAL,
        Types.REF,
        Types.ROWID,
        Types.SMALLINT,
        Types.SQLXML,
        Types.STRUCT,
        Types.TIME,
        Types.TIMESTAMP,
        Types.TINYINT,
        Types.VARBINARY,
        Types.VARCHAR);
      assertThat(typeCodes).contains(typeCode1);
    } while (columns.next());
  }

  /**
   * Basic test that column TYPE_NAME exists and is strings (such "INTEGER").
   */
  @Test
  public void testColumn_TYPE_NAME_isString() throws Exception {
    // Get metadata for some INTEGER column.
    final ResultSet columns =
      dbMetadata.getColumns(null, "INFORMATION_SCHEMA", "COLUMNS",
        "ORDINAL_POSITION");
    assertThat(columns.next()).isTrue();

    // TYPE_NAME should be character string for type name "INTEGER", so
    // getString( "TYPE_NAME" ) should succeed and getInt( "TYPE_NAME" ) should
    // fail:
    final String typeName1 = columns.getString("TYPE_NAME");
    assertThat(typeName1).isEqualTo("INTEGER");

    assertThatThrownBy(() -> columns.getInt("TYPE_NAME"))
      .isInstanceOf(SQLException.class);

    // TYPE_NAME should be at ordinal position 6 (seemingly):
    assertThat(columns.getMetaData().getColumnLabel(6)).isEqualTo("TYPE_NAME");

    // Also, getString( 6 ) should succeed and return the same type name as above:
    final String typeName2 = columns.getString(6);
    assertThat(typeName2).isEqualTo(typeName1);
  }
}
