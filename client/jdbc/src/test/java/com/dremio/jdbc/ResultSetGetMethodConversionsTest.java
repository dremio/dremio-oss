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
package com.dremio.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

// TODO:  Resolve requirements:
// - For SMALLINT retrieved with getObject(), is result required (by user
//   expectations, from behavior of other JDBC drivers) to be a Short?  Can
//   it be a smaller type (e.g., Byte, if the value fits)?  Can it be a larger
//    type (e.g., Long)?
// - For INTEGER retrieved by getShort(...):
//   - Is that always an error, or is it allowed as least sometimes?
//   - For INTEGER, does getShort(...) return value if value fits in short?
//   - For INTEGER, if value does not fit in short, does getShort(...) throw
//     exception, or return something.  If it returns:
//     - Does it return maximum short value or does it use modulus?
//     - Does it set warning status (on ResultSet)?
// - Should getString(...) on BOOLEAN return "TRUE"/"FALSE" or "true"/"false"?
//   (TODO:  Check SQL spec for general canonical form and results of
//    CAST( TRUE AS VARCHAR ).)


/**
 * Integration-level unit test for ResultSet's <code>get<i>Type</i>(<i>column ID</i>)</code>
 * methods' type conversions.
 * <p>
 *   This test class is intended for higher-level type-vs.-type coverage tests.
 *   Detailed-case tests (e.g., boundary conditions) are intended to be in
 *   {@link com.dremio.jdbc.impl.TypeConvertingSqlAccessor}).
 * </p>
 */
public class ResultSetGetMethodConversionsTest extends JdbcWithServerTestBase {
  private static ResultSet testDataRow;
  private static ResultSet testDataRowWithNulls;

  @BeforeClass
  public static void setUpConnectionAndMetadataToCheck() throws SQLException {
    JdbcWithServerTestBase.setUpConnection();

    // Set up result row with values of various types.
    final Statement stmt = getConnection().createStatement();
    testDataRow = stmt.executeQuery(
        ""
        +   "SELECT  "
        + "\n"
        + "\n  TRUE                             AS  C_BOOLEAN_TRUE, "
        // TODO(DRILL-2470): Uncomment when TINYINT is implemented:
        //+ "\n  CAST(  1 AS TINYINT            ) AS  C_TINYINT_1, "
        // TODO(DRILL-2470): Uncomment when SMALLINT is implemented:
        //+ "\n  CAST(  2 AS SMALLINT           ) AS  C_SMALLINT_2, "
        + "\n  CAST(  3 AS INTEGER            ) AS  C_INTEGER_3, "
        + "\n  CAST(  4 AS BIGINT             ) AS  C_BIGINT_4, "
        // TODO(DRILL-2683): Uncomment when REAL is implemented:
        //+ "\n  CAST(  5.5 AS REAL             ) AS \"C_REAL_5.5\", "
        + "\n  CAST(  6.6 AS DOUBLE PRECISION ) AS \"C_DOUBLE_PREC._6.6\", "
        + "\n  CAST(  7.7 AS FLOAT            ) AS \"C_FLOAT_7.7\", "
        + "\n  CAST( 10.10 AS DECIMAL(4,2)         ) AS \"C_DECIMAL_10.10\", "
        + "\n  CAST( 10.5  AS DECIMAL(3,1)         ) AS \"C_DECIMAL_10.5\", "
        + "\n  CAST( 11.11 AS DECIMAL(9,2)    ) AS \"C_DECIMAL(9,2)_11.11\", "
        + "\n  CAST( 12.12 AS DECIMAL(18,2)   ) AS \"C_DECIMAL(18,2)_12.12\", "
        + "\n  CAST( 13.13 AS DECIMAL(28,2)   ) AS \"C_DECIMAL(28,2)_13.13\", "
        + "\n  CAST( 14.14 AS DECIMAL(38,2)   ) AS \"C_DECIMAL(38,2)_14.14\", "
        + "\n  '' "
        + "\nFROM INFORMATION_SCHEMA.CATALOGS "
        + "\nLIMIT 1 " );
    // Note: Assertions must be enabled (as they have been so far in tests).
    assertThat(testDataRow.next()).isTrue();

    final Statement stmtForNulls = getConnection().createStatement();
    testDataRowWithNulls = stmtForNulls.executeQuery(
        ""
            +   "SELECT  "
            + "\n"
            + "\n  CAST(null as boolean)                  AS  C_BOOLEAN_TRUE, "
            // TODO(DRILL-2470): Uncomment when TINYINT is implemented:
            //+ "\n  CAST(  null AS TINYINT            ) AS  C_TINYINT_1, "
            // TODO(DRILL-2470): Uncomment when SMALLINT is implemented:
            //+ "\n  CAST(  null AS SMALLINT           ) AS  C_SMALLINT_2, "
            + "\n  CAST(  null AS INTEGER            ) AS  C_INTEGER_3, "
            + "\n  CAST(  null AS BIGINT             ) AS  C_BIGINT_4, "
            // TODO(DRILL-2683): Uncomment when REAL is implemented:
            //+ "\n  CAST(  null AS REAL             ) AS \"C_REAL_5.5\", "
            + "\n  CAST(  null AS DOUBLE PRECISION ) AS \"C_DOUBLE_PREC._6.6\", "
            + "\n  CAST(  null AS FLOAT            ) AS \"C_FLOAT_7.7\", "
            + "\n  CAST( null AS DECIMAL         ) AS \"C_DECIMAL_10.10\", "
            + "\n  CAST( null  AS DECIMAL         ) AS \"C_DECIMAL_10.5\", "
            + "\n  CAST( null AS DECIMAL(9,2)    ) AS \"C_DECIMAL(9,2)_11.11\", "
            + "\n  CAST( null AS DECIMAL(18,2)   ) AS \"C_DECIMAL(18,2)_12.12\", "
            + "\n  CAST( null AS DECIMAL(28,2)   ) AS \"C_DECIMAL(28,2)_13.13\", "
            + "\n  CAST( null AS DECIMAL(38,2)   ) AS \"C_DECIMAL(38,2)_14.14\", "
            + "\n  '' "
            + "\nFROM (VALUES(1))" );
    // Note: Assertions must be enabled (as they have been so far in tests).
    assertThat(testDataRowWithNulls.next()).isTrue();
  }


  ////////////////////////////////////////
  // - getByte:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - ROWID;


  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_getByte_handles_TINYINT() throws SQLException {
    assertThat(testDataRow.getByte("C_TINYINT_1")).isEqualTo((byte) 1);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_getByte_handles_SMALLINT() throws SQLException {
    assertThat(testDataRow.getByte("C_SMALLINT_2")).isEqualTo((byte) 2);
  }

  @Test
  public void test_getByte_handles_INTEGER() throws SQLException {
    assertThat(testDataRow.getByte("C_INTEGER_3")).isEqualTo((byte) 3);
  }

  @Test
  public void test_getByte_handles_BIGINT() throws SQLException {
    assertThat(testDataRow.getByte("C_BIGINT_4")).isEqualTo((byte) 4);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_getByte_handles_REAL() throws SQLException {
    assertThat(testDataRow.getByte("C_REAL_5.5")).isEqualTo((byte) 5);
  }

  @Test
  public void test_getByte_handles_DOUBLE() throws SQLException {
    assertThat(testDataRow.getByte("C_DOUBLE_PREC._6.6")).isEqualTo((byte) 6);
  }

  @Test
  public void test_getByte_handles_FLOAT() throws SQLException {
    assertThat(testDataRow.getByte("C_FLOAT_7.7")).isEqualTo((byte) 7);
  }

  @Test
  public void test_getByte_handles_DECIMAL() throws SQLException {
    assertThat(testDataRow.getByte("C_DECIMAL_10.10")).isEqualTo((byte) 10);
  }

  ////////////////////////////////////////
  // - getShort:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_getShort_handles_TINYINT() throws SQLException {
    assertThat(testDataRow.getShort("C_TINYINT_1")).isEqualTo((short) 1);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_getShort_handles_SMALLINT() throws SQLException {
    assertThat(testDataRow.getShort("C_SMALLINT_2")).isEqualTo((short) 2);
  }

  @Test
  public void test_getShort_handles_INTEGER() throws SQLException {
    assertThat(testDataRow.getShort("C_INTEGER_3")).isEqualTo((short) 3);
  }

  @Test
  public void test_getShort_handles_BIGINT() throws SQLException {
    assertThat(testDataRow.getShort("C_BIGINT_4")).isEqualTo((short) 4);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_getShort_handles_REAL() throws SQLException {
    assertThat(testDataRow.getShort("C_REAL_5.5")).isEqualTo((short) 5);
  }

  @Test
  public void test_getShort_handles_DOUBLE() throws SQLException {
    assertThat(testDataRow.getShort("C_DOUBLE_PREC._6.6")).isEqualTo((short) 6);
  }

  @Test
  public void test_getShort_handles_FLOAT() throws SQLException {
    assertThat(testDataRow.getShort("C_FLOAT_7.7")).isEqualTo((short) 7);
  }

  @Test
  public void test_getShort_handles_DECIMAL() throws SQLException {
    assertThat(testDataRow.getShort("C_DECIMAL_10.10")).isEqualTo((short) 10);
  }

  ////////////////////////////////////////
  // - getInt:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_getInt_handles_TINYINT() throws SQLException {
    assertThat(testDataRow.getInt("C_TINYINT_1")).isEqualTo(1);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_getInt_handles_SMALLINT() throws SQLException {
    assertThat(testDataRow.getInt("C_SMALLINT_2")).isEqualTo(2);
  }

  @Test
  public void test_getInt_handles_INTEGER() throws SQLException {
    assertThat(testDataRow.getInt("C_INTEGER_3")).isEqualTo(3);
  }

  @Test
  public void test_getInt_handles_BIGINT() throws SQLException {
    assertThat(testDataRow.getInt("C_BIGINT_4")).isEqualTo(4);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_getInt_handles_REAL() throws SQLException {
    assertThat(testDataRow.getInt("C_REAL_5.5")).isEqualTo(5);
  }

  @Test
  public void test_getInt_handles_DOUBLE() throws SQLException {
    assertThat(testDataRow.getInt("C_DOUBLE_PREC._6.6")).isEqualTo(6);
  }

  @Test
  public void test_getInt_handles_FLOAT() throws SQLException {
    assertThat(testDataRow.getInt("C_FLOAT_7.7")).isEqualTo(7);
  }

  @Test
  public void test_getInt_handles_DECIMAL() throws SQLException {
    assertThat(testDataRow.getInt("C_DECIMAL_10.10")).isEqualTo(10);
  }

  ////////////////////////////////////////
  // - getLong:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_getLong_handles_TINYINT() throws SQLException {
    assertThat(testDataRow.getLong("C_TINYINT_1")).isEqualTo(1L);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_getLong_handles_SMALLINT() throws SQLException {
    assertThat(testDataRow.getLong("C_SMALLINT_2")).isEqualTo(2L);
  }

  @Test
  public void test_getLong_handles_INTEGER() throws SQLException {
    assertThat(testDataRow.getLong("C_INTEGER_3")).isEqualTo(3L);
  }

  @Test
  public void test_getLong_handles_BIGINT() throws SQLException {
    assertThat(testDataRow.getLong("C_BIGINT_4")).isEqualTo(4L);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_getLong_handles_REAL() throws SQLException {
    assertThat(testDataRow.getLong("C_REAL_5.5")).isEqualTo(5L);
  }

  @Test
  public void test_getLong_handles_DOUBLE() throws SQLException {
    assertThat(testDataRow.getLong("C_DOUBLE_PREC._6.6")).isEqualTo(6L);
  }

  @Test
  public void test_getLong_handles_FLOAT() throws SQLException {
    assertThat(testDataRow.getLong("C_FLOAT_7.7")).isEqualTo(7L);
  }

  @Test
  public void test_getLong_handles_DECIMAL() throws SQLException {
    assertThat(testDataRow.getLong("C_DECIMAL_10.10")).isEqualTo(10L);
  }

  ////////////////////////////////////////
  // - getFloat:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_getFloat_handles_TINYINT() throws SQLException {
    assertThat(testDataRow.getFloat("C_TINYINT_1")).isEqualTo(1f);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_getFloat_handles_SMALLINT() throws SQLException {
    assertThat(testDataRow.getFloat("C_SMALLINT_2")).isEqualTo(2f);
  }

  @Test
  public void test_getFloat_handles_INTEGER() throws SQLException {
    assertThat(testDataRow.getFloat("C_INTEGER_3")).isEqualTo(3f);
  }

  @Test
  public void test_getFloat_handles_BIGINT() throws SQLException {
    assertThat(testDataRow.getFloat("C_BIGINT_4")).isEqualTo(4f);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_getFloat_handles_REAL() throws SQLException {
    assertThat(testDataRow.getFloat("C_REAL_5.5")).isEqualTo(5.5f);
  }

  @Test
  public void test_getFloat_handles_DOUBLE() throws SQLException {
    assertThat(testDataRow.getFloat("C_DOUBLE_PREC._6.6")).isEqualTo(6.6f);
  }

  @Test
  public void test_getFloat_handles_FLOAT() throws SQLException {
    assertThat(testDataRow.getFloat("C_FLOAT_7.7")).isEqualTo(7.7f);
  }

  @Test
  public void test_getFloat_handles_DECIMAL() throws SQLException {
    assertThat(testDataRow.getFloat("C_DECIMAL_10.10")).isEqualTo(10.10f);
  }

  ////////////////////////////////////////
  // - getDouble:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_getDouble_handles_TINYINT() throws SQLException {
    assertThat(testDataRow.getDouble("C_TINYINT_1")).isEqualTo(1D);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_getDouble_handles_SMALLINT() throws SQLException {
    assertThat(testDataRow.getDouble("C_SMALLINT_2")).isEqualTo(2D);
  }

  @Test
  public void test_getDouble_handles_INTEGER() throws SQLException {
    assertThat(testDataRow.getDouble("C_INTEGER_3")).isEqualTo(3D);
  }

  @Test
  public void test_getDouble_handles_BIGINT() throws SQLException {
    assertThat(testDataRow.getDouble("C_BIGINT_4")).isEqualTo(4D);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_getDouble_handles_REAL() throws SQLException {
    assertThat(testDataRow.getDouble("C_REAL_5.5")).isEqualTo((double) 5.5f);
  }

  @Test
  public void test_getDouble_handles_DOUBLE() throws SQLException {
    assertThat(testDataRow.getDouble("C_DOUBLE_PREC._6.6")).isEqualTo(6.6d);
  }

  @Test
  public void test_getDouble_handles_FLOAT() throws SQLException {
    assertThat(testDataRow.getDouble("C_FLOAT_7.7")).isEqualTo((double) 7.7f);
  }

  @Test
  public void test_getDouble_handles_DECIMAL() throws SQLException {
    assertThat(testDataRow.getDouble("C_DECIMAL_10.10")).isEqualTo(10.10d);
  }

  ////////////////////////////////////////
  // - getBigDecimal:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_getBigDecimal_handles_TINYINT() throws SQLException {
    assertThat(testDataRow.getBigDecimal("C_TINYINT_1")).isEqualTo(new BigDecimal(1));
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_getBigDecimal_handles_SMALLINT() throws SQLException {
    assertThat(testDataRow.getBigDecimal("C_SMALLINT_2")).isEqualTo(new BigDecimal(2));
  }

  @Test
  public void test_getBigDecimal_handles_INTEGER() throws SQLException {
    assertThat(testDataRow.getBigDecimal("C_INTEGER_3")).isEqualTo(new BigDecimal(3));
  }

  @Test
  public void test_getBigDecimal_handles_BIGINT() throws SQLException {
    assertThat(testDataRow.getBigDecimal("C_BIGINT_4")).isEqualTo(new BigDecimal(4));
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_getBigDecimal_handles_REAL() throws SQLException {
    assertThat(testDataRow.getBigDecimal("C_REAL_5.5")).isEqualTo(BigDecimal.valueOf(5.5f));
  }

  @Test
  public void test_getBigDecimal_handles_DOUBLE() throws SQLException {
    assertThat(testDataRow.getBigDecimal("C_DOUBLE_PREC._6.6")).isEqualTo(BigDecimal.valueOf(6.6d));
  }

  @Test
  public void test_getBigDecimal_handles_FLOAT() throws SQLException {
    assertThat(testDataRow.getBigDecimal("C_FLOAT_7.7")).isEqualTo(BigDecimal.valueOf(7.7f));
  }

  @Test
  public void test_getBigDecimal_handles_DECIMAL_1() throws SQLException {
    assertThat(testDataRow.getBigDecimal("C_DECIMAL_10.5")).isEqualTo(new BigDecimal("10.5"));
  }

  @Ignore("TODO(DRILL-3367): unignore when DECIMAL is no longer DOUBLE")
  @Test
  public void test_getBigDecimal_handles_DECIMAL_2() throws SQLException {
    assertThat(testDataRow.getBigDecimal("C_DECIMAL_10.10")).isEqualTo(new BigDecimal("10.10"));
  }

  ////////////////////////////////////////
  // - getBoolean:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //

  ////////////////////////////////////////
  // - getString:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - NCHAR, NVARCHAR, LONGNVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - DATE, TIME, TIMESTAMP;
  //   - DATALINK;
  //

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_getString_handles_TINYINT() throws SQLException {
    assertThat(testDataRow.getString("C_TINYINT_1")).isEqualTo("1");
    assertThat(testDataRowWithNulls.getString("C_TINYINT_1")).isEqualTo(null);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_getString_handles_SMALLINT() throws SQLException {
    assertThat(testDataRow.getString("C_SMALLINT_2")).isEqualTo("2");
    assertThat(testDataRowWithNulls.getString("C_SMALLINT_2")).isEqualTo(null);
  }

  @Test
  public void test_getString_handles_INTEGER() throws SQLException {
    assertThat(testDataRow.getString("C_INTEGER_3")).isEqualTo("3");
    assertThat(testDataRowWithNulls.getString("C_INTEGER_3")).isEqualTo(null);
  }

  @Test
  public void test_getString_handles_BIGINT() throws SQLException {
    assertThat(testDataRow.getString("C_BIGINT_4")).isEqualTo("4");
    assertThat(testDataRowWithNulls.getString("C_BIGINT_4")).isEqualTo(null);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_getString_handles_REAL() throws SQLException {
    assertThat(testDataRow.getString("C_REAL_5.5")).isEqualTo("5.5????");
    assertThat(testDataRowWithNulls.getString("C_REAL_5.5")).isEqualTo(null);
  }

  @Test
  public void test_getString_handles_DOUBLE() throws SQLException {
    assertThat(testDataRow.getString("C_DOUBLE_PREC._6.6")).isEqualTo("6.6");
    assertThat(testDataRowWithNulls.getString("C_DOUBLE_PREC._6.6")).isEqualTo(null);
  }

  @Test
  public void test_getString_handles_FLOAT() throws SQLException {
    assertThat(testDataRow.getString("C_FLOAT_7.7")).isEqualTo("7.7");
    assertThat(testDataRowWithNulls.getString("C_FLOAT_7.7")).isEqualTo(null);
  }

  @Test
  public void test_getString_handles_DECIMAL() throws SQLException {
    assertThat(testDataRow.getString("C_DECIMAL_10.10")).isEqualTo("10.10");
    assertThat(testDataRowWithNulls.getString("C_DECIMAL_10.10")).isEqualTo(null);
  }

  ////////////////////////////////////////
  // - getNString:s
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - NCHAR, NVARCHAR, LONGNVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - DATE, TIME, TIMESTAMP;
  //   - DATALINK;
  //

  ////////////////////////////////////////
  // - getBytes:
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //

  ////////////////////////////////////////
  // - getDate:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - DATE, TIMESTAMP;
  //

  ////////////////////////////////////////
  // - getTime:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - TIME, TIMESTAMP;
  //

  ////////////////////////////////////////
  // - getTimestamp:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - DATE, TIME, TIMESTAMP;
  //

  ////////////////////////////////////////
  // - getAsciiStream:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - CLOB, NCLOB;
  //

  ////////////////////////////////////////
  // - getBinaryStream:
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //

  ////////////////////////////////////////
  // - getCharacterStream:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - NCHAR, NVARCHAR, LONGNVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - CLOB, NCLOB;
  //   - SQLXML;
  //

  ////////////////////////////////////////
  // - getNCharacterStream:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - NCHAR, NVARCHAR, LONGNVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - CLOB, NCLOB;
  //   - SQLXML;
  //

  ////////////////////////////////////////
  // - getClob:
  //   - CLOB, NCLOB;
  //

  ////////////////////////////////////////
  // - getNClob:
  //   - CLOB, NCLOB;
  //

  ////////////////////////////////////////
  // - getBlob:
  //   - BLOB;
  //

  ////////////////////////////////////////
  // - getArray:
  //   - ARRAY;
  //

  ////////////////////////////////////////
  // - getRef:
  //   - REF;
  //

  ////////////////////////////////////////
  // - getURL:
  //   - DATALINK;
  //

  ////////////////////////////////////////
  // - getObject:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - NCHAR, NVARCHAR, LONGNVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - CLOB, NCLOB;
  //   - BLOB;
  //   - DATE, TIME, TIMESTAMP;
  //   - TIME_WITH_TIMEZONE;
  //   - TIMESTAMP_WITH_TIMEZONE;
  //   - DATALINK;
  //   - ROWID;
  //   - SQLXML;
  //   - ARRAY;
  //   - REF;
  //   - STRUCT;
  //   - JAVA_OBJECT;
  //

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_getObject_handles_TINYINT() throws SQLException {
    assertThat(testDataRow.getObject("C_TINYINT_1")).isEqualTo(1);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_getObject_handles_SMALLINT() throws SQLException {
    assertThat(testDataRow.getObject("C_SMALLINT_2")).isEqualTo(2);
  }

  @Test
  public void test_getObject_handles_INTEGER() throws SQLException {
    assertThat(testDataRow.getObject("C_INTEGER_3")).isEqualTo(3);
  }

  @Test
  public void test_getObject_handles_BIGINT() throws SQLException {
    assertThat(testDataRow.getObject("C_BIGINT_4")).isEqualTo(4L);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_getObject_handles_REAL() throws SQLException {
    assertThat(testDataRow.getObject("C_REAL_5.5")).isEqualTo(5.5f);
  }

  @Test
  public void test_getObject_handles_DOUBLE() throws SQLException {
    assertThat(testDataRow.getObject("C_DOUBLE_PREC._6.6")).isEqualTo(6.6d);
  }

  @Test
  public void test_getObject_handles_FLOAT() throws SQLException {
    assertThat(testDataRow.getObject("C_FLOAT_7.7")).isEqualTo(7.7f);
  }

  @Ignore("TODO(DRILL-3367): unignore when DECIMAL is no longer DOUBLE")
  @Test
  public void test_getObject_handles_DECIMAL_1() throws SQLException {
    assertThat(testDataRow.getObject("C_DECIMAL_10.5")).isEqualTo(new BigDecimal("10.5"));
  }

  @Ignore("TODO(DRILL-3367): unignore when DECIMAL is no longer DOUBLE")
  @Test
  public void test_getObject_handles_DECIMAL_2() throws SQLException {
    assertThat(testDataRow.getObject("C_DECIMAL_10.10")).isEqualTo(new BigDecimal("10.10"));
  }

  ////////////////////////////////////////
  // - getRowId:
  //   - ROWID;
  //

  ////////////////////////////////////////
  // - getSQLXML:
  //   - SQLXML SQLXML

}
