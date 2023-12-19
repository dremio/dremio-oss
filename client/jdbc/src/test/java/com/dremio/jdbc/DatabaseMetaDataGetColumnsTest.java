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

import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.ResultSetMetaData.columnNullable;
import static java.sql.ResultSetMetaData.columnNullableUnknown;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

// NOTE: TestInformationSchemaColumns and DatabaseMetaDataGetColumnsTest
// have identical sections.  (Cross-maintain them for now; factor out later.)

// TODO:  MOVE notes to implementation (have this not (just) in test).

// TODO:  Determine for each numeric type whether its precision is reported in
//   decimal or binary (and NUM_PREC_RADIX is 10 or 2, respectively).
//   The SQL specification for INFORMATION_SCHEMA.COLUMNS seems to specify the
//   radix for each numeric type:
//   - 2 or 10 for SMALLINT, INTEGER, and BIGINT;
//   - only 10 for NUMERIC and DECIMAL; and
//   - only 2  for REAL, FLOAT, and DOUBLE PRECISION.
//   However, it is not clear what the JDBC API intends:
//   - It has NUM_PREC_RADIX, specifying a radix or 10 or 2, but doesn't specify
//     exactly what is applies to.  Apparently, it applies to COLUMN_SIZE abd
//     ResultMetaData.getPrecision() (which are defined in terms of maximum
//     precision for numeric types).
//   - Is has DECIMAL_DIGITS, which is <em>not</em> the number of decimal digits
//     of precision, but which it defines as the number of fractional digits--
//     without actually specifying that it's in decimal.

// TODO:  Review nullability (NULLABLE and IS_NULLABLE columns):
// - It's not clear what JDBC's requirements are.
//   - It does seem obvious that metadata should not contradictorily say that a
//   - column cannot contain nulls when the column currently does contain nulls.
//   - It's not clear whether metadata must say that a column cannot contains
//     nulls if JDBC specifies that the column always has a non-null value.
// - It's not clear why Dremio reports that columns that will never contain nulls
//   can contain nulls.
// - It's not clear why Dremio sets INFORMATION_SCHEMA.COLUMNS.IS_NULLABLE to
//   'NO' for some columns that contain only null (e.g., for
//   "CREATE VIEW x AS SELECT CAST(NULL AS ...) ..."


/**
 * Test class for Dremio's java.sql.DatabaseMetaData.getColumns() implementation.
 * <p>
 *   Based on JDBC 4.1 (Java 7).
 * </p>
 */
@Ignore("DX-2490")
public class DatabaseMetaDataGetColumnsTest extends JdbcWithServerTestBase {
  private static final String VIEW_SCHEMA = "dfs_test";
  private static final String VIEW_NAME =
      DatabaseMetaDataGetColumnsTest.class.getSimpleName() + "_View";

  /** Overall (connection-level) metadata. */
  protected static DatabaseMetaData dbMetadata;

  /** getColumns result metadata.  For checking columns themselves (not cell
   *  values or row order). */
  protected static ResultSetMetaData rowsMetadata;


  ////////////////////
  // Results from getColumns for test columns of various types.
  // Each ResultSet is positioned at first row for, and must not be modified by,
  // test methods.

  //////////
  // For columns in temporary test view (types accessible via casting):

  private static ResultSet mdrOptBOOLEAN;

  // TODO(DRILL-2470): re-enable TINYINT, SMALLINT, and REAL.
  private static ResultSet mdrReqTINYINT;
  private static ResultSet mdrOptSMALLINT;
  private static ResultSet mdrReqINTEGER;
  private static ResultSet mdrOptBIGINT;

  // TODO(DRILL-2470): re-enable TINYINT, SMALLINT, and REAL.
  private static ResultSet mdrOptREAL;
  private static ResultSet mdrOptFLOAT;
  private static ResultSet mdrReqDOUBLE;

  private static ResultSet mdrReqDECIMAL_5_3;
  // No NUMERIC while Dremio just maps it to DECIMAL.

  private static ResultSet mdrReqVARCHAR_10;
  private static ResultSet mdrOptVARCHAR;
  private static ResultSet mdrReqCHAR_5;
  // No NCHAR, etc., in Dremio (?).
  private static ResultSet mdrOptVARBINARY_16;
  private static ResultSet mdrOptBINARY_1048576;

  private static ResultSet mdrReqDATE;
  private static ResultSet mdrReqTIME;
  private static ResultSet mdrOptTIME_7;
  private static ResultSet mdrOptTIMESTAMP;
  // No "... WITH TIME ZONE" in Dremio.

  private static ResultSet mdrReqINTERVAL_Y;
  private static ResultSet mdrReqINTERVAL_3Y_Mo;
  private static ResultSet mdrReqINTERVAL_Mo;
  private static ResultSet mdrReqINTERVAL_D;
  private static ResultSet mdrReqINTERVAL_4D_H;
  private static ResultSet mdrReqINTERVAL_3D_Mi;
  private static ResultSet mdrReqINTERVAL_2D_S5;
  private static ResultSet mdrReqINTERVAL_H;
  private static ResultSet mdrReqINTERVAL_1H_Mi;
  private static ResultSet mdrReqINTERVAL_3H_S1;
  private static ResultSet mdrReqINTERVAL_Mi;
  private static ResultSet mdrReqINTERVAL_5Mi_S;
  private static ResultSet mdrReqINTERVAL_S;
  private static ResultSet mdrReqINTERVAL_3S;
  private static ResultSet mdrReqINTERVAL_3S1;

  // For columns in schema hive_test.default's infoschematest table:

  // listtype column:      VARCHAR(65535) ARRAY, non-null(?):
  private static ResultSet mdrReqARRAY;
  // maptype column:       (VARCHAR(65535), INTEGER) MAP, non-null(?):
  private static ResultSet mdrReqMAP;
  // structtype column:    STRUCT(INTEGER sint, BOOLEAN sboolean,
  //                              VARCHAR(65535) sstring), non-null(?):
  private static ResultSet mdrUnkSTRUCT;
  // uniontypetype column: OTHER (?), non=nullable(?):
  private static ResultSet mdrUnkUnion;


  private static ResultSet setUpRow( final String schemaName,
                                     final String tableOrViewName,
                                     final String columnName ) throws SQLException
  {
    System.out.println( "(Setting up row for " + tableOrViewName + "."
                        + columnName + ".)");
    assertThat(dbMetadata).isNotNull();
    final ResultSet testRow =
      dbMetadata.getColumns("DREMIO", schemaName, tableOrViewName, columnName);
    assertThat(testRow.next()).as("Test setup error:  No row for column DREMIO . \"" + schemaName
      + "\" . \"" + tableOrViewName + "\" . \"" + columnName + "\"").isTrue();
    return testRow;
  }

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    JdbcWithServerTestBase.setUpConnection();
    setUpMetadataToCheck();
  }

  protected static void setUpMetadataToCheck() throws SQLException {
    dbMetadata = getConnection().getMetaData();

    final Statement stmt = getConnection().createStatement();

    ResultSet util;

    /* TODO(DRILL-3253)(start): Update this once we have test plugin supporting all needed types
    // Create Hive test data, only if not created already (speed optimization):
    util = stmt.executeQuery( "SELECT * FROM INFORMATION_SCHEMA.COLUMNS "
                              + "WHERE TABLE_SCHEMA = 'hive_test.default' "
                              + "  AND TABLE_NAME = 'infoschematest'" );

    System.out.println( "(Hive infoschematest columns: " );
    int hiveTestColumnRowCount = 0;
    while ( util.next() ) {
      hiveTestColumnRowCount++;
      System.out.println(
          " Hive test column: "
          + util.getString( 1 ) + " - " + util.getString( 2 ) + " - "
          + util.getString( 3 ) + " - " + util.getString( 4 ) );
    }
    System.out.println( " Hive test column count: " + hiveTestColumnRowCount + ")" );
    if ( 0 == hiveTestColumnRowCount ) {
      // No Hive test data--create it.
      new HiveTestDataGenerator().generateTestData();
    } else if ( 17 == hiveTestColumnRowCount ) {
      // Hive data seems to exist already--skip recreating it.
    } else {
      fail("Expected 17 Hive test columns see " + hiveTestColumnRowCount + "."
            + "  Test code is out of date or Hive data is corrupted.");
    }
    TODO(DRILL-3253)(end) */

    // Create temporary test-columns view:
    util = stmt.executeQuery( "USE dfs_test" );
    assertThat( util.next() ).isTrue();
    assertThat(util.getBoolean(1)).as("Error setting schema for test: " + util.getString(2))
      .isTrue();
    // TODO(DRILL-2470): Adjust when TINYINT is implemented:
    // TODO(DRILL-2470): Adjust when SMALLINT is implemented:
    // TODO(DRILL-2683): Adjust when REAL is implemented:
    util = stmt.executeQuery(
        ""
        +   "CREATE OR REPLACE VIEW " + VIEW_NAME + " AS SELECT "
        + "\n  CAST( NULL    AS BOOLEAN            ) AS mdrOptBOOLEAN,        "
        + "\n  "
        + "\n  CAST(    1    AS INT            ) AS mdrReqTINYINT,        "
        + "\n  CAST( NULL    AS INT           ) AS mdrOptSMALLINT,       "
        //+ "\n  CAST(    1    AS TINYINT            ) AS mdrReqTINYINT,        "
        //+ "\n  CAST( NULL    AS SMALLINT           ) AS mdrOptSMALLINT,       "
        + "\n  CAST(    2    AS INTEGER            ) AS mdrReqINTEGER,        "
        + "\n  CAST( NULL    AS BIGINT             ) AS mdrOptBIGINT,         "
        + "\n  "
        + "\n  CAST( NULL    AS FLOAT               ) AS mdrOptREAL,           "
        //+ "\n  CAST( NULL    AS REAL               ) AS mdrOptREAL,           "
        + "\n  CAST( NULL    AS FLOAT              ) AS mdrOptFLOAT,          "
        + "\n  CAST(  3.3    AS DOUBLE             ) AS mdrReqDOUBLE,         "
        + "\n  "
        + "\n  CAST(  4.4    AS DECIMAL(5,3)       ) AS mdrReqDECIMAL_5_3,    "
        + "\n  "
        + "\n  CAST( 'Hi'    AS VARCHAR(10)        ) AS mdrReqVARCHAR_10,     "
        + "\n  CAST( NULL    AS VARCHAR            ) AS mdrOptVARCHAR,        "
        + "\n  CAST( '55'    AS CHAR(5)            ) AS mdrReqCHAR_5,         "
        + "\n  CAST( NULL    AS VARBINARY(16)      ) AS mdrOptVARBINARY_16,   "
        + "\n  CAST( NULL    AS VARBINARY(1048576) ) AS mdrOptBINARY_1048576, "
        + "\n  CAST( NULL    AS BINARY(8)          ) AS mdrOptBINARY_8,       "
        + "\n  "
        + "\n                   DATE '2015-01-01'    AS mdrReqDATE,           "
        + "\n                   TIME '23:59:59'      AS mdrReqTIME,           "
        + "\n  CAST( NULL    AS TIME(7)            ) AS mdrOptTIME_7,         "
        + "\n  CAST( NULL    AS TIMESTAMP          ) AS mdrOptTIMESTAMP,      "
        + "\n  INTERVAL '1'     YEAR                 AS mdrReqINTERVAL_Y,     "
        + "\n  INTERVAL '1-2'   YEAR(3) TO MONTH     AS mdrReqINTERVAL_3Y_Mo, "
        + "\n  INTERVAL '2'     MONTH                AS mdrReqINTERVAL_Mo,    "
        + "\n  INTERVAL '3'     DAY                  AS mdrReqINTERVAL_D,     "
        + "\n  INTERVAL '3 4'   DAY(4) TO HOUR       AS mdrReqINTERVAL_4D_H,  "
        + "\n  INTERVAL '3 4:5' DAY(3) TO MINUTE     AS mdrReqINTERVAL_3D_Mi, "
        + "\n  INTERVAL '3 4:5:6' DAY(2) TO SECOND(5) AS mdrReqINTERVAL_2D_S5, "
        + "\n  INTERVAL '4'     HOUR                 AS mdrReqINTERVAL_H,     "
        + "\n  INTERVAL '4:5'   HOUR(1) TO MINUTE    AS mdrReqINTERVAL_1H_Mi, "
        + "\n  INTERVAL '4:5:6' HOUR(3) TO SECOND(1) AS mdrReqINTERVAL_3H_S1, "
        + "\n  INTERVAL '5'     MINUTE               AS mdrReqINTERVAL_Mi,    "
        + "\n  INTERVAL '5:6'   MINUTE(5) TO SECOND  AS mdrReqINTERVAL_5Mi_S, "
        + "\n  INTERVAL '6'     SECOND               AS mdrReqINTERVAL_S,     "
        + "\n  INTERVAL '6'     SECOND(3)            AS mdrReqINTERVAL_3S,    "
        + "\n  INTERVAL '6'     SECOND(3, 1)         AS mdrReqINTERVAL_3S1,   "
        + "\n  '' "
        + "\nFROM INFORMATION_SCHEMA.COLUMNS "
        + "\nLIMIT 1 " );
    assertThat(util.next()).isTrue();
    assertThat(util.getBoolean(1)).as(
      "Error creating temporary test-columns view " + VIEW_NAME + ": "
        + util.getString(2)).isTrue();

    // Set up result rows for temporary test view and Hivetest columns:

    mdrOptBOOLEAN        = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptBOOLEAN" );

    // TODO(DRILL-2470): Uncomment when TINYINT is implemented:
    //mdrReqTINYINT        = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqTINYINT" );
    // TODO(DRILL-2470): Uncomment when SMALLINT is implemented:
    //mdrOptSMALLINT       = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptSMALLINT" );
    mdrReqINTEGER        = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTEGER" );
    mdrOptBIGINT         = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptBIGINT" );

    // TODO(DRILL-2683): Uncomment when REAL is implemented:
    //mdrOptREAL           = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptREAL" );
    mdrOptFLOAT          = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptFLOAT" );
    mdrReqDOUBLE         = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqDOUBLE" );

    mdrReqDECIMAL_5_3    = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqDECIMAL_5_3" );

    mdrReqVARCHAR_10     = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqVARCHAR_10" );
    mdrOptVARCHAR        = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptVARCHAR" );
    mdrReqCHAR_5         = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqCHAR_5" );
    mdrOptVARBINARY_16   = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptVARBINARY_16" );
    mdrOptBINARY_1048576 = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptBINARY_1048576" );

    mdrReqDATE           = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqDATE" );
    mdrReqTIME           = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqTIME" );
    mdrOptTIME_7         = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptTIME_7" );
    mdrOptTIMESTAMP      = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptTIMESTAMP" );

    mdrReqINTERVAL_Y     = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_Y" );
    mdrReqINTERVAL_3Y_Mo = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_3Y_Mo" );
    mdrReqINTERVAL_Mo    = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_Mo" );
    mdrReqINTERVAL_D     = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_D" );
    mdrReqINTERVAL_4D_H  = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_4D_H" );
    mdrReqINTERVAL_3D_Mi = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_3D_Mi" );
    mdrReqINTERVAL_2D_S5 = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_2D_S5" );
    mdrReqINTERVAL_H     = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_H" );
    mdrReqINTERVAL_1H_Mi = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_1H_Mi" );
    mdrReqINTERVAL_3H_S1 = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_3H_S1" );
    mdrReqINTERVAL_Mi    = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_Mi" );
    mdrReqINTERVAL_5Mi_S = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_5Mi_S" );
    mdrReqINTERVAL_S     = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_S" );
    mdrReqINTERVAL_3S    = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_3S" );
    mdrReqINTERVAL_3S1   = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_3S1" );

    /* TODO(DRILL-3253)(start): Update this once we have test plugin supporting all needed types.
    mdrReqARRAY   = setUpRow( "hive_test.default", "infoschematest", "listtype" );
    mdrReqMAP     = setUpRow( "hive_test.default", "infoschematest", "maptype" );
    mdrUnkSTRUCT = setUpRow( "hive_test.default", "infoschematest", "structtype" );
    mdrUnkUnion  = setUpRow( "hive_test.default", "infoschematest", "uniontypetype" );
    TODO(DRILL-3253)(end) */

    // Set up getColumns(...)) result set' metadata:

    // Get all columns for more diversity of values (e.g., nulls and non-nulls),
    // in case that affects metadata (e.g., nullability) (in future).
    final ResultSet allColumns = dbMetadata.getColumns( null /* any catalog */,
                                                        null /* any schema */,
                                                        "%"  /* any table */,
                                                        "%"  /* any column */ );
    rowsMetadata = allColumns.getMetaData();
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    final ResultSet util =
      getConnection().createStatement().executeQuery("DROP VIEW " + VIEW_NAME + "");
    assertThat(util.next()).isTrue();
    assertThat(util.getBoolean(1)).as(
      "Error dropping temporary test-columns view " + VIEW_NAME + ": "
        + util.getString(2)).isTrue();
    JdbcWithServerTestBase.tearDownConnection();
  }


  private Integer getIntOrNull( ResultSet row, String columnName ) throws SQLException {
    final int value = row.getInt( columnName );
    return row.wasNull() ? null : value;
  }


  //////////////////////////////////////////////////////////////////////
  // Tests:

  ////////////////////////////////////////////////////////////
  // Number of columns.

  @Test
  public void testMetadataHasRightNumberOfColumns() throws SQLException {
    // TODO:  Review:  Is this check valid?  (Are extra columns allowed?)
    assertThat(rowsMetadata.getColumnCount()).isEqualTo(24);
  }


  ////////////////////////////////////////////////////////////
  // #1: TABLE_CAT:
  // - JDBC:   "1. ... String => table catalog (may be null)"
  // - Dremio:  Apparently chooses always "DREMIO".
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable(?);

  @Test
  public void test_TABLE_CAT_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(1)).isEqualTo("TABLE_CAT");
  }

  @Test
  public void test_TABLE_CAT_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("TABLE_CAT")).isEqualTo("DREMIO");
  }

  // Not bothering with other test columns for TABLE_CAT.

  @Test
  public void test_TABLE_CAT_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(1)).isEqualTo("TABLE_CAT");
  }

  @Test
  public void test_TABLE_CAT_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(1)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_TABLE_CAT_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(1)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_TABLE_CAT_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(1)).isEqualTo(String.class.getName());
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_TABLE_CAT_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(1)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #2: TABLE_SCHEM:
  // - JDBC:   "2. ... String => table schema (may be null)"
  // - Dremio:  Always reports a schema name.
  // - (Meta): VARCHAR (NVARCHAR?); Nullable?;

  @Test
  public void test_TABLE_SCHEM_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(2)).isEqualTo("TABLE_SCHEM");
  }

  @Test
  public void test_TABLE_SCHEM_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("TABLE_SCHEM")).isEqualTo(VIEW_SCHEMA);
  }

  // Not bothering with other _local_view_ test columns for TABLE_SCHEM.

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_TABLE_SCHEM_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(mdrReqARRAY.getString("TABLE_SCHEM")).isEqualTo("hive_test.default");
  }

  // Not bothering with other Hive test columns for TABLE_SCHEM.

  @Test
  public void test_TABLE_SCHEM_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(2)).isEqualTo("TABLE_SCHEM");
  }

  @Test
  public void test_TABLE_SCHEM_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(2)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_TABLE_SCHEM_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(2)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_TABLE_SCHEM_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(2)).isEqualTo(String.class.getName());
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_TABLE_SCHEM_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(2)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #3: TABLE_NAME:
  // - JDBC:  "3. ... String => table name"
  // - Dremio:
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable?;

  @Test
  public void test_TABLE_NAME_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(3)).isEqualTo("TABLE_NAME");
  }

  @Test
  public void test_TABLE_NAME_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("TABLE_NAME")).isEqualTo(VIEW_NAME);
  }

  // Not bothering with other _local_view_ test columns for TABLE_NAME.

  @Test
  public void test_TABLE_NAME_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(3)).isEqualTo("TABLE_NAME");
  }

  @Test
  public void test_TABLE_NAME_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(3)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_TABLE_NAME_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(3)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_TABLE_NAME_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(3)).isEqualTo(String.class.getName());
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_TABLE_NAME_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(3)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #4: COLUMN_NAME:
  // - JDBC:  "4. ... String => column name"
  // - Drill:
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable(?);

  @Test
  public void test_COLUMN_NAME_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(4)).isEqualTo("COLUMN_NAME");
  }

  @Test
  public void test_COLUMN_NAME_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("COLUMN_NAME")).isEqualTo("mdrOptBOOLEAN");
  }

  // Not bothering with other _local_view_ test columns for TABLE_SCHEM.

  @Test
  @Ignore("TODO(DDremio3253): unignore when we have all-types test storage plugin")
  public void test_COLUMN_NAME_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(mdrReqARRAY.getString("COLUMN_NAME")).isEqualTo("listtype");
  }

  // Not bothering with other Hive test columns for TABLE_SCHEM.

  @Test
  public void test_COLUMN_NAME_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(4)).isEqualTo("COLUMN_NAME");
  }

  @Test
  public void test_COLUMN_NAME_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(4)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_COLUMN_NAME_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(4)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_COLUMN_NAME_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(4)).isEqualTo(String.class.getName());
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_COLUMN_NAME_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(4)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #5: DATA_TYPE:
  // - JDBC:  "5. ... int => SQL type from java.sql.Types"
  // - Dremio:
  // - (Meta): INTEGER(?);  Non-nullable(?);

  @Test
  public void test_DATA_TYPE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(5)).isEqualTo("DATA_TYPE");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "DATA_TYPE")).isEqualTo(Types.BOOLEAN);
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "DATA_TYPE")).isEqualTo(Types.TINYINT);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "DATA_TYPE")).isEqualTo(Types.SMALLINT);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "DATA_TYPE")).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "DATA_TYPE")).isEqualTo(Types.BIGINT);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "DATA_TYPE")).isEqualTo(Types.REAL);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "DATA_TYPE")).isEqualTo(Types.FLOAT);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "DATA_TYPE")).isEqualTo(Types.DOUBLE);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "DATA_TYPE")).isEqualTo(Types.DECIMAL);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "DATA_TYPE")).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "DATA_TYPE")).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "DATA_TYPE")).isEqualTo(Types.CHAR);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "DATA_TYPE")).isEqualTo(Types.VARBINARY);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptBINARY_1048576CHECK() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "DATA_TYPE")).isEqualTo(Types.VARBINARY);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "DATA_TYPE")).isEqualTo(Types.DATE);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "DATA_TYPE")).isEqualTo(Types.TIME);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "DATA_TYPE")).isEqualTo(Types.TIME);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "DATA_TYPE")).isEqualTo(Types.TIMESTAMP);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "DATA_TYPE")).isEqualTo(Types.OTHER);
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqINTERVAL_H_S3() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "DATA_TYPE")).isEqualTo(Types.OTHER);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATA_TYPE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "DATA_TYPE")).isEqualTo(Types.ARRAY);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATA_TYPE_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "DATA_TYPE")).isEqualTo(Types.OTHER);
    // To-do:  Determine which.
    assertThat(getIntOrNull(mdrReqMAP, "DATA_TYPE")).isEqualTo(Types.JAVA_OBJECT);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATA_TYPE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "DATA_TYPE")).isEqualTo(Types.STRUCT);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATA_TYPE_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "DATA_TYPE")).isEqualTo(Types.OTHER);
    // To-do:  Determine which.
    assertThat(getIntOrNull(mdrUnkUnion, "DATA_TYPE")).isEqualTo(Types.JAVA_OBJECT);
  }

  @Test
  public void test_DATA_TYPE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(5)).isEqualTo("DATA_TYPE");
  }

  @Test
  public void test_DATA_TYPE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(5)).isEqualTo("INTEGER");
  }

  @Test
  public void test_DATA_TYPE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(5)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_DATA_TYPE_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(5)).isEqualTo(Integer.class.getName());
  }

  @Test
  public void test_DATA_TYPE_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(5)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #6: TYPE_NAME:
  // - JDBC:  "6. ... String => Data source dependent type name, for a UDT the
  //     type name is fully qualified"
  // - Dremio:
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable?;

  @Test
  public void test_TYPE_NAME_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(6)).isEqualTo("TYPE_NAME");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("TYPE_NAME")).isEqualTo("BOOLEAN");
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_TYPE_NAME_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(mdrReqTINYINT.getString("TYPE_NAME")).isEqualTo("TINYINT");
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_TYPE_NAME_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(mdrOptSMALLINT.getString("TYPE_NAME")).isEqualTo("SMALLINT");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(mdrReqINTEGER.getString("TYPE_NAME")).isEqualTo("INTEGER");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(mdrOptBIGINT.getString("TYPE_NAME")).isEqualTo("BIGINT");
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_TYPE_NAME_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(mdrOptREAL.getString("TYPE_NAME")).isEqualTo("REAL");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(mdrOptFLOAT.getString("TYPE_NAME")).isEqualTo("FLOAT");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(mdrReqDOUBLE.getString("TYPE_NAME")).isEqualTo("DOUBLE");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(mdrReqDECIMAL_5_3.getString("TYPE_NAME")).isEqualTo("DECIMAL");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(mdrReqVARCHAR_10.getString("TYPE_NAME")).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(mdrOptVARCHAR.getString("TYPE_NAME")).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(mdrReqCHAR_5.getString("TYPE_NAME")).isEqualTo("CHARACTER");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(mdrOptVARBINARY_16.getString("TYPE_NAME")).isEqualTo("BINARY VARYING");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrOptBINARY_1048576CHECK() throws SQLException {
    assertThat(mdrOptBINARY_1048576.getString("TYPE_NAME")).isEqualTo("BINARY VARYING");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(mdrReqDATE.getString("TYPE_NAME")).isEqualTo("DATE");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(mdrReqTIME.getString("TYPE_NAME")).isEqualTo("TIME");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(mdrOptTIME_7.getString("TYPE_NAME")).isEqualTo("TIME");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(mdrOptTIMESTAMP.getString("TYPE_NAME")).isEqualTo("TIMESTAMP");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    // (What SQL standard specifies for DATA_TYPE in INFORMATION_SCHEMA.COLUMNS:)
    assertThat(mdrReqINTERVAL_Y.getString("TYPE_NAME")).isEqualTo("INTERVAL");
  }

  @Test
  public void test_TYPE_NAME_hasRightValue_mdrReqINTERVAL_H_S3() throws SQLException {
    // (What SQL standard specifies for DATA_TYPE in INFORMATION_SCHEMA.COLUMNS:)
    assertThat(mdrReqINTERVAL_3H_S1.getString("TYPE_NAME")).isEqualTo("INTERVAL");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_TYPE_NAME_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(mdrReqARRAY.getString("TYPE_NAME")).isEqualTo("ARRAY");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_TYPE_NAME_hasRightValue_tbdMAP() throws SQLException {
    assertThat(mdrReqMAP.getString("TYPE_NAME")).isEqualTo("MAP");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_TYPE_NAME_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(mdrUnkSTRUCT.getString("TYPE_NAME")).isEqualTo("STRUCT");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_TYPE_NAME_hasRightValue_tbdUnion() throws SQLException {
    assertThat(mdrUnkUnion.getString("TYPE_NAME")).isEqualTo("OTHER");
    fail("Expected value is not resolved yet.");
  }

  @Test
  public void test_TYPE_NAME_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(6)).isEqualTo("TYPE_NAME");
  }

  @Test
  public void test_TYPE_NAME_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(6)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_TYPE_NAME_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(6)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_TYPE_NAME_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(6)).isEqualTo(String.class.getName());
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_TYPE_NAME_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(6)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #7: COLUMN_SIZE:
  // - JDBC:  "7. ... int => column size."
  //     "The COLUMN_SIZE column specifies the column size for the given column.
  //      For numeric data, this is the maximum precision.
  //      For character data, this is the length in characters.
  //      For datetime datatypes, this is the length in characters of the String
  //        representation (assuming the maximum allowed precision of the
  //        fractional seconds component).
  //      For binary data, this is the length in bytes.
  //      For the ROWID datatype, this is the length in bytes.
  //      Null is returned for data types where the column size is not applicable."
  //   - "Maximum precision" seem to mean maximum number of digits that can
  //     appear.
  // - (Meta): INTEGER(?); Nullable;

  @Test
  public void test_COLUMN_SIZE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(7)).isEqualTo("COLUMN_SIZE");
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "COLUMN_SIZE")).isEqualTo(1);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqTINYINT() throws SQLException {
    // 8 nodes
    assertThat(getIntOrNull(mdrReqTINYINT, "COLUMN_SIZE")).isEqualTo(8);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrOptSMALLINT() throws SQLException {
    // 16 nodes
    assertThat(getIntOrNull(mdrOptSMALLINT, "COLUMN_SIZE")).isEqualTo(16);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTEGER() throws SQLException {
    // 32 nodes
    assertThat(getIntOrNull(mdrReqINTEGER, "COLUMN_SIZE")).isEqualTo(32);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrOptBIGINT() throws SQLException {
    // 64 nodes
    assertThat(getIntOrNull(mdrOptBIGINT, "COLUMN_SIZE")).isEqualTo(64);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrOptREAL() throws SQLException {
    // 24 nodes of precision
    assertThat(getIntOrNull(mdrOptREAL, "COLUMN_SIZE")).isEqualTo(24);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrOptFLOAT() throws SQLException {
    // 24 nodes of precision (same as REAL--current Dremio behavior)
    assertThat(getIntOrNull(mdrOptFLOAT, "COLUMN_SIZE")).isEqualTo(24);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqDOUBLE() throws SQLException {
    // 53 nodes of precision
    assertThat(getIntOrNull(mdrReqDOUBLE, "COLUMN_SIZE")).isEqualTo(53);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "COLUMN_SIZE")).isEqualTo(5);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "COLUMN_SIZE")).isEqualTo(10);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "COLUMN_SIZE")).isEqualTo(65536);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "COLUMN_SIZE")).isEqualTo(5);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "COLUMN_SIZE")).isEqualTo(16);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrOptBINARY_1048576() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "COLUMN_SIZE")).isEqualTo(1048576);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "COLUMN_SIZE")).isEqualTo(10);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "COLUMN_SIZE")).isEqualTo(8  /* HH:MM:SS */);
  }

  @Ignore("TODO(DRILL-3225): unignore when datetime precision is implemented")
  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "COLUMN_SIZE")).isEqualTo(
      8  /* HH:MM:SS */ + 1 /* '.' */ + 7 /* sssssss */);
  }

  @Test
  public void test_COLUMN_SIZE_hasINTERIMValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "COLUMN_SIZE")).isEqualTo(8  /* HH:MM:SS */);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "COLUMN_SIZE")).isEqualTo(
      19 /* YYYY-MM-DDTHH:MM:SS */);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "COLUMN_SIZE")).isEqualTo(4);  // "P12Y"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_3Y_Mo() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3Y_Mo, "COLUMN_SIZE")).isEqualTo(8);  // "P123Y12M"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_Mo() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Mo, "COLUMN_SIZE")).isEqualTo(4);  // "P12M"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_D() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_D, "COLUMN_SIZE")).isEqualTo(4);  // "P12D"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_4D_H() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_4D_H, "COLUMN_SIZE")).isEqualTo(10);  // "P1234DT12H"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_3D_Mi() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3D_Mi, "COLUMN_SIZE")).isEqualTo(12);  // "P123DT12H12M"
  }

  @Ignore("TODO(DRILL-3244): unignore when fractional secs. prec. is right")
  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_2D_S5() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_2D_S5, "COLUMN_SIZE")).isEqualTo(
      20);  // "P12DT12H12M12.12345S"
  }

  @Test
  public void test_COLUMN_SIZE_hasINTERIMValue_mdrReqINTERVAL_2D_S5() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_2D_S5, "COLUMN_SIZE")).isEqualTo(
      17);  // "P12DT12H12M12.12S"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_3H() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_H, "COLUMN_SIZE")).isEqualTo(5);  // "PT12H"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_4H_Mi() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_1H_Mi, "COLUMN_SIZE")).isEqualTo(7);  // "PT1H12M"
  }

  @Ignore("TODO(DRILL-3244): unignore when fractional secs. prec. is right")
  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_3H_S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "COLUMN_SIZE")).isEqualTo(
      14);  // "PT123H12M12.1S"
  }

  @Test
  public void test_COLUMN_SIZE_hasINTERIMValue_mdrReqINTERVAL_3H_S1() throws SQLException {
    assertThat(
      getIntOrNull(mdrReqINTERVAL_3H_S1, "COLUMN_SIZE")).isEqualTo(16);  // "PT123H12M12.123S"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_Mi() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Mi, "COLUMN_SIZE")).isEqualTo(5);  // "PT12M"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_5Mi_S() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_5Mi_S, "COLUMN_SIZE")).isEqualTo(
      18);  // "PT12345M12.123456S"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_S() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_S, "COLUMN_SIZE")).isEqualTo(12);  // "PT12.123456S"
  }

  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_3S() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S, "COLUMN_SIZE")).isEqualTo(13);  // "PT123.123456S"
  }

  @Ignore("TODO(DRILL-3244): unignore when fractional secs. prec. is right")
  @Test
  public void test_COLUMN_SIZE_hasRightValue_mdrReqINTERVAL_3S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S1, "COLUMN_SIZE")).isEqualTo(8);  // "PT123.1S"
  }

  @Test
  public void test_COLUMN_SIZE_hasINTERIMValue_mdrReqINTERVAL_3S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S1, "COLUMN_SIZE")).isEqualTo(10);  // "PT123.123S"
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_COLUMN_SIZE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "COLUMN_SIZE")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_COLUMN_SIZE_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "COLUMN_SIZE")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_COLUMN_SIZE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "COLUMN_SIZE")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_COLUMN_SIZE_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "COLUMN_SIZE")).isNull();
  }

  @Test
  public void test_COLUMN_SIZE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(7)).isEqualTo("COLUMN_SIZE");
  }

  @Test
  public void test_COLUMN_SIZE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(7)).isEqualTo("INTEGER");
  }

  @Test
  public void test_COLUMN_SIZE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(7)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_COLUMN_SIZE_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(7)).isEqualTo(Integer.class.getName());
  }

  @Test
  public void test_COLUMN_SIZE_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(7)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #8: BUFFER_LENGTH:
  // - JDBC:   "8. ... is not used"
  // - Dremio:
  // - (Meta):

  // Since "unused," check only certain meta-metadata.

  @Test
  public void test_BUFFER_LENGTH_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(8)).isEqualTo("BUFFER_LENGTH");
  }

  // No specific value or even type to check for.

  @Test
  public void test_BUFFER_LENGTH_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(8)).isEqualTo("BUFFER_LENGTH");
  }

  ////////////////////////////////////////////////////////////
  // #9: DECIMAL_DIGITS:
  // - JDBC:  "9. ... int => the number of fractional digits. Null is
  //     returned for data types where DECIMAL_DIGITS is not applicable."
  //   - Resolve:  When exactly null?
  // - Dremio:
  // - (Meta):  INTEGER(?); Nullable;

  @Test
  public void test_DECIMAL_DIGITS_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(9)).isEqualTo("DECIMAL_DIGITS");
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "DECIMAL_DIGITS")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "DECIMAL_DIGITS")).isEqualTo(0);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "DECIMAL_DIGITS")).isEqualTo(0);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "DECIMAL_DIGITS")).isEqualTo(0);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "DECIMAL_DIGITS")).isEqualTo(0);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "DECIMAL_DIGITS")).isEqualTo(7);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "DECIMAL_DIGITS")).isEqualTo(7);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "DECIMAL_DIGITS")).isEqualTo(15);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "DECIMAL_DIGITS")).isEqualTo(3);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "DECIMAL_DIGITS")).isNull();
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "DECIMAL_DIGITS")).isNull();
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "DECIMAL_DIGITS")).isNull();
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "DECIMAL_DIGITS")).isNull();
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrOptBINARY_1048576CHECK() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "DECIMAL_DIGITS")).isNull();
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqDATE() throws SQLException {
    // Zero because, per SQL spec.,  DATE doesn't (seem to) have a datetime
    // precision, but its DATETIME_PRECISION value must not be null.
    assertThat(getIntOrNull(mdrReqDATE, "DECIMAL_DIGITS")).isEqualTo(0);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqTIME() throws SQLException {
    // Zero is default datetime precision for TIME in SQL DATETIME_PRECISION.
    assertThat(getIntOrNull(mdrReqTIME, "DECIMAL_DIGITS")).isEqualTo(0);
  }

  @Ignore("TODO(DRILL-3225): unignore when datetime precision is implemented")
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "DECIMAL_DIGITS")).isEqualTo(7);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasINTERIMValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "DECIMAL_DIGITS")).isEqualTo(0);
  }

  @Ignore("TODO(DRILL-3225): unignore when datetime precision is implemented")
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    // 6 is default datetime precision for TIMESTAMP.
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "DECIMAL_DIGITS")).isEqualTo(0);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_3Y_Mo() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3Y_Mo, "DECIMAL_DIGITS")).isEqualTo(0);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_Mo() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Mo, "DECIMAL_DIGITS")).isEqualTo(0);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_D() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_D, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_4D_H() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_4D_H, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_3D_Mi() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_3D_Mi, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Ignore("TODO(DRILL-3244): unignore when fractional secs. prec. is right")
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_2D_S5() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_2D_S5, "DECIMAL_DIGITS")).isEqualTo(5);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasINTERIMValue_mdrReqINTERVAL_2D_S5() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_2D_S5, "DECIMAL_DIGITS")).isEqualTo(2);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_3H() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_H, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_1H_Mi() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_1H_Mi, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Ignore("TODO(DRILL-3244): unignore when fractional secs. prec. is right")
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_3H_S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "DECIMAL_DIGITS")).isEqualTo(1);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasINTERIMValue_mdrReqINTERVAL_3H_S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "DECIMAL_DIGITS")).isEqualTo(3);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_Mi() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_Mi, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_5Mi_S() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_5Mi_S, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_S() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_S, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_3S() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasINTERIMValue_mdrReqINTERVAL_3S() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Ignore("TODO(DRILL-3244): unignore when fractional secs. prec. is right")
  @Test
  public void test_DECIMAL_DIGITS_hasRightValue_mdrReqINTERVAL_3S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S, "DECIMAL_DIGITS")).isEqualTo(1);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasINTERIMValue_mdrReqINTERVAL_3S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S, "DECIMAL_DIGITS")).isEqualTo(6);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DECIMAL_DIGITS_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "DECIMAL_DIGITS")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DECIMAL_DIGITS_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "DECIMAL_DIGITS")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DECIMAL_DIGITS_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "DECIMAL_DIGITS")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DECIMAL_DIGITS_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "DECIMAL_DIGITS")).isNull();
  }

  @Test
  public void test_DECIMAL_DIGITS_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(9)).isEqualTo("DECIMAL_DIGITS");
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(9)).isEqualTo("INTEGER");
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(9)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(9)).isEqualTo(Integer.class.getName());
  }

  @Test
  public void test_DECIMAL_DIGITS_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(9)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #10: NUM_PREC_RADIX:
  // - JDBC:  "10. ... int => Radix (typically either 10 or 2)"
  //   - Seems should be null for non-numeric, but unclear.
  // - Dremio:  ?
  // - (Meta): INTEGER?; Nullable?;
  //
  // Note:  Some MS page says NUM_PREC_RADIX specifies the units (decimal digits
  // or binary nodes COLUMN_SIZE, and is NULL for non-numeric columns.

  @Test
  public void test_NUM_PREC_RADIX_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(10)).isEqualTo("NUM_PREC_RADIX");
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "NUM_PREC_RADIX")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "NUM_PREC_RADIX")).isEqualTo(2);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "NUM_PREC_RADIX")).isEqualTo(2);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "NUM_PREC_RADIX")).isEqualTo(2);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "NUM_PREC_RADIX")).isEqualTo(2);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "NUM_PREC_RADIX")).isEqualTo(2);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "NUM_PREC_RADIX")).isEqualTo(2);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "NUM_PREC_RADIX")).isEqualTo(2);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "NUM_PREC_RADIX")).isEqualTo(10);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "NUM_PREC_RADIX")).isNull();
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "NUM_PREC_RADIX")).isNull();
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "NUM_PREC_RADIX")).isNull();
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "NUM_PREC_RADIX")).isNull();
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrOptBINARY_1048576CHECK() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "NUM_PREC_RADIX")).isNull();
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "NUM_PREC_RADIX")).isEqualTo(10);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "NUM_PREC_RADIX")).isEqualTo(10);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "NUM_PREC_RADIX")).isEqualTo(10);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "NUM_PREC_RADIX")).isEqualTo(10);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "NUM_PREC_RADIX")).isEqualTo(10);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightValue_mdrReqINTERVAL_3H_S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "NUM_PREC_RADIX")).isEqualTo(10);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUM_PREC_RADIX_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "NUM_PREC_RADIX")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUM_PREC_RADIX_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "NUM_PREC_RADIX")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUM_PREC_RADIX_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "NUM_PREC_RADIX")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUM_PREC_RADIX_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "NUM_PREC_RADIX")).isNull();
  }

  @Test
  public void test_NUM_PREC_RADIX_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(10)).isEqualTo("NUM_PREC_RADIX");
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(10)).isEqualTo("INTEGER");
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(10)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(10)).isEqualTo(Integer.class.getName());
  }

  @Test
  public void test_NUM_PREC_RADIX_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(10)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #11: NULLABLE:
  // - JDBC:  "11. ... int => is NULL allowed.
  //     columnNoNulls - might not allow NULL values
  //     columnNullable - definitely allows NULL values
  //     columnNullableUnknown - nullability unknown"
  // - Dremio:
  // - (Meta): INTEGER(?); Non-nullable(?).

  @Test
  public void test_NULLABLE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(11)).isEqualTo("NULLABLE");
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_NULLABLE_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_NULLABLE_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_NULLABLE_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "NULLABLE")).isEqualTo(columnNullable);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "NULLABLE")).isEqualTo(columnNullable);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_NULLABLE_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "NULLABLE")).isEqualTo(columnNullable);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "NULLABLE")).isEqualTo(columnNullable);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "NULLABLE")).isEqualTo(columnNullable);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "NULLABLE")).isEqualTo(columnNullable);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrOptBINARY_1048576() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "NULLABLE")).isEqualTo(columnNullable);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "NULLABLE")).isEqualTo(columnNullable);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "NULLABLE")).isEqualTo(columnNullable);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Test
  public void test_NULLABLE_hasRightValue_mdrReqINTERVAL_3H_S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "NULLABLE")).isEqualTo(columnNoNulls);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NULLABLE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "NULLABLE")).isEqualTo(columnNoNulls);
    // To-do:  Determine which.
    assertThat(getIntOrNull(mdrReqARRAY, "NULLABLE")).isEqualTo(columnNullable);
    // To-do:  Determine which.
    assertThat(getIntOrNull(mdrReqARRAY, "NULLABLE")).isEqualTo(columnNullableUnknown);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NULLABLE_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "NULLABLE")).isEqualTo(columnNoNulls);
    // To-do:  Determine which.
    assertThat(getIntOrNull(mdrReqMAP, "NULLABLE")).isEqualTo(columnNullable);
    // To-do:  Determine which.
    assertThat(getIntOrNull(mdrReqMAP, "NULLABLE")).isEqualTo(columnNullableUnknown);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NULLABLE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "NULLABLE")).isEqualTo(columnNullable);
    // To-do:  Determine which.
    assertThat(getIntOrNull(mdrUnkSTRUCT, "NULLABLE")).isEqualTo(columnNoNulls);
    // To-do:  Determine which.
    assertThat(getIntOrNull(mdrUnkSTRUCT, "NULLABLE")).isEqualTo(columnNullableUnknown);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NULLABLE_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "NULLABLE")).isEqualTo(columnNullable);
    // To-do:  Determine which.
    assertThat(getIntOrNull(mdrUnkUnion, "NULLABLE")).isEqualTo(columnNoNulls);
    // To-do:  Determine which.
    assertThat(getIntOrNull(mdrUnkUnion, "NULLABLE")).isEqualTo(columnNullableUnknown);
  }

  @Test
  public void test_NULLABLE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(11)).isEqualTo("NULLABLE");
  }

  @Test
  public void test_NULLABLE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(11)).isEqualTo("INTEGER");
  }

  @Test
  public void test_NULLABLE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(11)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_NULLABLE_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(11)).isEqualTo(Integer.class.getName());
  }

  @Test
  public void test_NULLABLE_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(11)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #12: REMARKS:
  // - JDBC:  "12. ... String => comment describing column (may be null)"
  // - Dremio: none, so always null
  // - (Meta): VARCHAR (NVARCHAR?); Nullable;

  @Test
  public void test_REMARKS_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(12)).isEqualTo("REMARKS");
  }

  @Test
  public void test_REMARKS_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("REMARKS")).isNull();
  }

  @Test
  public void test_REMARKS_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(12)).isEqualTo("REMARKS");
  }

  @Test
  public void test_REMARKS_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(12)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_REMARKS_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(12)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_REMARKS_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(12)).isEqualTo(String.class.getName());
  }

  @Test
  public void test_REMARKS_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(12)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #13: COLUMN_DEF:
  // - JDBC:  "13. ... String => default value for the column, which should be
  //     interpreted as a string when the value is enclosed in single quotes
  //     (may be null)"
  // - Dremio:  no real default values, right?
  // - (Meta): VARCHAR (NVARCHAR?);  Nullable;

  @Test
  public void test_COLUMN_DEF_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(13)).isEqualTo("COLUMN_DEF");
  }

  @Test
  public void test_COLUMN_DEF_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("COLUMN_DEF")).isNull();
  }

  @Test
  public void test_COLUMN_DEF_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(13)).isEqualTo("COLUMN_DEF");
  }

  @Test
  public void test_COLUMN_DEF_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(13)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_COLUMN_DEF_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(13)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_COLUMN_DEF_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(13)).isEqualTo(String.class.getName()); //???Text
  }

  @Test
  public void test_COLUMN_DEF_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(13)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #14: SQL_DATA_TYPE:
  // - JDBC:  "14. ... int => unused"
  // - Dremio:
  // - (Meta): INTEGER(?);

  // Since "unused," check only certain meta-metadata.

  @Test
  public void test_SQL_DATA_TYPE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(14)).isEqualTo("SQL_DATA_TYPE");
  }

  // No specific value to check for.

  @Test
  public void test_SQL_DATA_TYPE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(14)).isEqualTo("SQL_DATA_TYPE");
  }

  @Test
  public void test_SQL_DATA_TYPE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(14)).isEqualTo("INTEGER");
  }

  @Test
  public void test_SQL_DATA_TYPE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(14)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_SQL_DATA_TYPE_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(14)).isEqualTo(Integer.class.getName());
  }

  ////////////////////////////////////////////////////////////
  // #15: SQL_DATETIME_SUB:
  // - JDBC:  "15. ... int => unused"
  // - Dremio:
  // - (Meta):  INTEGER(?);

  // Since "unused," check only certain meta-metadata.

  @Test
  public void test_SQL_DATETIME_SUB_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(15)).isEqualTo("SQL_DATETIME_SUB");
  }

  // No specific value to check for.

  @Test
  public void test_SQL_DATETIME_SUB_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(15)).isEqualTo("SQL_DATETIME_SUB");
  }

  @Test
  public void test_SQL_DATETIME_SUB_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(15)).isEqualTo("INTEGER");
  }

  @Test
  public void test_SQL_DATETIME_SUB_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(15)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_SQL_DATETIME_SUB_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(15)).isEqualTo(Integer.class.getName());
  }

  ////////////////////////////////////////////////////////////
  // #16: CHAR_OCTET_LENGTH:
  // - JDBC:  "16. ... int => for char types the maximum number of bytes
  //     in the column"
  //   - apparently should be null for non-character types
  // - Dremio:
  // - (Meta): INTEGER(?); Nullable(?);

  @Test
  public void test_CHAR_OCTET_LENGTH_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(16)).isEqualTo("CHAR_OCTET_LENGTH");
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "CHAR_OCTET_LENGTH"))
      .isEqualTo(10   /* chars. */
        * 4  /* max. UTF-8 bytes per char. */);
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "CHAR_OCTET_LENGTH")).isEqualTo(
      65536 /* chars. (default of 65536) */
        * 4  /* max. UTF-8 bytes per char. */);
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "CHAR_OCTET_LENGTH"))
      .isEqualTo(5    /* chars. */
        * 4  /* max. UTF-8 bytes per char. */);
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrOptBINARY_1048576CHECK() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightValue_mdrReqINTERVAL_3H_S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHAR_OCTET_LENGTH_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHAR_OCTET_LENGTH_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHAR_OCTET_LENGTH_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHAR_OCTET_LENGTH_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "CHAR_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(16)).isEqualTo("CHAR_OCTET_LENGTH");
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(16)).isEqualTo("INTEGER");
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(16)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(16)).isEqualTo(Integer.class.getName());
  }

  @Test
  public void test_CHAR_OCTET_LENGTH_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(16)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #17: ORDINAL_POSITION:
  // - JDBC:  "17. ... int => index of column in table (starting at 1)"
  // - Dremio:
  // - (Meta):  INTEGER(?); Non-nullable(?).

  @Test
  public void test_ORDINAL_POSITION_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(17)).isEqualTo("ORDINAL_POSITION");
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "ORDINAL_POSITION")).isEqualTo(1);
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "ORDINAL_POSITION")).isEqualTo(2);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "ORDINAL_POSITION")).isEqualTo(3);
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "ORDINAL_POSITION")).isEqualTo(4);
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "ORDINAL_POSITION")).isEqualTo(5);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "ORDINAL_POSITION")).isEqualTo(6);
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "ORDINAL_POSITION")).isEqualTo(7);
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "ORDINAL_POSITION")).isEqualTo(8);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_ORDINAL_POSITION_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "ORDINAL_POSITION")).isEqualTo(14);
  }

  @Test
  public void test_ORDINAL_POSITION_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(17)).isEqualTo("ORDINAL_POSITION");
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(17)).isEqualTo("INTEGER");
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(17)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(17)).isEqualTo(Integer.class.getName());
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(17)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #18: IS_NULLABLE:
  // - JDBC:  "18. ... String => ISO rules are used to determine the nullability for a column.
  //     YES --- if the column can include NULLs
  //     NO --- if the column cannot include NULLs
  //     empty string --- if the nullability for the column is unknown"
  // - Dremio:  ?
  // - (Meta): VARCHAR (NVARCHAR?); Not nullable?

  @Test
  public void test_IS_NULLABLE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(18)).isEqualTo("IS_NULLABLE");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("IS_NULLABLE")).isEqualTo("YES");
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(mdrReqTINYINT.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(mdrOptSMALLINT.getString("IS_NULLABLE")).isEqualTo("YES");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(mdrReqINTEGER.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(mdrOptBIGINT.getString("IS_NULLABLE")).isEqualTo("YES");
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(mdrOptREAL.getString("IS_NULLABLE")).isEqualTo("YES");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(mdrOptFLOAT.getString("IS_NULLABLE")).isEqualTo("YES");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(mdrReqDOUBLE.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(mdrReqDECIMAL_5_3.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(mdrReqVARCHAR_10.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(mdrOptVARCHAR.getString("IS_NULLABLE")).isEqualTo("YES");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(mdrReqCHAR_5.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(mdrOptVARBINARY_16.getString("IS_NULLABLE")).isEqualTo("YES");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrOptBINARY_1048576CHECK() throws SQLException {
    assertThat(mdrOptBINARY_1048576.getString("IS_NULLABLE")).isEqualTo("YES");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(mdrReqDATE.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(mdrReqTIME.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(mdrOptTIME_7.getString("IS_NULLABLE")).isEqualTo("YES");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(mdrOptTIMESTAMP.getString("IS_NULLABLE")).isEqualTo("YES");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(mdrReqINTERVAL_Y.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  public void test_IS_NULLABLE_hasRightValue_mdrReqINTERVAL_3H_S1() throws SQLException {
    assertThat(mdrReqINTERVAL_3H_S1.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_IS_NULLABLE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(mdrReqARRAY.getString("IS_NULLABLE")).isEqualTo("YES");
    // To-do:  Determine which.
    assertThat(mdrReqARRAY.getString("IS_NULLABLE")).isEqualTo("NO");
    // To-do:  Determine which.
    assertThat(mdrReqARRAY.getString("IS_NULLABLE")).isEqualTo("");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_IS_NULLABLE_hasRightValue_tbdMAP() throws SQLException {
    assertThat(mdrReqMAP.getString("IS_NULLABLE")).isEqualTo("YES");
    // To-do:  Determine which.
    assertThat(mdrReqMAP.getString("IS_NULLABLE")).isEqualTo("NO");
    // To-do:  Determine which.
    assertThat(mdrReqMAP.getString("IS_NULLABLE")).isEqualTo("");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_IS_NULLABLE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(mdrUnkSTRUCT.getString("IS_NULLABLE")).isEqualTo("YES");
    // To-do:  Determine which.
    assertThat(mdrUnkSTRUCT.getString("IS_NULLABLE")).isEqualTo("NO");
    // To-do:  Determine which.
    assertThat(mdrUnkSTRUCT.getString("IS_NULLABLE")).isEqualTo("");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_IS_NULLABLE_hasRightValue_tbdUnion() throws SQLException {
    assertThat(mdrUnkUnion.getString("IS_NULLABLE")).isEqualTo("YES");
    // To-do:  Determine which.
    assertThat(mdrUnkUnion.getString("IS_NULLABLE")).isEqualTo("NO");
    // To-do:  Determine which.
    assertThat(mdrUnkUnion.getString("IS_NULLABLE")).isEqualTo("");
  }

  @Test
  public void test_IS_NULLABLE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(18)).isEqualTo("IS_NULLABLE");
  }

  @Test
  public void test_IS_NULLABLE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(18)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_IS_NULLABLE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(18)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_IS_NULLABLE_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(18)).isEqualTo(String.class.getName());
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_IS_NULLABLE_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(18)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #19: SCOPE_CATALOG:
  // - JDBC:  "19. ... String => catalog of table that is the scope of a
  //     reference attribute (null if DATA_TYPE isn't REF)"
  // - Dremio:
  // - (Meta): VARCHAR (NVARCHAR?); Nullable;

  @Test
  public void test_SCOPE_CATALOG_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(19)).isEqualTo("SCOPE_CATALOG");
  }

  @Test
  public void test_SCOPE_CATALOG_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    final String value = mdrOptBOOLEAN.getString("SCOPE_SCHEMA");
    assertThat(value).isNull();
  }

  @Test
  public void test_SCOPE_CATALOG_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(19)).isEqualTo("SCOPE_CATALOG");
  }

  @Test
  public void test_SCOPE_CATALOG_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(19)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_SCOPE_CATALOG_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(19)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_SCOPE_CATALOG_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(19)).isEqualTo(String.class.getName());
  }

  @Test
  public void test_SCOPE_CATALOG_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(19)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #20: SCOPE_SCHEMA:
  // - JDBC:  "20. ... String => schema of table that is the scope of a
  //     reference attribute (null if the DATA_TYPE isn't REF)"
  // - Dremio:  no REF, so always null?
  // - (Meta): VARCHAR?; Nullable;

  @Test
  public void test_SCOPE_SCHEMA_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(20)).isEqualTo("SCOPE_SCHEMA");
  }

  @Test
  public void test_SCOPE_SCHEMA_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    final String value = mdrOptBOOLEAN.getString("SCOPE_SCHEMA");
    assertThat(value).isNull();
  }

  @Test
  public void test_SCOPE_SCHEMA_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(20)).isEqualTo("SCOPE_SCHEMA");
  }

  @Test
  public void test_SCOPE_SCHEMA_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(20)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_SCOPE_SCHEMA_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(20)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_SCOPE_SCHEMA_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(20)).isEqualTo(String.class.getName());
  }

  @Test
  public void test_SCOPE_SCHEMA_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(20)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #21: SCOPE_TABLE:
  // - JDBC:  "21. ... String => table name that this the scope of a reference
  //     attribute (null if the DATA_TYPE isn't REF)"
  // - Dremio:
  // - (Meta): VARCHAR (NVARCHAR?); Nullable;

  @Test
  public void test_SCOPE_TABLE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(21)).isEqualTo("SCOPE_TABLE");
  }

  @Test
  public void test_SCOPE_TABLE_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    final String value = mdrOptBOOLEAN.getString("SCOPE_TABLE");
    assertThat(value).isNull();
  }

  @Test
  public void test_SCOPE_TABLE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(21)).isEqualTo("SCOPE_TABLE");
  }

  @Test
  public void test_SCOPE_TABLE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(21)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_SCOPE_TABLE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(21)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_SCOPE_TABLE_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(21)).isEqualTo(String.class.getName());
  }

  @Test
  public void test_SCOPE_TABLE_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(21)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #22: SOURCE_DATA_TYPE:
  // - JDBC:  "22. ... short => source type of a distinct type or user-generated
  //     Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't
  //     DISTINCT or user-generated REF)"
  // - Dremio:  not DISTINCT or REF, so null?
  // - (Meta): SMALLINT(?);  Nullable;

  @Test
  public void test_SOURCE_DATA_TYPE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(22)).isEqualTo("SOURCE_DATA_TYPE");
  }

  @Test
  public void test_SOURCE_DATA_TYPE_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("SOURCE_DATA_TYPE")).isNull();
  }

  @Test
  public void test_SOURCE_DATA_TYPE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(22)).isEqualTo("SOURCE_DATA_TYPE");
  }

  @Test
  public void test_SOURCE_DATA_TYPE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(22)).isEqualTo("SMALLINT");
  }

  @Test
  public void test_SOURCE_DATA_TYPE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(22)).isEqualTo(Types.SMALLINT);
  }

  @Test
  public void test_SOURCE_DATA_TYPE_hasRightClass() throws SQLException {
    assertThat(rowsMetadata.getColumnClassName(22)).isEqualTo(Short.class.getName());
  }

  @Test
  public void test_SOURCE_DATA_TYPE_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(22)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #23: IS_AUTOINCREMENT:
  // - JDBC:  "23. ... String => Indicates whether this column is auto incremented
  //     YES --- if the column is auto incremented
  //     NO --- if the column is not auto incremented
  //     empty string --- if it cannot be determined whether the column is auto incremented"
  // - Dremio:
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable(?);

  @Test
  public void test_IS_AUTOINCREMENT_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(23)).isEqualTo("IS_AUTOINCREMENT");
  }

  @Test
  public void test_IS_AUTOINCREMENT_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    // TODO:  Can it be 'NO' (not auto-increment) rather than '' (unknown)?
    assertThat(mdrOptBOOLEAN.getString("IS_AUTOINCREMENT")).isEqualTo("");
  }

  // Not bothering with other test columns for IS_AUTOINCREMENT.

  @Test
  public void test_IS_AUTOINCREMENT_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(23)).isEqualTo("IS_AUTOINCREMENT");
  }

  @Test
  public void test_IS_AUTOINCREMENT_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(23)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_IS_AUTOINCREMENT_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(23)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_IS_AUTOINCREMENT_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(23)).isEqualTo(String.class.getName());
  }

  @Test
  public void test_IS_AUTOINCREMENT_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(23)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #24: IS_GENERATEDCOLUMN:
  // - JDBC:  "24. ... String => Indicates whether this is a generated column
  //     YES --- if this a generated column
  //     NO --- if this not a generated column
  //     empty string --- if it cannot be determined whether this is a generated column"
  // - Dremio:
  // - (Meta): VARCHAR (NVARCHAR?); Non-nullable(?)

  @Test
  public void test_IS_GENERATEDCOLUMN_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(24)).isEqualTo("IS_GENERATEDCOLUMN");
  }

  @Test
  public void test_IS_GENERATEDCOLUMN_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    // TODO:  Can it be 'NO' (not auto-increment) rather than '' (unknown)?
    assertThat(mdrOptBOOLEAN.getString("IS_GENERATEDCOLUMN")).isEqualTo("");
  }

  // Not bothering with other test columns for IS_GENERATEDCOLUMN.

  @Test
  public void test_IS_GENERATEDCOLUMN_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(24)).isEqualTo("IS_GENERATEDCOLUMN");
  }

  @Test
  public void test_IS_GENERATEDCOLUMN_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(24)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_IS_GENERATEDCOLUMN_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(24)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_IS_GENERATEDCOLUMN_hasRightClass() throws SQLException {

    assertThat(rowsMetadata.getColumnClassName(24)).isEqualTo(String.class.getName());
  }

  @Test
  public void test_IS_GENERATEDCOLUMN_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(24)).isEqualTo(columnNoNulls);
  }

} // class DatabaseMetaGetColumnsDataTest
