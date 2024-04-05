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

import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.sql.ResultSetMetaData.columnNullable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.dremio.jdbc.JdbcWithServerTestBase;
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

// TODO:  Review nullability (NULLABLE and IS_NULLABLE columns):
// - It's not clear why Dremio sets INFORMATION_SCHEMA.COLUMNS.IS_NULLABLE to
// 'NO' for some columns that contain only null (e.g., for
// "CREATE VIEW x AS SELECT CAST(NULL AS ...) ..."

/** Test class for Dremio's INFORMATION_SCHEMA.COLUMNS implementation. */
@Ignore("DX-2490")
public class TestInformationSchemaColumns extends JdbcWithServerTestBase {
  private static final String VIEW_SCHEMA = "dfs_test";
  private static final String VIEW_NAME =
      TestInformationSchemaColumns.class.getSimpleName() + "_View";

  /** Result set metadata. For checking columns themselves (not cell values or row order). */
  private static ResultSetMetaData rowsMetadata;

  ////////////////////
  // Results from INFORMATION_SCHEMA.COLUMN for test columns of various types.
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

  /**
   * Gets SQL &lt;character string literal> representing given string value.
   *
   * <p>(Encodes {@code '} as {@code ''} and every other character as itself.)
   */
  private static String encodeAsSqlCharStrLiteral(String value) {
    return "'" + value.replaceAll("'", "''") + "'";
  }

  private static ResultSet setUpRow(
      final String schemaName, final String tableOrViewName, final String columnName)
      throws SQLException {
    System.out.println("(Setting up row for " + tableOrViewName + "." + columnName + ".)");
    final Statement stmt = getConnection().createStatement();
    final ResultSet mdrUnk =
        stmt.executeQuery(
            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_CATALOG = "
                + encodeAsSqlCharStrLiteral("DREMIO")
                + "  AND TABLE_SCHEMA  = "
                + encodeAsSqlCharStrLiteral(schemaName)
                + "  AND TABLE_NAME    = "
                + encodeAsSqlCharStrLiteral(tableOrViewName)
                + "  AND COLUMN_NAME   = "
                + encodeAsSqlCharStrLiteral(columnName));
    assertThat(mdrUnk.next())
        .as(
            "Test setup error:  No row for column DREMIO . \""
                + schemaName
                + "\" . \""
                + tableOrViewName
                + "\" . \""
                + columnName
                + "\"")
        .isTrue();
    return mdrUnk;
  }

  @BeforeClass
  public static void setUpConnection() throws SQLException {

    // Get JDBC connection to Dremio:
    // (Note: Can't use JdbcTest's connect(...) because JdbcTest closes
    // Connection--and other JDBC objects--on test method failure, but this test
    // class uses some objects across methods.)
    JdbcWithServerTestBase.setUpConnection();
    final Statement stmt = getConnection().createStatement();

    ResultSet util;

    /* TODO(DRILL-3253)(start): Update this once we have test plugin supporting all needed types.
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
    util = stmt.executeQuery("USE dfs_test.tmp");
    assertThat(util.next()).isTrue();
    assertThat(util.getBoolean(1))
        .as("Error setting schema for test: " + util.getString(2))
        .isTrue();

    // TODO(DRILL-2470): Adjust when TINYINT is implemented:
    // TODO(DRILL-2470): Adjust when SMALLINT is implemented:
    // TODO(DRILL-2683): Adjust when REAL is implemented:
    util =
        stmt.executeQuery(
            ""
                + "CREATE OR REPLACE VIEW "
                + VIEW_NAME
                + " AS SELECT "
                + "\n  CAST( NULL    AS BOOLEAN            ) AS mdrOptBOOLEAN,        "
                + "\n  "
                + "\n  CAST(    1    AS INT            ) AS mdrReqTINYINT,        "
                + "\n  CAST( NULL    AS INT           ) AS mdrOptSMALLINT,       "
                // + "\n  CAST(    1    AS TINYINT            ) AS mdrReqTINYINT,        "
                // + "\n  CAST( NULL    AS SMALLINT           ) AS mdrOptSMALLINT,       "
                + "\n  CAST(    2    AS INTEGER            ) AS mdrReqINTEGER,        "
                + "\n  CAST( NULL    AS BIGINT             ) AS mdrOptBIGINT,         "
                + "\n  "
                + "\n  CAST( NULL    AS FLOAT               ) AS mdrOptREAL,           "
                // + "\n  CAST( NULL    AS REAL               ) AS mdrOptREAL,           "
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
                + "\nLIMIT 1 ");
    assertThat(util.next()).isTrue();
    assertThat(util.getBoolean(1))
        .as("Error creating temporary test-columns view " + VIEW_NAME + ": " + util.getString(2))
        .isTrue();

    // Set up result rows for temporary test view and Hivetest columns:

    mdrOptBOOLEAN = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrOptBOOLEAN");

    // TODO(DRILL-2470): Uncomment when TINYINT is implemented:
    // mdrReqTINYINT        = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrReqTINYINT" );
    // TODO(DRILL-2470): Uncomment when SMALLINT is implemented:
    // mdrOptSMALLINT       = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptSMALLINT" );
    mdrReqINTEGER = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTEGER");
    mdrOptBIGINT = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrOptBIGINT");

    // TODO(DRILL-2683): Uncomment when REAL is implemented:
    // mdrOptREAL           = setUpRow( VIEW_SCHEMA, VIEW_NAME, "mdrOptREAL" );
    mdrOptFLOAT = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrOptFLOAT");
    mdrReqDOUBLE = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqDOUBLE");

    mdrReqDECIMAL_5_3 = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqDECIMAL_5_3");

    mdrReqVARCHAR_10 = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqVARCHAR_10");
    mdrOptVARCHAR = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrOptVARCHAR");
    mdrReqCHAR_5 = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqCHAR_5");
    mdrOptVARBINARY_16 = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrOptVARBINARY_16");
    mdrOptBINARY_1048576 = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrOptBINARY_1048576");

    mdrReqDATE = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqDATE");
    mdrReqTIME = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqTIME");
    mdrOptTIME_7 = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrOptTIME_7");
    mdrOptTIMESTAMP = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrOptTIMESTAMP");

    mdrReqINTERVAL_Y = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_Y");
    mdrReqINTERVAL_3Y_Mo = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_3Y_Mo");
    mdrReqINTERVAL_Mo = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_Mo");
    mdrReqINTERVAL_D = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_D");
    mdrReqINTERVAL_4D_H = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_4D_H");
    mdrReqINTERVAL_3D_Mi = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_3D_Mi");
    mdrReqINTERVAL_2D_S5 = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_2D_S5");
    mdrReqINTERVAL_H = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_H");
    mdrReqINTERVAL_1H_Mi = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_1H_Mi");
    mdrReqINTERVAL_3H_S1 = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_3H_S1");
    mdrReqINTERVAL_Mi = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_Mi");
    mdrReqINTERVAL_5Mi_S = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_5Mi_S");
    mdrReqINTERVAL_S = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_S");
    mdrReqINTERVAL_3S = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_3S");
    mdrReqINTERVAL_3S1 = setUpRow(VIEW_SCHEMA, VIEW_NAME, "mdrReqINTERVAL_3S1");

    /* TODO(DRILL-3253)(start): Update this once we have test plugin supporting all needed types.
    mdrReqARRAY   = setUpRow( "hive_test.default", "infoschematest", "listtype" );
    mdrReqMAP     = setUpRow( "hive_test.default", "infoschematest", "maptype" );
    mdrUnkSTRUCT = setUpRow( "hive_test.default", "infoschematest", "structtype" );
    mdrUnkUnion  = setUpRow( "hive_test.default", "infoschematest", "uniontypetype" );
    TODO(DRILL-3253)(end) */

    // Set up result set's metadata:

    // Get all columns for more diversity of values (e.g., nulls and non-nulls),
    // in case that affects metadata (e.g., nullability) (in future).
    final Statement stmt2 = getConnection().createStatement();
    final ResultSet allColumns = stmt2.executeQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS ");
    rowsMetadata = allColumns.getMetaData();
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    final ResultSet util =
        getConnection().createStatement().executeQuery("DROP VIEW " + VIEW_NAME + "");
    assertThat(util.next()).isTrue();
    assertThat(util.getBoolean(1))
        .as("Error dropping temporary test-columns view " + VIEW_NAME + ": " + util.getString(2))
        .isTrue();
    JdbcWithServerTestBase.tearDownConnection();
  }

  private Integer getIntOrNull(ResultSet row, String columnName) throws SQLException {
    final int value = row.getInt(columnName);
    return row.wasNull() ? null : value;
  }

  //////////////////////////////////////////////////////////////////////
  // Tests:

  // INFORMATION_SCHEMA.COLUMNS column positions:
  //
  // 1. TABLE_CATALOG
  // 2. TABLE_SCHEMA
  // 3. TABLE_NAME
  // 4. COLUMN_NAME
  // 5. ORDINAL_POSITION
  // 6. COLUMN_DEFAULT
  // 7. IS_NULLABLE
  // 8. DATA_TYPE
  // 9. CHARACTER_MAXIMUM_LENGTH
  // 10. CHARACTER_OCTET_LENGTH
  // 11. NUMERIC_PRECISION
  // 12. NUMERIC_PRECISION_RADIX
  // 13. NUMERIC_SCALE
  // 14. DATETIME_PRECISION
  // 15. INTERVAL_TYPE
  // 16. INTERVAL_PRECISION
  // 17. CHARACTER_SET_CATALOG
  // 18. CHARACTER_SET_SCHEMA
  // 19. CHARACTER_SET_NAME
  // 20. COLLATION_CATALOG
  // 21. COLLATION_SCHEMA
  // 22. COLLATION_NAME
  // 23. DOMAIN_CATALOG
  // 24. DOMAIN_SCHEMA
  // 25. DOMAIN_NAME
  // 26. UDT_CATALOG
  // 27. UDT_SCHEMA
  // 28. UDT_NAME
  // 29. SCOPE_CATALOG
  // 30. SCOPE_SCHEMA
  // 31. SCOPE_NAME
  // 32. MAXIMUM_CARDINALITY
  // 33. DTD_IDENTIFIER
  // 34. IS_SELF_REFERENCING
  // 35. IS_IDENTITY
  // 36. IDENTITY_GENERATION
  // 37. IDENTITY_START
  // 38. IDENTITY_INCREMENT
  // 39. IDENTITY_MAXIMUM
  // 40. IDENTITY_MINIMUM
  // 41. IDENTITY_CYCLE
  // 42. IS_GENERATED
  // 43. GENERATION_EXPRESSION
  // 44. IS_SYSTEM_TIME_PERIOD_START
  // 45. IS_SYSTEM_TIME_PERIOD_END
  // 46. SYSTEM_TIME_PERIOD_TIMESTAMP_GENERATION
  // 47. IS_UPDATABLE
  // 48. DECLARED_DATA_TYPE
  // 49. DECLARED_NUMERIC_PRECISION
  // 50. DECLARED_NUMERIC_SCALE

  ////////////////////////////////////////////////////////////
  // Number of columns.

  @Ignore("unless and until all COLUMNS columns are present")
  @Test
  public void testMetadataHasRightNumberOfColumns() throws SQLException {
    // Is this check valid?  (Are extra columns allowed?)
    assertThat(rowsMetadata.getColumnCount()).isEqualTo(50);
  }

  @Test
  public void testMetadataHasInterimNumberOfColumns() throws SQLException {
    assertThat(rowsMetadata.getColumnCount()).isEqualTo(16);
  }

  ////////////////////////////////////////////////////////////
  // #1: TABLE_CATALOG:
  // - SQL:
  // - Dremio:  Always "DREMIO".
  // - (Meta): VARCHAR; Non-nullable(?);

  @Test
  public void test_TABLE_CATALOG_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(1)).isEqualTo("TABLE_CATALOG");
  }

  @Test
  public void test_TABLE_CATALOG_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("TABLE_CATALOG")).isEqualTo("DREMIO");
  }

  // Not bothering with other test columns for TABLE_CAT.

  @Test
  public void test_TABLE_CATALOG_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(1)).isEqualTo("TABLE_CATALOG");
  }

  @Test
  public void test_TABLE_CATALOG_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(1)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_TABLE_CATALOG_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(1)).isEqualTo(Types.VARCHAR);
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_TABLE_CATALOG_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(1)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #2: TABLE_SCHEMA:
  // - SQL:
  // - Dremio:
  // - (Meta): VARCHAR; Non-nullable(?);

  @Test
  public void test_TABLE_SCHEMA_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(2)).isEqualTo("TABLE_SCHEMA");
  }

  @Test
  public void test_TABLE_SCHEMA_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("TABLE_SCHEMA")).isEqualTo(VIEW_SCHEMA);
  }

  // Not bothering with other _local_view_ test columns for TABLE_SCHEM.

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_TABLE_SCHEMA_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(mdrReqARRAY.getString("TABLE_SCHEMA")).isEqualTo("hive_test.default");
  }

  // Not bothering with other Hive test columns for TABLE_SCHEM.

  @Test
  public void test_TABLE_SCHEMA_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(2)).isEqualTo("TABLE_SCHEMA");
  }

  @Test
  public void test_TABLE_SCHEMA_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(2)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_TABLE_SCHEMA_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(2)).isEqualTo(Types.VARCHAR);
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_TABLE_SCHEMA_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(2)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #3: TABLE_NAME:
  // - SQL:
  // - Dremio:
  // - (Meta): VARCHAR; Non-nullable(?);

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

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_TABLE_NAME_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(3)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #4: COLUMN_NAME:
  // - SQL:
  // - Dremio:
  // - (Meta): VARCHAR; Non-nullable(?);

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
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
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

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_COLUMN_NAME_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(4)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #5: ORDINAL_POSITION:
  // - SQL:
  // - Dremio:
  // - (Meta):  INTEGER NOT NULL

  @Test
  public void test_ORDINAL_POSITION_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(5)).isEqualTo("ORDINAL_POSITION");
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getInt("ORDINAL_POSITION")).isEqualTo(1);
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(mdrReqTINYINT.getInt("ORDINAL_POSITION")).isEqualTo(2);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(mdrOptSMALLINT.getInt("ORDINAL_POSITION")).isEqualTo(3);
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(mdrReqINTEGER.getInt("ORDINAL_POSITION")).isEqualTo(4);
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(mdrOptBIGINT.getInt("ORDINAL_POSITION")).isEqualTo(5);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(mdrOptREAL.getInt("ORDINAL_POSITION")).isEqualTo(6);
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(mdrOptFLOAT.getInt("ORDINAL_POSITION")).isEqualTo(7);
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(mdrReqDOUBLE.getInt("ORDINAL_POSITION")).isEqualTo(8);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_ORDINAL_POSITION_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(mdrReqARRAY.getInt("ORDINAL_POSITION")).isEqualTo(14);
  }

  @Test
  public void test_ORDINAL_POSITION_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(5)).isEqualTo("ORDINAL_POSITION");
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(5)).isEqualTo("INTEGER");
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(5)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_ORDINAL_POSITION_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(5)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #6: COLUMN_DEFAULT:
  // - SQL:
  // - Dremio:
  // - (Meta): VARCHAR; Nullable;

  @Test
  public void test_COLUMN_DEFAULT_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(6)).isEqualTo("COLUMN_DEFAULT");
  }

  @Test
  public void test_COLUMN_DEFAULT_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("COLUMN_DEFAULT")).isNull();
  }

  // Not bothering with other _local_view_ test columns for COLUMN_DEFAULT.

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_COLUMN_DEFAULT_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(mdrReqARRAY.getString("COLUMN_DEFAULT")).isNull();
  }

  // Not bothering with other Hive test columns for TABLE_SCHEM.

  @Test
  public void test_COLUMN_DEFAULT_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(6)).isEqualTo("COLUMN_DEFAULT");
  }

  @Test
  public void test_COLUMN_DEFAULT_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(6)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_COLUMN_DEFAULT_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(6)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_COLUMN_DEFAULT_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(6)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #7: IS_NULLABLE:
  // - SQL:
  //   YES  The column is possibly nullable.
  //   NO   The column is known not nullable.
  // - Dremio:
  // - (Meta): VARCHAR; Non-nullable(?).

  @Test
  public void test_IS_NULLABLE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(7)).isEqualTo("IS_NULLABLE");
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
  public void test_IS_NULLABLE_hasRightValue_mdrOptBINARY_1048576() throws SQLException {
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
  public void test_IS_NULLABLE_hasRightValue_mdrReqINTERVAL_H_S3() throws SQLException {
    assertThat(mdrReqINTERVAL_3H_S1.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_IS_NULLABLE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(mdrReqARRAY.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_IS_NULLABLE_hasRightValue_tbdMAP() throws SQLException {
    assertThat(mdrReqMAP.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  @Test
  public void test_IS_NULLABLE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(mdrUnkSTRUCT.getString("IS_NULLABLE")).isEqualTo("YES");
    // To-do:  Determine which.
    assertThat(mdrUnkSTRUCT.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  @Test
  public void test_IS_NULLABLE_hasRightValue_tbdUnion() throws SQLException {
    assertThat(mdrUnkUnion.getString("IS_NULLABLE")).isEqualTo("YES");
    // To-do:  Determine which.
    assertThat(mdrUnkUnion.getString("IS_NULLABLE")).isEqualTo("NO");
  }

  @Test
  public void test_IS_NULLABLE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(7)).isEqualTo("IS_NULLABLE");
  }

  @Test
  public void test_IS_NULLABLE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(7)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_IS_NULLABLE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(7)).isEqualTo(Types.VARCHAR);
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_IS_NULLABLE_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(7)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #8: DATA_TYPE:
  // - SQL:
  // - Dremio:
  // - (Meta): VARCHAR; Non-nullable(?);

  @Test
  public void test_DATA_TYPE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(8)).isEqualTo("DATA_TYPE");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("DATA_TYPE")).isEqualTo("BOOLEAN");
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(mdrReqTINYINT.getString("DATA_TYPE")).isEqualTo("TINYINT");
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(mdrOptSMALLINT.getString("DATA_TYPE")).isEqualTo("SMALLINT");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(mdrReqINTEGER.getString("DATA_TYPE")).isEqualTo("INTEGER");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(mdrOptBIGINT.getString("DATA_TYPE")).isEqualTo("BIGINT");
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(mdrOptREAL.getString("DATA_TYPE")).isEqualTo("REAL");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(mdrOptFLOAT.getString("DATA_TYPE")).isEqualTo("FLOAT");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(mdrReqDOUBLE.getString("DATA_TYPE")).isEqualTo("DOUBLE");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(mdrReqDECIMAL_5_3.getString("DATA_TYPE")).isEqualTo("DECIMAL");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(mdrReqVARCHAR_10.getString("DATA_TYPE")).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(mdrOptVARCHAR.getString("DATA_TYPE")).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(mdrReqCHAR_5.getString("DATA_TYPE")).isEqualTo("CHARACTER");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(mdrOptVARBINARY_16.getString("DATA_TYPE")).isEqualTo("BINARY VARYING");
  }

  @Ignore("TODO(DRILL-3368): unignore when BINARY is implemented enough")
  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptBINARY_1048576() throws SQLException {
    assertThat(mdrOptBINARY_1048576.getString("DATA_TYPE"))
        .isEqualTo("BINARY VARYING"); // ?? current
    assertThat(mdrOptBINARY_1048576.getString("DATA_TYPE")).isEqualTo("BINARY"); // ?? should be
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(mdrReqDATE.getString("DATA_TYPE")).isEqualTo("DATE");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqTIME() throws SQLException {
    // (TIME defaults to TIME WITHOUT TIME ZONE, which uses "TIME" here.)
    assertThat(mdrReqTIME.getString("DATA_TYPE")).isEqualTo("TIME");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptTIME_7() throws SQLException {
    // (TIME defaults to TIME WITHOUT TIME ZONE, which uses "TIME" here.)
    assertThat(mdrOptTIME_7.getString("DATA_TYPE")).isEqualTo("TIME");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(mdrOptTIMESTAMP.getString("DATA_TYPE")).isEqualTo("TIMESTAMP");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(mdrReqINTERVAL_Y.getString("DATA_TYPE")).isEqualTo("INTERVAL");
  }

  @Test
  public void test_DATA_TYPE_hasRightValue_mdrReqINTERVAL_H_S3() throws SQLException {
    assertThat(mdrReqINTERVAL_3H_S1.getString("DATA_TYPE")).isEqualTo("INTERVAL");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATA_TYPE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(mdrReqARRAY.getString("DATA_TYPE")).isEqualTo("ARRAY");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATA_TYPE_hasRightValue_tbdMAP() throws SQLException {
    assertThat(mdrReqMAP.getString("DATA_TYPE")).isEqualTo("MAP");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATA_TYPE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(mdrUnkSTRUCT.getString("DATA_TYPE")).isEqualTo("STRUCT");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATA_TYPE_hasRightValue_tbdUnion() throws SQLException {
    assertThat(mdrUnkUnion.getString("DATA_TYPE")).isEqualTo("OTHER");
    fail("Expected value is not resolved yet.");
  }

  @Test
  public void test_DATA_TYPE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(8)).isEqualTo("DATA_TYPE");
  }

  @Test
  public void test_DATA_TYPE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(8)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_DATA_TYPE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(8)).isEqualTo(Types.VARCHAR);
  }

  @Ignore("until resolved:  any requirement on nullability (DRILL-2420?)")
  @Test
  public void test_DATA_TYPE_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(8)).isEqualTo(columnNoNulls);
  }

  ////////////////////////////////////////////////////////////
  // #9: CHARACTER_MAXIMUM_LENGTH:
  // - SQL:
  // - Dremio:
  // - (Meta): INTEGER; Nullable;

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(9)).isEqualTo("CHARACTER_MAXIMUM_LENGTH");
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "CHARACTER_MAXIMUM_LENGTH")).isEqualTo(10);
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "CHARACTER_MAXIMUM_LENGTH")).isEqualTo(65536);
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "CHARACTER_MAXIMUM_LENGTH")).isEqualTo(5);
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "CHARACTER_MAXIMUM_LENGTH")).isEqualTo(16);
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrOptBINARY_1048576()
      throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "CHARACTER_MAXIMUM_LENGTH")).isEqualTo(1048576);
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_mdrReqINTERVAL_H_S3()
      throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "CHARACTER_MAXIMUM_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(9)).isEqualTo("CHARACTER_MAXIMUM_LENGTH");
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(9)).isEqualTo("INTEGER");
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(9)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_CHARACTER_MAXIMUM_LENGTH_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(9)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #10: CHARACTER_OCTET_LENGTH:
  // - SQL:
  // - Dremio:
  // - (Meta): INTEGER; Nullable;

  @Test
  public void test_CHARACTER_OCTET_LENGTH_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(10)).isEqualTo("CHARACTER_OCTET_LENGTH");
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "CHARACTER_OCTET_LENGTH"))
        .isEqualTo(10 /* chars. */ * 4 /* max. UTF-8 bytes per char. */);
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "CHARACTER_OCTET_LENGTH"))
        .isEqualTo(65536 /* chars. (default of 65536) */ * 4 /* max. UTF-8 bytes per char. */);
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "CHARACTER_OCTET_LENGTH"))
        .isEqualTo(5 /* chars. */ * 4 /* max. UTF-8 bytes per char. */);
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "CHARACTER_OCTET_LENGTH")).isEqualTo(16);
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrOptBINARY_1048576() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "CHARACTER_OCTET_LENGTH")).isEqualTo(1048576);
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_mdrReqINTERVAL_H_S3() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_CHARACTER_OCTET_LENGTH_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "CHARACTER_OCTET_LENGTH")).isNull();
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(10)).isEqualTo("CHARACTER_OCTET_LENGTH");
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(10)).isEqualTo("INTEGER");
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(10)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_CHARACTER_OCTET_LENGTH_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(10)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #11: NUMERIC_PRECISION:
  // - SQL:
  // - Dremio:
  // - (Meta): INTEGER; Nullable;

  @Test
  public void test_NUMERIC_PRECISION_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(11)).isEqualTo("NUMERIC_PRECISION");
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "NUMERIC_PRECISION")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "NUMERIC_PRECISION")).isEqualTo(8);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "NUMERIC_PRECISION")).isEqualTo(16);
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "NUMERIC_PRECISION")).isEqualTo(32);
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "NUMERIC_PRECISION")).isEqualTo(64);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "NUMERIC_PRECISION")).isEqualTo(24);
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "NUMERIC_PRECISION")).isEqualTo(24);
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "NUMERIC_PRECISION")).isEqualTo(53);
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "NUMERIC_PRECISION")).isEqualTo(5);
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrOptBINARY_1048576() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightValue_mdrReqINTERVAL_H_S3() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_PRECISION_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_PRECISION_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_PRECISION_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_PRECISION_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "NUMERIC_PRECISION")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(11)).isEqualTo("NUMERIC_PRECISION");
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(11)).isEqualTo("INTEGER");
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(11)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_NUMERIC_PRECISION_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(11)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #12: NUMERIC_PRECISION_RADIX:
  // - SQL:
  // - Dremio:
  // - (Meta): INTEGER; Nullable;

  @Test
  public void test_NUMERIC_PRECISION_RADIX_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(12)).isEqualTo("NUMERIC_PRECISION_RADIX");
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "NUMERIC_PRECISION_RADIX")).isEqualTo(2);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "NUMERIC_PRECISION_RADIX")).isEqualTo(2);
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "NUMERIC_PRECISION_RADIX")).isEqualTo(2);
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "NUMERIC_PRECISION_RADIX")).isEqualTo(2);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "NUMERIC_PRECISION_RADIX")).isEqualTo(2);
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "NUMERIC_PRECISION_RADIX")).isEqualTo(2);
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "NUMERIC_PRECISION_RADIX")).isEqualTo(2);
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "NUMERIC_PRECISION_RADIX")).isEqualTo(10);
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrOptBINARY_1048576()
      throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_mdrReqINTERVAL_H_S3() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_PRECISION_RADIX_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "NUMERIC_PRECISION_RADIX")).isNull();
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(12)).isEqualTo("NUMERIC_PRECISION_RADIX");
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(12)).isEqualTo("INTEGER");
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(12)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_NUMERIC_PRECISION_RADIX_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(12)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #13: NUMERIC_SCALE:
  // - SQL:
  // - Dremio:
  // - (Meta):  INTEGER; Nullable;

  @Test
  public void test_NUMERIC_SCALE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(13)).isEqualTo("NUMERIC_SCALE");
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "NUMERIC_SCALE")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "NUMERIC_SCALE")).isEqualTo(0);
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "NUMERIC_SCALE")).isEqualTo(0);
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "NUMERIC_SCALE")).isEqualTo(0);
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "NUMERIC_SCALE")).isEqualTo(0);
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "NUMERIC_SCALE")).isEqualTo(3);
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrOptBINARY_1048576() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightValue_mdrReqINTERVAL_H_S3() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "NUMERIC_SCALE")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_SCALE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "NUMERIC_SCALE")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_SCALE_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "NUMERIC_SCALE")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_SCALE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "NUMERIC_SCALE")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_NUMERIC_SCALE_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "NUMERIC_SCALE")).isNull();
  }

  @Test
  public void test_NUMERIC_SCALE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(13)).isEqualTo("NUMERIC_SCALE");
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(13)).isEqualTo("INTEGER");
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(13)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_NUMERIC_SCALE_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(13)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #14: DATETIME_PRECISION:
  // - SQL:
  // - Dremio:
  // - (Meta): INTEGER; Nullable;

  @Test
  public void test_DATETIME_PRECISION_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(14)).isEqualTo("DATETIME_PRECISION");
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "DATETIME_PRECISION")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "DATETIME_PRECISION")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "DATETIME_PRECISION")).isNull();
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrOptBINARY_1048576() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqDATE() throws SQLException {
    // Zero because DATE doesn't (seem to) have a datetime precision, but its
    // DATETIME_PRECISION value must not be null.
    assertThat(getIntOrNull(mdrReqDATE, "DATETIME_PRECISION")).isEqualTo(0);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqTIME() throws SQLException {
    // Zero is default datetime precision for TIME.
    assertThat(getIntOrNull(mdrReqTIME, "DATETIME_PRECISION")).isEqualTo(0);
  }

  @Ignore("TODO(DRILL-3225): unignore when datetime precision is implemented")
  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "DATETIME_PRECISION")).isEqualTo(7);
  }

  @Test
  @Ignore("TODO(DRILL-3225): unignore when datetime precision is implemented")
  public void test_DATETIME_PRECISION_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    // 6 is default datetime precision for TIMESTAMP.
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "DATETIME_PRECISION")).isEqualTo(6);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "DATETIME_PRECISION")).isEqualTo(0);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_3Y_Mo() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3Y_Mo, "DATETIME_PRECISION")).isEqualTo(0);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_Mo() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Mo, "DATETIME_PRECISION")).isEqualTo(0);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_D() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_D, "DATETIME_PRECISION")).isEqualTo(6);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_4D_H() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_4D_H, "DATETIME_PRECISION")).isEqualTo(6);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_3D_Mi() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_3D_Mi, "DATETIME_PRECISION")).isEqualTo(6);
  }

  @Ignore("TODO(DRILL-3244): unignore when fractional secs. prec. is right")
  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_2D_S5() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_2D_S5, "DATETIME_PRECISION")).isEqualTo(5);
  }

  @Test
  public void test_DATETIME_PRECISION_hasINTERIMValue_mdrReqINTERVAL_2D_S5() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_2D_S5, "DATETIME_PRECISION")).isEqualTo(2);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_H() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_H, "DATETIME_PRECISION")).isEqualTo(6);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_1H_Mi() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_1H_Mi, "DATETIME_PRECISION")).isEqualTo(6);
  }

  @Ignore("TODO(DRILL-3244): unignore when fractional secs. prec. is right")
  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_H_S3() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "DATETIME_PRECISION")).isEqualTo(1);
  }

  @Test
  public void test_DATETIME_PRECISION_hasINTERIMValue_mdrReqINTERVAL_H_S3() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "DATETIME_PRECISION")).isEqualTo(3);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_Mi() throws SQLException {
    // 6 seems to be Dremio's (Calcite's) choice (to use default value for the
    // fractional seconds precision for when SECOND _is_ present) since the SQL
    // spec. (ISO/IEC 9075-2:2011(E) 10.1 <interval qualifier>) doesn't seem to
    // specify the fractional seconds precision when SECOND is _not_ present.
    assertThat(getIntOrNull(mdrReqINTERVAL_Mi, "DATETIME_PRECISION")).isEqualTo(6);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_5Mi_S() throws SQLException {
    // 6 is default fractional seconds precision when SECOND field is present.
    assertThat(getIntOrNull(mdrReqINTERVAL_5Mi_S, "DATETIME_PRECISION")).isEqualTo(6);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_S() throws SQLException {
    // 6 is default for interval fraction seconds precision.
    assertThat(getIntOrNull(mdrReqINTERVAL_S, "DATETIME_PRECISION")).isEqualTo(6);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_3S() throws SQLException {
    // 6 is default fractional seconds precision when SECOND field is present.
    assertThat(getIntOrNull(mdrReqINTERVAL_3S, "DATETIME_PRECISION")).isEqualTo(6);
  }

  @Ignore("TODO(DRILL-3244): unignore when fractional secs. prec. is right")
  @Test
  public void test_DATETIME_PRECISION_hasRightValue_mdrReqINTERVAL_3S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S1, "DATETIME_PRECISION")).isEqualTo(1);
  }

  @Test
  public void test_DATETIME_PRECISION_hasINTERIMValue_mdrReqINTERVAL_3S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S1, "DATETIME_PRECISION")).isEqualTo(3);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATETIME_PRECISION_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "DATETIME_PRECISION")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATETIME_PRECISION_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "DATETIME_PRECISION")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATETIME_PRECISION_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "DATETIME_PRECISION")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_DATETIME_PRECISION_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "DATETIME_PRECISION")).isNull();
  }

  @Test
  public void test_DATETIME_PRECISION_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(14)).isEqualTo("DATETIME_PRECISION");
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(14)).isEqualTo("INTEGER");
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(14)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_DATETIME_PRECISION_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(14)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #15: INTERVAL_TYPE:
  // - SQL:
  // - Dremio:
  // - (Meta): VARCHAR; Nullable;

  @Test
  public void test_INTERVAL_TYPE_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(15)).isEqualTo("INTERVAL_TYPE");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(mdrOptBOOLEAN.getString("INTERVAL_TYPE")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(mdrReqTINYINT.getString("INTERVAL_TYPE")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(mdrOptSMALLINT.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(mdrReqINTEGER.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(mdrOptBIGINT.getString("INTERVAL_TYPE")).isNull();
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(mdrOptREAL.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(mdrOptFLOAT.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(mdrReqDOUBLE.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(mdrReqDECIMAL_5_3.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(mdrReqVARCHAR_10.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(mdrOptVARCHAR.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(mdrReqCHAR_5.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(mdrOptVARBINARY_16.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrOptBINARY_1048576() throws SQLException {
    assertThat(mdrOptBINARY_1048576.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(mdrReqDATE.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(mdrReqTIME.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(mdrOptTIME_7.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(mdrOptTIMESTAMP.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    assertThat(mdrReqINTERVAL_Y.getString("INTERVAL_TYPE")).isEqualTo("YEAR");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_3Y_Mo() throws SQLException {
    assertThat(mdrReqINTERVAL_3Y_Mo.getString("INTERVAL_TYPE")).isEqualTo("YEAR TO MONTH");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_2Mo() throws SQLException {
    assertThat(mdrReqINTERVAL_Mo.getString("INTERVAL_TYPE")).isEqualTo("MONTH");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_D() throws SQLException {
    assertThat(mdrReqINTERVAL_D.getString("INTERVAL_TYPE")).isEqualTo("DAY");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_4D_H() throws SQLException {
    assertThat(mdrReqINTERVAL_4D_H.getString("INTERVAL_TYPE")).isEqualTo("DAY TO HOUR");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_3D_Mi() throws SQLException {
    assertThat(mdrReqINTERVAL_3D_Mi.getString("INTERVAL_TYPE")).isEqualTo("DAY TO MINUTE");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_H_S3() throws SQLException {
    assertThat(mdrReqINTERVAL_3H_S1.getString("INTERVAL_TYPE")).isEqualTo("HOUR TO SECOND");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_2D_S5() throws SQLException {
    assertThat(mdrReqINTERVAL_2D_S5.getString("INTERVAL_TYPE")).isEqualTo("DAY TO SECOND");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_H() throws SQLException {
    assertThat(mdrReqINTERVAL_H.getString("INTERVAL_TYPE")).isEqualTo("HOUR");
    // fail( "???VERIFY" );
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_1H_Mi() throws SQLException {
    assertThat(mdrReqINTERVAL_1H_Mi.getString("INTERVAL_TYPE")).isEqualTo("HOUR TO MINUTE");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_3H_S1() throws SQLException {
    assertThat(mdrReqINTERVAL_3H_S1.getString("INTERVAL_TYPE")).isEqualTo("HOUR TO SECOND");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_Mi() throws SQLException {
    assertThat(mdrReqINTERVAL_Mi.getString("INTERVAL_TYPE")).isEqualTo("MINUTE");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_5Mi_S() throws SQLException {
    assertThat(mdrReqINTERVAL_5Mi_S.getString("INTERVAL_TYPE")).isEqualTo("MINUTE TO SECOND");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_S() throws SQLException {
    assertThat(mdrReqINTERVAL_S.getString("INTERVAL_TYPE")).isEqualTo("SECOND");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_3S() throws SQLException {
    assertThat(mdrReqINTERVAL_3S.getString("INTERVAL_TYPE")).isEqualTo("SECOND");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightValue_mdrReqINTERVAL_3S1() throws SQLException {
    assertThat(mdrReqINTERVAL_3S1.getString("INTERVAL_TYPE")).isEqualTo("SECOND");
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_INTERVAL_TYPE_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(mdrReqARRAY.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_INTERVAL_TYPE_hasRightValue_tbdMAP() throws SQLException {
    assertThat(mdrReqMAP.getString("INTERVAL_TYPE")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_INTERVAL_TYPE_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "INTERVAL_TYPE")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_INTERVAL_TYPE_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "INTERVAL_TYPE")).isNull();
  }

  @Test
  public void test_INTERVAL_TYPE_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(15)).isEqualTo("INTERVAL_TYPE");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(15)).isEqualTo("CHARACTER VARYING");
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(15)).isEqualTo(Types.VARCHAR);
  }

  @Test
  public void test_INTERVAL_TYPE_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(15)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // #16: INTERVAL_PRECISION:
  // - SQL:
  // - Dremio:
  // - (Meta): INTEGER; Nullable;

  @Test
  public void test_INTERVAL_PRECISION_isAtRightPosition() throws SQLException {
    assertThat(rowsMetadata.getColumnLabel(16)).isEqualTo("INTERVAL_PRECISION");
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrOptBOOLEAN() throws SQLException {
    assertThat(getIntOrNull(mdrOptBOOLEAN, "INTERVAL_PRECISION")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when TINYINT is implemented")
  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqTINYINT() throws SQLException {
    assertThat(getIntOrNull(mdrReqTINYINT, "INTERVAL_PRECISION")).isNull();
  }

  @Ignore("TODO(DRILL-2470): unignore when SMALLINT is implemented")
  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrOptSMALLINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptSMALLINT, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTEGER() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTEGER, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrOptBIGINT() throws SQLException {
    assertThat(getIntOrNull(mdrOptBIGINT, "INTERVAL_PRECISION")).isNull();
  }

  @Ignore("TODO(DRILL-2683): unignore when REAL is implemented")
  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrOptREAL() throws SQLException {
    assertThat(getIntOrNull(mdrOptREAL, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrOptFLOAT() throws SQLException {
    assertThat(getIntOrNull(mdrOptFLOAT, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqDOUBLE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDOUBLE, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqDECIMAL_5_3() throws SQLException {
    assertThat(getIntOrNull(mdrReqDECIMAL_5_3, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqVARCHAR_10() throws SQLException {
    assertThat(getIntOrNull(mdrReqVARCHAR_10, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrOptVARCHAR() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARCHAR, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqCHAR_5() throws SQLException {
    assertThat(getIntOrNull(mdrReqCHAR_5, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrOptVARBINARY_16() throws SQLException {
    assertThat(getIntOrNull(mdrOptVARBINARY_16, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrOptBINARY_1048576() throws SQLException {
    assertThat(getIntOrNull(mdrOptBINARY_1048576, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqDATE() throws SQLException {
    assertThat(getIntOrNull(mdrReqDATE, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqTIME() throws SQLException {
    assertThat(getIntOrNull(mdrReqTIME, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrOptTIME_7() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIME_7, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrOptTIMESTAMP() throws SQLException {
    assertThat(getIntOrNull(mdrOptTIMESTAMP, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_Y() throws SQLException {
    // 2 is default field precision.
    assertThat(getIntOrNull(mdrReqINTERVAL_Y, "INTERVAL_PRECISION")).isEqualTo(2);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_3Y_Mo() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3Y_Mo, "INTERVAL_PRECISION")).isEqualTo(3);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_2Mo() throws SQLException {
    // 2 is default field precision.
    assertThat(getIntOrNull(mdrReqINTERVAL_Mo, "INTERVAL_PRECISION")).isEqualTo(2);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_D() throws SQLException {
    // 2 is default field precision.
    assertThat(getIntOrNull(mdrReqINTERVAL_D, "INTERVAL_PRECISION")).isEqualTo(2);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_4D_H() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_4D_H, "INTERVAL_PRECISION")).isEqualTo(4);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_3D_Mi() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3D_Mi, "INTERVAL_PRECISION")).isEqualTo(3);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_2D_S5() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_2D_S5, "INTERVAL_PRECISION")).isEqualTo(2);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_H() throws SQLException {
    // 2 is default field precision.
    assertThat(getIntOrNull(mdrReqINTERVAL_H, "INTERVAL_PRECISION")).isEqualTo(2);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_1H_Mi() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_1H_Mi, "INTERVAL_PRECISION")).isEqualTo(1);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_3H_S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3H_S1, "INTERVAL_PRECISION")).isEqualTo(3);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_Mi() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_Mi, "INTERVAL_PRECISION")).isEqualTo(2);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_5Mi_S() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_5Mi_S, "INTERVAL_PRECISION")).isEqualTo(5);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_S() throws SQLException {
    // 2 is default field precision.
    assertThat(getIntOrNull(mdrReqINTERVAL_S, "INTERVAL_PRECISION")).isEqualTo(2);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_3S() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S, "INTERVAL_PRECISION")).isEqualTo(3);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightValue_mdrReqINTERVAL_3S1() throws SQLException {
    assertThat(getIntOrNull(mdrReqINTERVAL_3S1, "INTERVAL_PRECISION")).isEqualTo(3);
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_INTERVAL_PRECISION_hasRightValue_tdbARRAY() throws SQLException {
    assertThat(getIntOrNull(mdrReqARRAY, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_INTERVAL_PRECISION_hasRightValue_tbdMAP() throws SQLException {
    assertThat(getIntOrNull(mdrReqMAP, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_INTERVAL_PRECISION_hasRightValue_tbdSTRUCT() throws SQLException {
    assertThat(getIntOrNull(mdrUnkSTRUCT, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  @Ignore("TODO(DRILL-3253): unignore when we have all-types test storage plugin")
  public void test_INTERVAL_PRECISION_hasRightValue_tbdUnion() throws SQLException {
    assertThat(getIntOrNull(mdrUnkUnion, "INTERVAL_PRECISION")).isNull();
  }

  @Test
  public void test_INTERVAL_PRECISION_hasSameNameAndLabel() throws SQLException {
    assertThat(rowsMetadata.getColumnName(16)).isEqualTo("INTERVAL_PRECISION");
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightTypeString() throws SQLException {
    assertThat(rowsMetadata.getColumnTypeName(16)).isEqualTo("INTEGER");
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightTypeCode() throws SQLException {
    assertThat(rowsMetadata.getColumnType(16)).isEqualTo(Types.INTEGER);
  }

  @Test
  public void test_INTERVAL_PRECISION_hasRightNullability() throws SQLException {
    assertThat(rowsMetadata.isNullable(16)).isEqualTo(columnNullable);
  }

  ////////////////////////////////////////////////////////////
  // Not (yet) implemented by Dremio:
  //
  // #17: CHARACTER_SET_CATALOG:
  // #18: CHARACTER_SET_SCHEMA:
  // #19: CHARACTER_SET_NAME:
  // #20: COLLATION_CATALOG:
  // #21: COLLATION_SCHEMA:
  // #22: COLLATION_NAME:
  // #23: DOMAIN_CATALOG:
  // #24: DOMAIN_SCHEMA:
  // #25: DOMAIN_NAME:
  // #26: UDT_CATALOG:
  // #27: UDT_SCHEMA:
  // #28: UDT_NAME:
  // #29: SCOPE_CATALOG:
  // #30: SCOPE_SCHEMA:
  // #31: SCOPE_NAME:
  // #32: MAXIMUM_CARDINALITY:
  // #33: DTD_IDENTIFIER:
  // #34: IS_SELF_REFERENCING:
  // #35: IS_IDENTITY:
  // #36: IDENTITY_GENERATION:
  // #37: IDENTITY_START:
  // #38: IDENTITY_INCREMENT:
  // #39: IDENTITY_MAXIMUM:
  // #40: IDENTITY_MINIMUM:
  // #41: IDENTITY_CYCLE:
  // #42: IS_GENERATED:
  // #43: GENERATION_EXPRESSION:
  // #44: IS_SYSTEM_TIME_PERIOD_START:
  // #45: IS_SYSTEM_TIME_PERIOD_END:
  // #46: SYSTEM_TIME_PERIOD_TIMESTAMP_GENERATION:
  // #47: IS_UPDATABLE:
  // #48: DECLARED_DATA_TYPE:
  // #49: DECLARED_NUMERIC_PRECISION:
  // #50: DECLARED_NUMERIC_SCALE:

} // class DatabaseMetaGetColumnsDataTest
