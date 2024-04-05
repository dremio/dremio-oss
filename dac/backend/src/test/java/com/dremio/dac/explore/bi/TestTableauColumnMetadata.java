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
package com.dremio.dac.explore.bi;

import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.TABLEAU_TYPE_NOMINAL;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.TABLEAU_TYPE_ORDINAL;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.TABLEAU_TYPE_QUANTITATIVE;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.TableauRole;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.getPrettyColumnName;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.getTableauDataType;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.getTableauRole;
import static com.dremio.dac.explore.bi.TableauMessageBodyGenerator.TableauColumnMetadata.getTableauType;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests TableauColumnMetadata class */
@RunWith(Enclosed.class)
public class TestTableauColumnMetadata {

  /** Tests getPrettyColumnName method */
  @RunWith(Parameterized.class)
  public static class PrettyColumnNameTest {

    private final String columnName;
    private final String expected;

    public PrettyColumnNameTest(String columnName, String expected) {
      this.columnName = columnName;
      this.expected = expected;
    }

    @Parameterized.Parameters(name = "Column Name:{0}, Pretty Column Name:{1}")
    public static List<Object[]> parameters() {
      return Arrays.asList(
          new Object[] {"c", "C"},
          new Object[] {"c2", "C2"},
          new Object[] {"col3", "Col3"},
          new Object[] {"CamelCase", "Camel Case"},
          new Object[] {"MultipleCamelCases", "Multiple Camel Cases"},
          new Object[] {"mixedCamelCase", "Mixed Camel Case"},
          new Object[] {"mixedCamelCASE", "Mixed Camel Case"},
          new Object[] {"mixedCamelCAsE", "Mixed Camel Cas E"},
          new Object[] {" leadingspace1", "Leadingspace1"},
          new Object[] {" leadingspace1", "Leadingspace1"},
          new Object[] {"trailingspace1 ", "Trailingspace1"},
          new Object[] {"  leadingspace2", "Leadingspace2"},
          new Object[] {"trailingspace2  ", "Trailingspace2"},
          new Object[] {"_leadingunderscore1", "_Leadingunderscore1"},
          new Object[] {"trailingunderscore1_", "Trailingunderscore1_"},
          new Object[] {"__leadingunderscore2", "__Leadingunderscore2"},
          new Object[] {"trailingunderscore2__", "Trailingunderscore2__"},
          new Object[] {"___leadingunderscore3", "___Leadingunderscore3"},
          new Object[] {"trailingunderscore3___", "Trailingunderscore3___"},
          new Object[] {"non_trailing_single_underscore", "Non Trailing Single Underscore"},
          new Object[] {
            "non__trailing___multiple____underscore", "Non__Trailing___Multiple____Underscore"
          },
          new Object[] {"_multiple_mixed__underscore___", "_Multiple Mixed__Underscore___"},
          new Object[] {"_multiple_mixed__underscore___", "_Multiple Mixed__Underscore___"},
          new Object[] {"EXPR${0}_other_characters", "Expr${0} Other Characters"});
    }

    @Test
    public void testGetPrettyColumnName() {
      assertEquals(expected, getPrettyColumnName(columnName));
    }
  }

  /** Tests getTableauDataType method */
  @RunWith(Parameterized.class)
  public static class TableauDataTypeTest {

    private final ArrowTypeID arrowTypeId;
    private final String expected;

    public TableauDataTypeTest(ArrowTypeID arrowTypeId, String expected) {
      this.arrowTypeId = arrowTypeId;
      this.expected = expected;
    }

    @Parameterized.Parameters(name = "ArrowTypeID:{0}, Tableau Data Type:{1}")
    public static List<Object[]> parameters() {
      return Arrays.asList(
          new Object[] {ArrowTypeID.Int, "integer"},
          new Object[] {ArrowTypeID.Decimal, "real"},
          new Object[] {ArrowTypeID.FloatingPoint, "real"},
          new Object[] {ArrowTypeID.Bool, "boolean"},
          new Object[] {ArrowTypeID.Date, "date"},
          new Object[] {ArrowTypeID.Time, "datetime"},
          new Object[] {ArrowTypeID.Timestamp, "datetime"},
          new Object[] {ArrowTypeID.Utf8, "string"},
          new Object[] {ArrowTypeID.LargeUtf8, "string"},
          new Object[] {ArrowTypeID.Binary, "string"},
          new Object[] {ArrowTypeID.LargeBinary, "string"},
          new Object[] {ArrowTypeID.Duration, "string"},
          new Object[] {ArrowTypeID.Interval, "string"});
    }

    @Test
    public void testGetTableauDataType() {
      assertEquals(expected, getTableauDataType(arrowTypeId));
    }
  }

  /** Tests getTableauRole method */
  @RunWith(Parameterized.class)
  public static class TableauRoleTest {

    private final ArrowTypeID arrowTypeId;
    private final String fieldName;
    private final String expected;

    public TableauRoleTest(ArrowTypeID arrowTypeId, String fieldName, String expected) {
      this.arrowTypeId = arrowTypeId;
      this.fieldName = fieldName;
      this.expected = expected;
    }

    @Parameterized.Parameters(name = "ArrowTypeID:{0}, Field Name:{1}, Tableau Data Type:{2}")
    public static List<Object[]> parameters() {
      return Arrays.asList(
          new Object[] {ArrowTypeID.Int, "col_int", "measure"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal", "measure"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float", "measure"},
          new Object[] {ArrowTypeID.Int, "code_col_int", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "Code_decimal", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "CODE_col_float", "dimension"},
          new Object[] {ArrowTypeID.Int, "id_col_int", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "Id_col_decimal", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "ID_col_float", "dimension"},
          new Object[] {ArrowTypeID.Int, "key_col_int", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "Key_decimal", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "KEY_col_float", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_code", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Code", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_CODE", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_id", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Id", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_ID", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_key", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Key", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_KEY", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_number", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Number", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_NUMBER", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_num", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Num", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_NUM", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_nbr", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Nbr", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_NBR", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_year", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Year", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_YEAR", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_yr", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Yr", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_YR", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_day", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Day", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_DAY", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_week", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Week", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_WEEK", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_wk", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Wk", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_WK", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_month", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Month", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_MONTH", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_quarter", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Quarter", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_QUARTER", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_qtr", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Qtr", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_QTR", "dimension"},
          new Object[] {ArrowTypeID.Int, "col_int_fy", "dimension"},
          new Object[] {ArrowTypeID.Decimal, "col_decimal_Fy", "dimension"},
          new Object[] {ArrowTypeID.FloatingPoint, "col_float_FY", "dimension"});
    }

    @Test
    public void testGetTableauRole() {
      assertEquals(expected, getTableauRole(arrowTypeId, fieldName).toString());
    }
  }

  /** Tests getTableauType method */
  @RunWith(Parameterized.class)
  public static class TableauTypeTest {

    private final ArrowTypeID arrowTypeId;
    private final TableauRole tableauRole;
    private final String expected;

    public TableauTypeTest(ArrowTypeID arrowTypeId, TableauRole tableauRole, String expected) {
      this.arrowTypeId = arrowTypeId;
      this.tableauRole = tableauRole;
      this.expected = expected;
    }

    @Parameterized.Parameters(name = "ArrowTypeID:{0}, Tableau Role:{1}, Tableau Type:{2}")
    public static List<Object[]> parameters() {
      return Arrays.asList(
          new Object[] {ArrowTypeID.Int, TableauRole.DIMENSION, TABLEAU_TYPE_ORDINAL},
          new Object[] {ArrowTypeID.Decimal, TableauRole.DIMENSION, TABLEAU_TYPE_ORDINAL},
          new Object[] {ArrowTypeID.FloatingPoint, TableauRole.DIMENSION, TABLEAU_TYPE_ORDINAL},
          new Object[] {ArrowTypeID.Int, TableauRole.MEASURE, TABLEAU_TYPE_QUANTITATIVE},
          new Object[] {ArrowTypeID.Decimal, TableauRole.MEASURE, TABLEAU_TYPE_QUANTITATIVE},
          new Object[] {ArrowTypeID.FloatingPoint, TableauRole.MEASURE, TABLEAU_TYPE_QUANTITATIVE},
          new Object[] {ArrowTypeID.Bool, TableauRole.DIMENSION, TABLEAU_TYPE_NOMINAL},
          new Object[] {ArrowTypeID.Date, TableauRole.DIMENSION, TABLEAU_TYPE_NOMINAL},
          new Object[] {ArrowTypeID.Time, TableauRole.DIMENSION, TABLEAU_TYPE_NOMINAL},
          new Object[] {ArrowTypeID.Timestamp, TableauRole.DIMENSION, TABLEAU_TYPE_NOMINAL},
          new Object[] {ArrowTypeID.Utf8, TableauRole.DIMENSION, TABLEAU_TYPE_NOMINAL},
          new Object[] {ArrowTypeID.LargeUtf8, TableauRole.DIMENSION, TABLEAU_TYPE_NOMINAL},
          new Object[] {ArrowTypeID.Binary, TableauRole.DIMENSION, TABLEAU_TYPE_NOMINAL},
          new Object[] {ArrowTypeID.LargeBinary, TableauRole.DIMENSION, TABLEAU_TYPE_NOMINAL},
          new Object[] {ArrowTypeID.Duration, TableauRole.DIMENSION, TABLEAU_TYPE_NOMINAL},
          new Object[] {ArrowTypeID.Interval, TableauRole.DIMENSION, TABLEAU_TYPE_NOMINAL});
    }

    @Test
    public void testGetTableauType() {
      assertEquals(expected, getTableauType(arrowTypeId, tableauRole));
    }
  }
}
