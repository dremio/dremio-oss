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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.options.OptionValue;
import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.RowLevelOperationMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Merge On Read DML Tests. For Copy On Write DML Tests, see {@link TestDml} <br>
 * Sql Validation tests ensure the input rowtype matches the expected rowtype. This input rowtype is
 * passed to the Physical planner.
 *
 * <p>The expected output rowtype will exclude outdated target columns. Target columns will be
 * excluded from output rowtype if one of the following conditions is true: <br>
 * - UPDATE commands where the column(s) are referenced in the Update Call <br>
 * - MERGE update-only commands where the columns are referenced in the Update Call <br>
 * - MERGE update-insert commands where the columns are both referenced in the update & Insert Call.
 * <br>
 * - MERGE insert-only commands where the columns are referenced in the Insert Call <br>
 */
public class TestDmlMergeOnRead extends TestTableManagementBase {

  private static final String TARGET_TABLE = "dfs_test.iceberg_MOR_DML_Test";
  private static final String SOURCE_TABLE = "dfs_test.source_table";

  @Before
  public void setup() throws Exception {
    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                "dremio.iceberg.merge_on_read_writer_with_positional_delete.enabled",
                true));

    runSQL(
        String.format(
            "CREATE TABLE %s "
                + "(order_id INT, "
                + "order_year INT, "
                + "order_date TIMESTAMP, "
                + "product_name VARCHAR, "
                + "amount DOUBLE)",
            TARGET_TABLE));

    runSQL(String.format("INSERT INTO %s SELECT * FROM %s", TARGET_TABLE, table.getTableName()));
    enableMergeOnRead(TARGET_TABLE);
    buildSourceTable(SOURCE_TABLE);
  }

  @After
  public void close() throws Exception {
    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                "dremio.iceberg.merge_on_read_writer_with_positional_delete.enabled",
                false));

    runSQL(String.format("DROP TABLE %s", TARGET_TABLE));
    runSQL(String.format("DROP TABLE %s", SOURCE_TABLE));
  }

  private void enableMergeOnRead(String testTable) throws Exception {
    runSQL(
        String.format(
            "ALTER TABLE %s SET TBLPROPERTIES "
                + "('write.delete.mode'='merge-on-read',"
                + "'write.update.mode'='merge-on-read', "
                + "'write.merge.mode'='merge-on-read')",
            testTable));
  }

  private void disableMergeOnRead() throws Exception {
    runSQL(
        String.format(
            "ALTER TABLE %s SET UNSET TBLPROPERTIES "
                + "('write.delete.mode'='merge-on-read',"
                + "'write.update.mode'='merge-on-read', "
                + "'write.merge.mode'='merge-on-read')",
            table.getTableName()));
  }

  private void buildSourceTable(String table) throws Exception {
    runSQL(
        String.format(
            "CREATE TABLE %s (c1 INT, c2 INT, c3 TIMESTAMP, c4 VARCHAR, c5 DOUBLE);", table));

    runSQL(
        String.format(
            "INSERT INTO %s (c1, c2, c3, c4, c5) "
                + "VALUES (123, 456, '2023-12-13 10:00:00', 'Another text', 123.45);",
            table));
  }

  @Test
  public void testSqlValidationDelete() throws Exception {
    String expectedInputRowType =
        "RecordType(VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X)";

    testMergeOnReadExtendedTableValidation(
        String.format("DELETE FROM %s", TARGET_TABLE), expectedInputRowType);
  }

  @Test
  public void testFeatureFlagErrorForDelete() throws Exception {
    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                "dremio.iceberg.merge_on_read_writer_with_positional_delete.enabled",
                false));

    String testColumn0 = userColumnList.get(0).getName();

    assertThatThrownBy(
            () -> runSQL(String.format("DELETE FROM %s WHERE %s > 0", TARGET_TABLE, testColumn0)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            String.format(
                "The target iceberg table's "
                    + "write.%s.mode table-property is set to 'merge-on-read', "
                    + "but dremio does not support this write property at this time. "
                    + "Please alter your write.%s.mode table property to 'copy-on-write' to proceed.",
                "delete", "delete"));
  }

  @Test
  public void testFeatureFlagErrorForUpdate() {
    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                "dremio.iceberg.merge_on_read_writer_with_positional_delete.enabled",
                false));

    String testColumn0 = userColumnList.get(0).getName();

    assertThatThrownBy(
            () -> runSQL(String.format("UPDATE %s SET %s = 0", TARGET_TABLE, testColumn0)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            String.format(
                "The target iceberg table's "
                    + "write.%s.mode table-property is set to 'merge-on-read', "
                    + "but dremio does not support this write property at this time. "
                    + "Please alter your write.%s.mode table property to 'copy-on-write' to proceed.",
                "update", "update"));
  }

  @Test
  public void testFeatureFlagErrorForMerge() {
    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM,
                "dremio.iceberg.merge_on_read_writer_with_positional_delete.enabled",
                false));

    String testColumn0 = userColumnList.get(0).getName();

    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "MERGE INTO %s USING %s AS s ON (%s = %s) "
                            + "WHEN MATCHED THEN UPDATE SET *",
                        TARGET_TABLE, SOURCE_TABLE, TARGET_TABLE + "." + testColumn0, "s.c1")))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            String.format(
                "The target iceberg table's "
                    + "write.%s.mode table-property is set to 'merge-on-read', "
                    + "but dremio does not support this write property at this time. "
                    + "Please alter your write.%s.mode table property to 'copy-on-write' to proceed.",
                "merge", "merge"));
  }

  @Test
  public void testSqlValidationExtendFeature() throws Exception {
    config
        .getContext()
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM, "debug.extend_on_select.enabled", true));

    runSQL(
        String.format(
            "SELECT %s.* FROM %s "
                + "EXTEND "
                + "(D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H VARCHAR, "
                + "D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X BIGINT)",
            table.getTableName(), table.getTableName()));
  }

  @Test
  public void testSqlValidationUpdateWhenSourceDifferentThanTarget() throws Exception {
    String expectedInputRowType =
        "RecordType("
            + "INTEGER order_year, "
            + "TIMESTAMP(3) order_date, "
            + "VARCHAR(65536) product_name, "
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER EXPR$0, "
            + "DOUBLE EXPR$1)";

    String testColumn0 = userColumnList.get(0).getName();
    String testColumn4 = userColumnList.get(4).getName();

    testMergeOnReadExtendedTableValidation(
        String.format(
            "UPDATE %s SET %s = %s, %s = %s FROM %s",
            TARGET_TABLE, testColumn0, "c1", testColumn4, "c5", SOURCE_TABLE),
        expectedInputRowType);
  }

  /**
   * Unique case for when target and source tables have matching columns in their schema. Calcite's
   * internal column-name adjustment does not apply to UPDATE DML, but still worthwhile to check.
   */
  @Test
  public void testSqlValidationUpdateWhenSourceDifferentThanTargetSameColumnNames()
      throws Exception {
    String expectedInputRowType =
        "RecordType("
            + "INTEGER order_year, "
            + "TIMESTAMP(3) order_date, "
            + "VARCHAR(65536) product_name, "
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER EXPR$0, "
            + "DOUBLE EXPR$1)";

    String testColumn0 = userColumnList.get(0).getName();
    String testColumn4 = userColumnList.get(4).getName();

    runSQL("CREATE TABLE dfs_test.foo9 (c1 INT, c2 INT, c3 TIMESTAMP, c4 VARCHAR, c5 DOUBLE);");
    runSQL(
        "INSERT INTO dfs_test.foo9 (c1, c2, c3, c4, c5) "
            + "VALUES (123, 456, '2023-12-13 10:00:00', 'Another text', 123.45);");

    testMergeOnReadExtendedTableValidation(
        String.format(
            "UPDATE %s SET %s = %s, %s = %s FROM %s",
            TARGET_TABLE, testColumn0, "order_year", testColumn4, "amount", "dfs_test.foo9"),
        expectedInputRowType);
  }

  @Test
  public void testSqlValidationUpdate() throws Exception {
    String expectedInputRowType =
        "RecordType("
            + "INTEGER order_year, "
            + "TIMESTAMP(3) order_date, "
            + "VARCHAR(65536) product_name, "
            + "DOUBLE amount, "
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER EXPR$0)"; // update cols

    String testColumn = userColumnList.get(0).getName();
    testMergeOnReadExtendedTableValidation(
        String.format("UPDATE %s SET %s = 0", TARGET_TABLE, testColumn), expectedInputRowType);
  }

  @Test
  public void testSqlValidationMergeUpdateOnlySourceDifferentThanTarget() throws Exception {
    String expectedRowInputType =
        "RecordType("
            + "INTEGER order_year, "
            + "VARCHAR(65536) product_name, "
            + "DOUBLE amount, "
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER c1, "
            + "TIMESTAMP(3) c3)";

    String testColumnInt = userColumnList.get(0).getName();
    String testColumnTimeStamp = userColumnList.get(2).getName();
    testMergeOnReadExtendedTableValidation(
        String.format(
            "MERGE INTO %s USING (SELECT * FROM %s) AS s ON (%s = %s) "
                + "WHEN MATCHED THEN UPDATE SET %s = %s, %s = %s",
            TARGET_TABLE,
            SOURCE_TABLE,
            TARGET_TABLE + '.' + testColumnInt,
            "s.c1",
            testColumnInt,
            "c1",
            testColumnTimeStamp,
            "c3"),
        expectedRowInputType);
  }

  @Test
  public void testSqlValidationMergeInsertOnlySourceDifferentThanTarget() throws Exception {
    String expectedRowInputType =
        "RecordType("
            + "INTEGER order_id, "
            + "INTEGER order_year, "
            + "VARCHAR(65536) product_name, "
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER $f5, "
            + "INTEGER $f6, "
            + "TIMESTAMP(3) c3, "
            + "VARCHAR(65536) $f8, "
            + "DOUBLE c5)";

    testMergeOnReadExtendedTableValidation(
        String.format(
            "MERGE INTO %s as o \n"
                + "USING %s AS f ON (o.amount = f.c5) \n"
                + "WHEN NOT MATCHED THEN INSERT (amount, order_date) VALUES (f.c5, f.c3)",
            TARGET_TABLE, SOURCE_TABLE),
        expectedRowInputType);
  }

  @Test
  public void testSqlValidationMergeInsertOnlyStar() throws Exception {
    String expectedRowInputType =
        "RecordType("
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER c1, "
            + "INTEGER c2, "
            + "TIMESTAMP(3) c3, "
            + "VARCHAR(65536) c4, "
            + "DOUBLE c5)";

    testMergeOnReadExtendedTableValidation(
        String.format(
            "MERGE INTO %s as o \n"
                + "USING %s AS f ON (o.amount = f.c5) \n"
                + "WHEN NOT MATCHED THEN INSERT *",
            TARGET_TABLE, SOURCE_TABLE),
        expectedRowInputType);
  }

  @Test
  public void testSqlValidationMergeTypeInsertUpdateSourceDifferentThanTargetNoOutdatedColumns()
      throws Exception {

    String expectedRowInputType =
        "RecordType("
            + "INTEGER order_id, "
            + "INTEGER order_year, "
            + "TIMESTAMP(3) order_date, "
            + "VARCHAR(65536) product_name, "
            + "DOUBLE amount, "
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER c1, "
            + "INTEGER $f8, "
            + "TIMESTAMP(3) $f9, "
            + "VARCHAR(65536) $f10, "
            + "DOUBLE $f11, "
            + "INTEGER $f12)";

    testMergeOnReadExtendedTableValidation(
        String.format(
            "MERGE INTO %s as o \n"
                + "USING %s AS f ON (o.amount = f.c5) \n"
                + "WHEN MATCHED THEN UPDATE SET amount = 0 \n"
                + "WHEN NOT MATCHED THEN INSERT (order_id) VALUES (f.c1);",
            TARGET_TABLE, SOURCE_TABLE),
        expectedRowInputType);
  }

  @Test
  public void testSqlValidationMergeTypeInsertUpdateSourceSameAsTargetSetNulls() throws Exception {

    String expectedRowInputType =
        "RecordType("
            + "INTEGER order_year0, "
            + "TIMESTAMP(3) order_date0, "
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER order_id, "
            + "INTEGER order_year, "
            + "TIMESTAMP(3) order_date, "
            + "VARCHAR(65536) product_name, "
            + "DOUBLE amount, "
            + "INTEGER $f9, "
            + "NULL $f10, "
            + "NULL $f11)";

    String testColumn0 = userColumnList.get(0).getName();
    String testColumn4 = userColumnList.get(3).getName();
    String testColumn5 = userColumnList.get(4).getName();
    testMergeOnReadExtendedTableValidation(
        String.format(
            "MERGE INTO %s USING (SELECT * FROM %s) AS s ON (%s = %s) "
                + "WHEN MATCHED THEN UPDATE SET %s = 0, %s = null, %s = null "
                + "WHEN NOT MATCHED THEN INSERT *;",
            TARGET_TABLE,
            TARGET_TABLE,
            TARGET_TABLE + '.' + testColumn4,
            "s." + testColumn4,
            testColumn0,
            testColumn4,
            testColumn5),
        expectedRowInputType);
  }

  /**
   * Unique case for when target and source tables have matching columns in their schema. Calcite
   * will internally modify the column names due to overlap between source & target. This test
   * verifies that Dremio resyncs it's reference to all outdated target columns by using a map with
   * Calcite's internal adjustments.
   */
  @Test
  public void testSqlValidationMergeTypeInsertUpdateSourceSameAsTargetStar() throws Exception {
    String expectedRowInputType =
        "RecordType("
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER c1, "
            + "INTEGER c2, "
            + "TIMESTAMP(3) c3, "
            + "VARCHAR(65536) c4, "
            + "DOUBLE c5, "
            + "INTEGER c10, "
            + "INTEGER c20, "
            + "TIMESTAMP(3) c30, "
            + "VARCHAR(65536) c40, "
            + "DOUBLE c50)";

    runSQL("CREATE TABLE dfs_test.foo (c1 INT, c2 INT, c3 TIMESTAMP, c4 VARCHAR, c5 DOUBLE);");
    runSQL(
        "INSERT INTO dfs_test.foo (c1, c2, c3, c4, c5) "
            + "VALUES (123, 456, '2023-12-13 10:00:00', 'Another text', 123.45);");

    enableMergeOnRead("dfs_test.foo");

    runSQL("CREATE TABLE dfs_test.foo2 (c1 INT, c2 INT, c3 TIMESTAMP, c4 VARCHAR, c5 DOUBLE);");
    runSQL(
        "INSERT INTO dfs_test.foo2 (c1, c2, c3, c4, c5) "
            + "VALUES (123, 456, '2023-12-13 10:00:00', 'Another text', 123.45);");

    testMergeOnReadExtendedTableValidation(
        "MERGE INTO dfs_test.foo USING dfs_test.foo2 AS s ON (dfs_test.foo.c1 = s.c1) "
            + "WHEN MATCHED THEN UPDATE SET *"
            + "WHEN NOT MATCHED THEN INSERT *;",
        expectedRowInputType);
  }

  /**
   * Unique case for when target and source tables have matching columns in their schema. Calcite
   * will internally modify the column names due to overlap between source & target. This test
   * verifies that Dremio resyncs it's reference to all outdated target columns by using a map with
   * Calcite's internal adjustments.
   */
  @Test
  public void testMergeTypeInsertUpdateStarSourceDiffThanTargetSubJoinsWithSameColumnName()
      throws Exception {
    String expectedRowInputType =
        "RecordType("
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER order_id, "
            + "INTEGER order_year, "
            + "TIMESTAMP(3) order_date, "
            + "VARCHAR(65536) product_name, "
            + "DOUBLE amount, "
            + "INTEGER order_id0, "
            + "INTEGER order_year0, "
            + "TIMESTAMP(3) order_date0, "
            + "VARCHAR(65536) product_name0, "
            + "DOUBLE amount0)";

    runSQL(
        "CREATE TABLE dfs_test.foo6 "
            + "(order_id INT, "
            + "order_year INT, "
            + "order_date TIMESTAMP, "
            + "product_name VARCHAR, "
            + "amount DOUBLE);");
    runSQL(
        "INSERT INTO dfs_test.foo6 (order_id, order_year, order_date, product_name, amount) "
            + "VALUES (456, 123, '2023-12-13 10:00:00', 'Another text', 123.45);");

    String testColumn = userColumnList.get(1).getName();
    testMergeOnReadExtendedTableValidation(
        String.format(
            "MERGE INTO %s USING %s AS s ON (%s = s.order_year) "
                + "WHEN MATCHED THEN UPDATE SET *"
                + "WHEN NOT MATCHED THEN INSERT *;",
            TARGET_TABLE, "dfs_test.foo6", TARGET_TABLE + '.' + testColumn),
        expectedRowInputType);
  }

  /**
   * Unique case for when target and source tables have matching columns in their schema. Calcite
   * will internally modify the column names due to overlap between source & target. This test
   * verifies that Dremio resyncs it's reference to all outdated target columns by using a map with
   * Calcite's internal adjustments.
   */
  @Test
  public void testMergeTypeUpdateOnlyStarSourceDiffThanTargetSubJoinsWithSameColumnName()
      throws Exception {
    String expectedRowInputType =
        "RecordType("
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER order_id, "
            + "INTEGER order_year, "
            + "TIMESTAMP(3) order_date, "
            + "VARCHAR(65536) product_name, "
            + "DOUBLE amount)";

    runSQL(
        "CREATE TABLE dfs_test.foo7 "
            + "(order_id INT, "
            + "order_year INT, "
            + "order_date TIMESTAMP, "
            + "product_name VARCHAR, "
            + "amount DOUBLE);");
    runSQL(
        "INSERT INTO dfs_test.foo7 (order_id, order_year, order_date, product_name, amount) "
            + "VALUES (456, 123, '2023-12-13 10:00:00', 'Another text', 123.45);");

    String testColumn = userColumnList.get(1).getName();
    testMergeOnReadExtendedTableValidation(
        String.format(
            "MERGE INTO %s USING %s AS s ON (%s = s.order_year) "
                + "WHEN MATCHED THEN UPDATE SET *;",
            TARGET_TABLE, "dfs_test.foo7", TARGET_TABLE + '.' + testColumn),
        expectedRowInputType);
  }

  @Test
  public void testSqlValidationUpdateStarWhenSourceDifferentThanTarget() throws Exception {

    String expectedInputRowType =
        "RecordType(VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER EXPR$0, "
            + "INTEGER EXPR$1, "
            + "TIMESTAMP(3) EXPR$2, "
            + "VARCHAR(65536) EXPR$3, "
            + "DOUBLE EXPR$4)";

    String testColumn0 = userColumnList.get(0).getName();
    String testColumn1 = userColumnList.get(1).getName();
    String testColumn2 = userColumnList.get(2).getName();
    String testColumn3 = userColumnList.get(3).getName();
    String testColumn4 = userColumnList.get(4).getName();

    testMergeOnReadExtendedTableValidation(
        String.format(
            "UPDATE %s SET "
                + "%s = %s, "
                + "%s = %s, "
                + "%s = %s, "
                + "%s = %s, "
                + "%s = %s "
                + "FROM %s",
            TARGET_TABLE,
            testColumn0,
            "c1",
            testColumn1,
            "c2",
            testColumn2,
            "c3",
            testColumn3,
            "c4",
            testColumn4,
            "c5",
            SOURCE_TABLE),
        expectedInputRowType);
  }

  @Test
  public void testSqlValidationMergeTypeInsertUpdateSourceSameAsTargetWithNulls() throws Exception {
    String expectedRowInputType =
        "RecordType("
            + "INTEGER order_id, "
            + "TIMESTAMP(3) order_date, "
            + "VARCHAR(65536) product_name, "
            + "DOUBLE amount, "
            + "VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, "
            + "BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_I_N_D_E_X, "
            + "INTEGER c1, "
            + "INTEGER c2, "
            + "TIMESTAMP(3) $f8, "
            + "VARCHAR(65536) $f9, "
            + "DOUBLE $f10, "
            + "INTEGER $f11, "
            + "INTEGER $f12)";

    testMergeOnReadExtendedTableValidation(
        String.format(
            "MERGE INTO %s as o \n"
                + "USING %s AS f ON (o.amount = f.c5) \n"
                + "WHEN MATCHED THEN UPDATE SET amount = 10, order_year = 2024 \n"
                + "WHEN NOT MATCHED THEN INSERT (order_year, order_id) VALUES (f.c2, f.c1);",
            TARGET_TABLE, SOURCE_TABLE),
        expectedRowInputType);
  }

  private void testMergeOnReadExtendedTableValidation(
      String query, String expectedInputRowTypeString) throws Exception {

    final SqlNode node = converter.parse(query);
    SqlDmlOperator sqlDmlOperator = (SqlDmlOperator) node;
    sqlDmlOperator.extendTableWithDataFileSystemColumns();
    sqlDmlOperator.setDmlWriteMode(RowLevelOperationMode.MERGE_ON_READ);
    final ConvertedRelNode convertedRelDeleteNode;
    convertedRelDeleteNode = SqlToRelTransformer.validateAndConvertForDml(config, node, null);
    List<RelDataTypeField> fields = convertedRelDeleteNode.getValidatedRowType().getFieldList();
    int totalColumnCount = convertedRelDeleteNode.getValidatedRowType().getFieldCount();
    assertThat(totalColumnCount).isEqualTo(userColumnCount + 2);
    // Verify that the last two columns are extended columns
    verifyNameAndType(
        fields.get(totalColumnCount - 2), ColumnUtils.FILE_PATH_COLUMN_NAME, SqlTypeName.VARCHAR);
    verifyNameAndType(
        fields.get(totalColumnCount - 1), ColumnUtils.ROW_INDEX_COLUMN_NAME, SqlTypeName.BIGINT);

    String actualInputRowTypeString =
        convertedRelDeleteNode.getConvertedNode().getInput(0).getRowType().toString();

    assertThat(actualInputRowTypeString).isEqualTo(expectedInputRowTypeString);
  }

  private void verifyNameAndType(RelDataTypeField field, String name, SqlTypeName type) {
    assertThat(field.getName()).isEqualTo(name);
    assertThat(field.getType().getSqlTypeName()).isEqualTo(type);
  }
}
