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
package com.dremio.exec.fn.hive;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.ExecConstants;

public class TestCoalesceMixedHive extends PlanTestBase {
  private static final String PLACEHOLDER = "---LARGESTRING---";

  @BeforeClass
  public static void setup() throws Exception {
    testNoResult("ALTER SYSTEM SET \"%s\" = 'concat_ws;unbase64'",
      ExecConstants.DISABLED_GANDIVA_FUNCTIONS.getOptionName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testNoResult("ALTER SYSTEM RESET \"%s\"", ExecConstants.DISABLED_GANDIVA_FUNCTIONS.getOptionName());
  }

  @Test
  public void testHiveFunctionsWithGandivaOverridden() throws Exception {
    // here unbase64 has to go to hive
    final String sql = "select CAST(base64(unbase64(t.currency_code)) AS VARCHAR(2)) as c1 " +
      "from cp.\"coalesce/b/0_0_0.parquet\" t ";
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("c1")
      .baselineValues("he")
      .baselineValues("he")
      .baselineValues("he")
      .baselineValues("he")
      .baselineValues("he")
      .go();
  }

  @Test
  public void testCoalesceMixed() throws Exception {
    final String sql = "SELECT * FROM " +
      "( SELECT *, COUNT(*) OVER(PARTITION BY version, date_of_comparison, layer, table_name, source_system_name, period_year, period_name, record_existance, col_diff) AS cnt, ROW_NUMBER() OVER (PARTITION BY version, date_of_comparison, layer, table_name, source_system_name, period_year, period_name, record_existance, col_diff) AS r FROM " +
      "( SELECT " +
      " 'CQG - Comparer Query Generator - v2.1' as version, " +
      " current_timestamp as date_of_comparison, " +
      " 'GP Prod vs DBR QA' as layer, " +
      " 'GPW_POWERMAX_400' as source_system_name, " +
      " 'xx_gl_balances' as table_name, " +
      " 2021 as period_year, " +
      " 'OCT-21' as period_name, " +
      " s.gl_balance_id as GP_Prod_gl_balance_id, " +
      " t.gl_balance_id as DBR_QA_gl_balance_id, " +
      " COALESCE(s.actual_flag, t.actual_flag) as actual_flag, " +
      " COALESCE(cast(s.code_combination_id as varchar), cast(t.code_combination_id as varchar)) as code_combination_id, " +
      " COALESCE(cast(s.ledger_id as varchar), cast(t.ledger_id as varchar)) as ledger_id, " +
      " COALESCE(cast(s.period_num as varchar), cast(t.period_num as varchar)) as period_num, " +
      " COALESCE(cast(s.period_year as varchar), cast(t.period_year as varchar)) as period_year, " +
      " COALESCE(coalesce(cast(s.budget_version_id as varchar),'<NULL>'), coalesce(cast(t.budget_version_id as varchar),'<NULL>')) as budget_version_id, " +
      " COALESCE(coalesce(cast(s.encumbrance_type_id as varchar),'<NULL>'), coalesce(cast(t.encumbrance_type_id as varchar),'<NULL>')) as encumbrance_type_id, " +
      " COALESCE(coalesce(cast(s.template_id as varchar),'<NULL>'), coalesce(cast(t.template_id as varchar),'<NULL>')) as template_id, " +
      " COALESCE(coalesce(cast(s.translated_flag as varchar),'<NULL>'), coalesce(cast(t.translated_flag as varchar),'<NULL>')) as translated_flag, " +
      " COALESCE(s.currency_code, t.currency_code) as currency_code, " +
      " CASE " +
      "   WHEN s.ctid is NULL THEN 'GP Prod record missing'  " +
      "   WHEN t.ctid is NULL THEN 'DBR QA record missing'  " +
      "   ELSE 'Both side record exist' " +
      " END record_existance, " +
      "  CASE WHEN s.ctid IS NULL OR t.ctid IS NULL THEN '[ALL columns differ]' ELSE " +
      "  COALESCE(NULLIF( " +
      "  CONCAT_WS(', ', " +
      "   CASE COALESCE(TRIM(TRAILING '0.' FROM CAST(ROUND(s.accounted_begin_balance,6) AS VARCHAR)),'')                             WHEN COALESCE(TRIM(TRAILING '0.' FROM CAST(ROUND(t.accounted_begin_balance,6) AS VARCHAR)),'')                            THEN CAST(NULL AS VARCHAR) ELSE 'accounted_begin_balance' END, " +
      "   CASE COALESCE(CAST(s.account_segment AS CHAR),'')                                                                          WHEN COALESCE(CAST(t.account_segment AS CHAR),'')                                                                         THEN CAST(NULL AS VARCHAR) ELSE 'account_segment' END, " +
      "   CASE COALESCE(CAST(s.sap_flex_segment_id_00 AS CHAR),'')                                                                   WHEN COALESCE(CAST(t.sap_flex_segment_id_00 AS CHAR),'')                                                                  THEN CAST(NULL AS VARCHAR) ELSE 'sap_flex_segment_id_00' END " +
      "  ),''),'ALL OK') END as col_diff, " +
      "  CASE WHEN s.ctid IS NULL OR t.ctid IS NULL THEN '[]' ELSE " +
      "  CONCAT_WS(', ', " +
      "   CASE COALESCE(TRIM(TRAILING '0.' FROM CAST(ROUND(s.accounted_begin_balance,6) AS VARCHAR)),'')                             WHEN COALESCE(TRIM(TRAILING '0.' FROM CAST(ROUND(t.accounted_begin_balance,6) AS VARCHAR)),'')                            THEN CAST(NULL AS VARCHAR) ELSE 'accounted_begin_balance'                                              ||'( ['|| COALESCE(TRIM(TRAILING '0.' FROM CAST(ROUND(s.accounted_begin_balance,6) AS VARCHAR)),'<NULL>')                       ||'] vs ['|| COALESCE(TRIM(TRAILING '0.' FROM CAST(ROUND(t.accounted_begin_balance,6) AS VARCHAR)),'<NULL>')                       ||'] )' END, " +
      "   CASE COALESCE(CAST(s.last_updated_by AS CHAR),'')                                                                          WHEN COALESCE(CAST(t.last_updated_by AS CHAR),'')                                                                         THEN CAST(NULL AS VARCHAR) ELSE 'last_updated_by'                                                      ||'( ['|| COALESCE(CAST(s.last_updated_by AS CHAR),'<NULL>')                                                                    ||'] vs ['|| COALESCE(CAST(t.last_updated_by AS CHAR),'<NULL>')                                                                    ||'] )' END, " +
      "   CASE COALESCE(CAST(s.usd_mor_rate_type AS CHAR),'')                                                                        WHEN COALESCE(CAST(t.usd_mor_rate_type AS CHAR),'')                                                                       THEN CAST(NULL AS VARCHAR) ELSE 'usd_mor_rate_type'                                                    ||'( ['|| COALESCE(CAST(s.usd_mor_rate_type AS CHAR),'<NULL>')                                                                  ||'] vs ['|| COALESCE(CAST(t.usd_mor_rate_type AS CHAR),'<NULL>')                                                                  ||'] )' END " +
      "  ) END as col_diff_with_value " +
      " " +
      " FROM " +
      "  ( " +
      "   SELECT " +
      "    1 ctid, " +
      "    * " +
      "     FROM cp.\"coalesce/b/0_0_0.parquet\" t " +
      "   WHERE TRUE " +
      "   AND period_year = 2021 " +
      "   AND period_name = 'OCT-21' " +
      "  ) t " +
      "  " +
      "FULL JOIN " +
      " ( " +
      "   SELECT " +
      "    1 ctid, " +
      "    * " +
      "   FROM cp.\"coalesce/a/0_0_0.parquet\" s " +
      "   WHERE TRUE " +
      "   AND source_system_name = 'GPW_POWERMAX_400' " +
      "   AND period_year = 2021 " +
      "   AND period_name = 'OCT-21' " +
      "  ) s " +
      " " +
      "  ON TRUE " +
      "  AND s.actual_flag = t.actual_flag AND cast(s.code_combination_id as varchar) = cast(t.code_combination_id as varchar) AND cast(s.ledger_id as varchar) = cast(t.ledger_id as varchar) AND cast(s.period_num as varchar) = cast(t.period_num as varchar) AND cast(s.period_year as varchar) = cast(t.period_year as varchar) AND coalesce(cast(s.budget_version_id as varchar),'<NULL>') = coalesce(cast(t.budget_version_id as varchar),'<NULL>') AND coalesce(cast(s.encumbrance_type_id as varchar),'<NULL>') = coalesce(cast(t.encumbrance_type_id as varchar),'<NULL>') AND coalesce(cast(s.template_id as varchar),'<NULL>') = coalesce(cast(t.template_id as varchar),'<NULL>') AND coalesce(cast(s.translated_flag as varchar),'<NULL>') = coalesce(cast(t.translated_flag as varchar),'<NULL>') AND s.currency_code = t.currency_code " +
      "  ) columns " +
      " ) columns " +
      " WHERE r=1 " +
      "ORDER BY record_existance,cnt DESC " +
      "LIMIT 5000;";
    testBuilder().sqlQuery(sql).unOrdered().expectsEmptyResultSet().go();
  }

  @Test
  public void testCoalesceLargeFunctionArg() throws Exception {
    final String sql =
      " SELECT count(*) as cnt FROM  " +
        "   (  SELECT " +
        "     CASE WHEN s.ctid IS NULL OR t.ctid IS NULL THEN '[ALL columns differ]' ELSE " +
        "     COALESCE(NULLIF( " +
        "     CONCAT_WS(', ', ---LARGESTRING--- " + "     ),''),'ALL OK') END as col_diff " +
        "  " +
        "   FROM " +
        "     ( " +
        "       SELECT " +
        "         1 ctid, " +
        "         * " +
        "     FROM cp.\"coalesce/b/0_0_0.parquet\" t " +
        "       WHERE TRUE " +
        "       AND period_name = 'hello world' " +
        "     ) t " +
        "    " +
        " FULL JOIN " +
        "   ( " +
        "       SELECT " +
        "         1 ctid, " +
        "         * " +
        "   FROM cp.\"coalesce/a/0_0_0.parquet\" s " +
        "       WHERE TRUE " +
        "       AND source_system_name = 'hello world' " +
        "     ) s " +
        "  " +
        "     ON TRUE " +
        "     AND s.actual_flag = t.actual_flag " +
        "     ) columns where col_diff like '%budget_version_id%'; ";
    final String largeCaseAsArg = FileUtils.getResourceAsString("/case-coalesce-s1.txt");
    String finalSql = sql.replaceAll(PLACEHOLDER, largeCaseAsArg);
    testBuilder().sqlQuery(finalSql).unOrdered().baselineColumns("cnt").baselineValues(25L).go();
  }
}
