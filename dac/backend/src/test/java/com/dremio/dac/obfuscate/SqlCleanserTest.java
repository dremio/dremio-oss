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
package com.dremio.dac.obfuscate;

import static junit.framework.TestCase.assertEquals;

import org.junit.Test;

import com.dremio.context.RequestContext;
import com.dremio.context.SupportContext;

public class SqlCleanserTest {

  @Test
  public void cleanseSql1() throws Exception {
    String sql = "SELECT CAST({fn TRUNCATE(EXTRACT(YEAR FROM \"worker_date_0610\".\"ref_dt\"),0)} AS INTEGER) AS \"yr_ref_dt_ok\"\n" +
      "FROM \"WFA\".\"worker_date_0610\" \"worker_date_0610\"\n" +
      "  INNER JOIN \"WFA\".\"VW_HCMCORE_CORE_INDICATORS_0610\" \"VW_HCMCORE_CORE_INDICATORS_0610\" ON ((\"worker_date_0610\".\"hcmcore_wrkr_key\" = \"VW_HCMCORE_CORE_INDICATORS_0610\".\"HCMCORE_WRKR_KEY\") AND (\"worker_date_0610\".\"ref_dt_num\" = \"VW_HCMCORE_CORE_INDICATORS_0610\".\"JOIN_DT_NUM\"))\n" +
      "  INNER JOIN \"WFA\".\"VW_HCMCORE_WORKER_0610\" \"VW_HCMCORE_WORKER_0610\" ON (\"worker_date_0610\".\"hcmcore_wrkr_key\" = \"VW_HCMCORE_WORKER_0610\".\"HCMCORE_WRKR_KEY\")\n" +
      "  INNER JOIN \"WFA\".\"VW_HCMCORE_WORKER_C_ACTUAL_0610\" \"VW_HCMCORE_WORKER_C_ACTUAL_0610\" ON (\"worker_date_0610\".\"hcmcore_wrkr_key\" = \"VW_HCMCORE_WORKER_C_ACTUAL_0610\".\"HCMCORE_WRKR_KEY\")\n" +
      "WHERE ((\"VW_HCMCORE_WORKER_0610\".\"ent_usr_id\" = 'D513219') AND (\"VW_HCMCORE_WORKER_C_ACTUAL_0610\".\"ent_usr_id\" = 'D513219') AND (\"VW_HCMCORE_CORE_INDICATORS_0610\".\"ent_usr_id\" = 'D513219') AND (\"VW_HCMCORE_WORKER_0610\".\"usr_id\" = 'D513219') AND (\"VW_HCMCORE_WORKER_C_ACTUAL_0610\".\"usr_id\" = 'D513219') AND (\"VW_HCMCORE_CORE_INDICATORS_0610\".\"usr_id\" = 'D513219'))\n" +
      "GROUP BY CAST({fn TRUNCATE(EXTRACT(YEAR FROM \"worker_date_0610\".\"ref_dt\"),0)} AS INTEGER)";

    String expectedSql = "SELECT CAST({fn TRUNCATE(EXTRACT(YEAR FROM \"5b29115\".\"field_c8478bbc\"), 0) }AS INTEGER) AS \"field_55ef3b99\"\n" +
      "FROM \"1cb72\".\"5b29115\" AS \"5b29115\"\n" +
      "INNER JOIN \"1cb72\".\"e1bbec\" AS \"e1bbec\" ON \"5b29115\".\"field_6aafc530\" = \"e1bbec\".\"field_6aafc530\" AND \"5b29115\".\"field_fe102883\" = \"e1bbec\".\"field_5800fc0c\"\n" +
      "INNER JOIN \"1cb72\".\"b11d7e7a\" AS \"b11d7e7a\" ON \"5b29115\".\"field_6aafc530\" = \"b11d7e7a\".\"field_6aafc530\"\n" +
      "INNER JOIN \"1cb72\".\"970600a5\" AS \"970600a5\" ON \"5b29115\".\"field_6aafc530\" = \"970600a5\".\"field_6aafc530\"\n" +
      "WHERE \"b11d7e7a\".\"field_980a797a\" = 'literal_6a6306a7' AND \"970600a5\".\"field_980a797a\" = 'literal_6a6306a7' AND \"e1bbec\".\"field_980a797a\" = 'literal_6a6306a7' AND \"b11d7e7a\".\"field_ce30d3a6\" = 'literal_6a6306a7' AND \"970600a5\".\"field_ce30d3a6\" = 'literal_6a6306a7' AND \"e1bbec\".\"field_ce30d3a6\" = 'literal_6a6306a7'\n" +
      "GROUP BY CAST({fn TRUNCATE(EXTRACT(YEAR FROM \"5b29115\".\"field_c8478bbc\"), 0) }AS INTEGER)";

    ObfuscationUtils.setFullObfuscation(true);

    String observedSql =
      RequestContext.current()
        .with(SupportContext.CTX_KEY, new SupportContext("dummy", "dummy"))
        .call(() -> SqlCleanser.cleanseSql(sql));
    assertEquals("Obfuscating of sql failed.", expectedSql, observedSql);
  }

  @Test
  public void cleanseSql2() throws Exception {
    String sql = "/*query87*/select count(*) \n" +
      " from ((select distinct c_last_name, c_first_name, d_date\n" +
      "     from store_sales, date_dim, customer\n" +
      "     where store_sales.ss_sold_date_sk = date_dim.d_date_sk\n" +
      "       and store_sales.ss_customer_sk = customer.c_customer_sk\n" +
      "       and d_month_seq between 1188 and 1188+11)\n" +
      "     except\n" +
      "    (select distinct c_last_name, c_first_name, d_date\n" +
      "     from catalog_sales, date_dim, customer\n" +
      "     where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk\n" +
      "       and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk\n" +
      "       and d_month_seq between 1188 and 1188+11)\n" +
      "     except\n" +
      "    (select distinct c_last_name, c_first_name, d_date\n" +
      "     from web_sales, date_dim, customer\n" +
      "     where web_sales.ws_sold_date_sk = date_dim.d_date_sk\n" +
      "       and web_sales.ws_bill_customer_sk = customer.c_customer_sk\n" +
      "       and d_month_seq between 1188 and 1188+11)\n" +
      "     ) cool_cust ;";

    String expectedSql = "SELECT COUNT(\"field_0\")\n" +
      "FROM (SELECT DISTINCT \"field_b3afe9b8\", \"field_30f8d816\", \"field_b00e46a9\"\n" +
      "FROM \"a769b82e\",\n" +
      "\"6ae01c77\",\n" +
      "\"24217fde\"\n" +
      "WHERE \"a769b82e\".\"field_37167b9d\" = \"6ae01c77\".\"field_cd523fae\" AND \"a769b82e\".\"field_cca5afa\" = \"24217fde\".\"field_adf8607d\" AND \"field_83229c05\" BETWEEN ASYMMETRIC 1188 AND 1188 + 11\n" +
      "EXCEPT\n" +
      "SELECT DISTINCT \"field_b3afe9b8\", \"field_30f8d816\", \"field_b00e46a9\"\n" +
      "FROM \"b731d566\",\n" +
      "\"6ae01c77\",\n" +
      "\"24217fde\"\n" +
      "WHERE \"b731d566\".\"field_995fd78d\" = \"6ae01c77\".\"field_cd523fae\" AND \"b731d566\".\"field_546ff210\" = \"24217fde\".\"field_adf8607d\" AND \"field_83229c05\" BETWEEN ASYMMETRIC 1188 AND 1188 + 11\n" +
      "EXCEPT\n" +
      "SELECT DISTINCT \"field_b3afe9b8\", \"field_30f8d816\", \"field_b00e46a9\"\n" +
      "FROM \"d067a861\",\n" +
      "\"6ae01c77\",\n" +
      "\"24217fde\"\n" +
      "WHERE \"d067a861\".\"field_9e8424a1\" = \"6ae01c77\".\"field_cd523fae\" AND \"d067a861\".\"field_65bf9524\" = \"24217fde\".\"field_adf8607d\" AND \"field_83229c05\" BETWEEN ASYMMETRIC 1188 AND 1188 + 11) AS \"ea1f589\"";

    ObfuscationUtils.setFullObfuscation(true);

    String observedSql =
      RequestContext.current()
        .with(SupportContext.CTX_KEY, new SupportContext("dummy", "dummy"))
        .call(() -> SqlCleanser.cleanseSql(sql));
    assertEquals("Obfuscating of sql failed.", expectedSql, observedSql);
  }

}
