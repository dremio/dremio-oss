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

import static org.junit.Assert.assertEquals;

import com.dremio.context.RequestContext;
import com.dremio.context.SupportContext;
import org.junit.Test;

public class SqlCleanserTest {

  @Test
  public void cleanseSql1() throws Exception {
    String sql =
        "SELECT CAST({fn TRUNCATE(EXTRACT(YEAR FROM \"worker_date_0610\".\"ref_dt\"),0)} AS INTEGER) AS \"yr_ref_dt_ok\"\n"
            + "FROM \"WFA\".\"worker_date_0610\" \"worker_date_0610\"\n"
            + "  INNER JOIN \"WFA\".\"VW_HCMCORE_CORE_INDICATORS_0610\" \"VW_HCMCORE_CORE_INDICATORS_0610\" ON ((\"worker_date_0610\".\"hcmcore_wrkr_key\" = \"VW_HCMCORE_CORE_INDICATORS_0610\".\"HCMCORE_WRKR_KEY\") AND (\"worker_date_0610\".\"ref_dt_num\" = \"VW_HCMCORE_CORE_INDICATORS_0610\".\"JOIN_DT_NUM\"))\n"
            + "  INNER JOIN \"WFA\".\"VW_HCMCORE_WORKER_0610\" \"VW_HCMCORE_WORKER_0610\" ON (\"worker_date_0610\".\"hcmcore_wrkr_key\" = \"VW_HCMCORE_WORKER_0610\".\"HCMCORE_WRKR_KEY\")\n"
            + "  INNER JOIN \"WFA\".\"VW_HCMCORE_WORKER_C_ACTUAL_0610\" \"VW_HCMCORE_WORKER_C_ACTUAL_0610\" ON (\"worker_date_0610\".\"hcmcore_wrkr_key\" = \"VW_HCMCORE_WORKER_C_ACTUAL_0610\".\"HCMCORE_WRKR_KEY\")\n"
            + "WHERE ((\"VW_HCMCORE_WORKER_0610\".\"ent_usr_id\" = 'D513219') AND (\"VW_HCMCORE_WORKER_C_ACTUAL_0610\".\"ent_usr_id\" = 'D513219') AND (\"VW_HCMCORE_CORE_INDICATORS_0610\".\"ent_usr_id\" = 'D513219') AND (\"VW_HCMCORE_WORKER_0610\".\"usr_id\" = 'D513219') AND (\"VW_HCMCORE_WORKER_C_ACTUAL_0610\".\"usr_id\" = 'D513219') AND (\"VW_HCMCORE_CORE_INDICATORS_0610\".\"usr_id\" = 'D513219'))\n"
            + "GROUP BY CAST({fn TRUNCATE(EXTRACT(YEAR FROM \"worker_date_0610\".\"ref_dt\"),0)} AS INTEGER)";

    String expectedSql =
        "SELECT CAST({fn TRUNCATE(EXTRACT(YEAR FROM \"5b29115\".\"field_c8478bbc\"), 0) }AS INTEGER) AS \"field_55ef3b99\"\n"
            + "FROM \"1cb72\".\"5b29115\" AS \"5b29115\"\n"
            + "INNER JOIN \"1cb72\".\"e1bbec\" AS \"e1bbec\" ON \"5b29115\".\"field_6aafc530\" = \"e1bbec\".\"field_6aafc530\" AND \"5b29115\".\"field_fe102883\" = \"e1bbec\".\"field_5800fc0c\"\n"
            + "INNER JOIN \"1cb72\".\"b11d7e7a\" AS \"b11d7e7a\" ON \"5b29115\".\"field_6aafc530\" = \"b11d7e7a\".\"field_6aafc530\"\n"
            + "INNER JOIN \"1cb72\".\"970600a5\" AS \"970600a5\" ON \"5b29115\".\"field_6aafc530\" = \"970600a5\".\"field_6aafc530\"\n"
            + "WHERE \"b11d7e7a\".\"field_980a797a\" = 'literal_6a6306a7' AND \"970600a5\".\"field_980a797a\" = 'literal_6a6306a7' AND \"e1bbec\".\"field_980a797a\" = 'literal_6a6306a7' AND \"b11d7e7a\".\"field_ce30d3a6\" = 'literal_6a6306a7' AND \"970600a5\".\"field_ce30d3a6\" = 'literal_6a6306a7' AND \"e1bbec\".\"field_ce30d3a6\" = 'literal_6a6306a7'\n"
            + "GROUP BY CAST({fn TRUNCATE(EXTRACT(YEAR FROM \"5b29115\".\"field_c8478bbc\"), 0) }AS INTEGER)";

    ObfuscationUtils.setFullObfuscation(true);

    String observedSql =
        RequestContext.current()
            .with(
                SupportContext.CTX_KEY,
                new SupportContext(
                    "dummy",
                    "dummy",
                    new String[] {SupportContext.SupportRole.BASIC_SUPPORT_ROLE.getValue()}))
            .call(() -> SqlCleanser.cleanseSql(sql));
    assertEquals("Obfuscating of sql failed.", expectedSql, observedSql);
  }

  @Test
  public void cleanseSql2() throws Exception {
    String sql =
        "/*query87*/select count(*) \n"
            + " from ((select distinct c_last_name, c_first_name, d_date\n"
            + "     from store_sales, date_dim, customer\n"
            + "     where store_sales.ss_sold_date_sk = date_dim.d_date_sk\n"
            + "       and store_sales.ss_customer_sk = customer.c_customer_sk\n"
            + "       and d_month_seq between 1188 and 1188+11)\n"
            + "     except\n"
            + "    (select distinct c_last_name, c_first_name, d_date\n"
            + "     from catalog_sales, date_dim, customer\n"
            + "     where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk\n"
            + "       and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk\n"
            + "       and d_month_seq between 1188 and 1188+11)\n"
            + "     except\n"
            + "    (select distinct c_last_name, c_first_name, d_date\n"
            + "     from web_sales, date_dim, customer\n"
            + "     where web_sales.ws_sold_date_sk = date_dim.d_date_sk\n"
            + "       and web_sales.ws_bill_customer_sk = customer.c_customer_sk\n"
            + "       and d_month_seq between 1188 and 1188+11)\n"
            + "     ) cool_cust ;";

    String expectedSql =
        "SELECT COUNT(\"field_0\")\n"
            + "FROM (SELECT DISTINCT \"field_b3afe9b8\", \"field_30f8d816\", \"field_b00e46a9\"\n"
            + "FROM \"a769b82e\",\n"
            + "\"6ae01c77\",\n"
            + "\"24217fde\"\n"
            + "WHERE \"a769b82e\".\"field_37167b9d\" = \"6ae01c77\".\"field_cd523fae\" AND \"a769b82e\".\"field_cca5afa\" = \"24217fde\".\"field_adf8607d\" AND \"field_83229c05\" BETWEEN ASYMMETRIC 1188 AND 1188 + 11\n"
            + "EXCEPT\n"
            + "SELECT DISTINCT \"field_b3afe9b8\", \"field_30f8d816\", \"field_b00e46a9\"\n"
            + "FROM \"b731d566\",\n"
            + "\"6ae01c77\",\n"
            + "\"24217fde\"\n"
            + "WHERE \"b731d566\".\"field_995fd78d\" = \"6ae01c77\".\"field_cd523fae\" AND \"b731d566\".\"field_546ff210\" = \"24217fde\".\"field_adf8607d\" AND \"field_83229c05\" BETWEEN ASYMMETRIC 1188 AND 1188 + 11\n"
            + "EXCEPT\n"
            + "SELECT DISTINCT \"field_b3afe9b8\", \"field_30f8d816\", \"field_b00e46a9\"\n"
            + "FROM \"d067a861\",\n"
            + "\"6ae01c77\",\n"
            + "\"24217fde\"\n"
            + "WHERE \"d067a861\".\"field_9e8424a1\" = \"6ae01c77\".\"field_cd523fae\" AND \"d067a861\".\"field_65bf9524\" = \"24217fde\".\"field_adf8607d\" AND \"field_83229c05\" BETWEEN ASYMMETRIC 1188 AND 1188 + 11) AS \"ea1f589\"";

    ObfuscationUtils.setFullObfuscation(true);

    String observedSql =
        RequestContext.current()
            .with(
                SupportContext.CTX_KEY,
                new SupportContext(
                    "dummy",
                    "dummy",
                    new String[] {SupportContext.SupportRole.BASIC_SUPPORT_ROLE.getValue()}))
            .call(() -> SqlCleanser.cleanseSql(sql));
    assertEquals("Obfuscating of sql failed.", expectedSql, observedSql);
  }

  @Test
  public void testDMLInsertObfuscation() throws Exception {
    String sql =
        "INSERT INTO \"WFA\".\"worker_data\".\"plumbers\"\n"
            + "  AT BRANCH main\n"
            + "  (\"age\", \"name\", \"drink\")\n"
            + "  VALUES\n"
            + "    (21, 'Ruth Asawa', 'Coffee'),\n"
            + "    (56, 'Luis Alberto Acuña', null)";

    String expectedSql =
        "INSERT INTO \"1cb72\".\"3a85d3cb\".\"9179b958\" "
            + "AT BRANCH literal_3305b9"
            + "(\"field_178ff\", \"field_337a8b\", \"field_5b69898\") "
            + "VALUES "
            + "ROW(21, 'literal_ccc22c10', 'literal_78a0cbec'),\n"
            + "ROW(56, 'literal_c87f56f5', NULL)";
    ObfuscationUtils.setFullObfuscation(true);

    String observedSql =
        RequestContext.current()
            .with(
                SupportContext.CTX_KEY,
                new SupportContext(
                    "dummy",
                    "dummy",
                    new String[] {SupportContext.SupportRole.BASIC_SUPPORT_ROLE.getValue()}))
            .call(() -> SqlCleanser.cleanseSql(sql));
    assertEquals("Obfuscating of sql failed.", expectedSql, observedSql);
  }

  @Test
  public void testDMLDeleteObfuscation() throws Exception {
    String sql =
        "DELETE FROM \"orders\" AS \"ors\"\n"
            + "WHERE       EXISTS (\n"
            + "              select 1\n"
            + "              from   \"returns\" AS \"ret\"\n"
            + "              where  \"ret\".\"order_id\" = \"ors\".\"order_id\")";

    String expectedSql =
        "DELETE FROM \"c3df62e5\" AS \"1aef0\"\n"
            + "WHERE EXISTS (SELECT 1\n"
            + "FROM 41796943 AS \"1b8a1\"\n"
            + "WHERE \"1b8a1\".\"field_4991ffac\" = \"1aef0\".\"field_4991ffac\")";
    ObfuscationUtils.setFullObfuscation(true);

    String observedSql =
        RequestContext.current()
            .with(
                SupportContext.CTX_KEY,
                new SupportContext(
                    "dummy",
                    "dummy",
                    new String[] {SupportContext.SupportRole.BASIC_SUPPORT_ROLE.getValue()}))
            .call(() -> SqlCleanser.cleanseSql(sql));
    assertEquals("Obfuscating of sql failed.", expectedSql, observedSql);
  }

  @Test
  public void testDMLOptimizeObfuscation() throws Exception {
    String sql =
        "OPTIMIZE TABLE                             \"demo\".\"example_table\"\n"
            + "REWRITE DATA USING BIN_PACK FOR PARTITIONS \"sales_year\" IN (\n"
            + "                                             2021,\n"
            + "                                             2022) AND\n"
            + "                                           \"sales_company\" IN (\n"
            + "                                             'Apple',\n"
            + "                                             'Microsoft',\n"
            + "                                             'Dell'\n"
            + "                                           ) (\n"
            + "                                             MIN_FILE_SIZE_MB = 100,\n"
            + "                                             MAX_FILE_SIZE_MB = 1000,\n"
            + "                                             TARGET_FILE_SIZE_MB = 512,\n"
            + "                                             MIN_INPUT_FILES = 6)";

    String expectedSql =
        "OPTIMIZE TABLE \"2efde3\".\"5b5578b9\" "
            + "REWRITE DATA USING BIN_PACK "
            + "FOR PARTITIONS "
            + "\"field_6670bf90\" IN (2021, 2022) "
            + "AND \"field_a095604a\" IN ('literal_3c8933a', 'literal_71d450ce', 'literal_2071e1') "
            + "(\"min_file_size_mb\" = 100, "
            + "\"max_file_size_mb\" = 1000, "
            + "\"target_file_size_mb\" = 512, "
            + "\"min_input_files\" = 6)";
    ObfuscationUtils.setFullObfuscation(true);

    String observedSql =
        RequestContext.current()
            .with(
                SupportContext.CTX_KEY,
                new SupportContext(
                    "dummy",
                    "dummy",
                    new String[] {SupportContext.SupportRole.BASIC_SUPPORT_ROLE.getValue()}))
            .call(() -> SqlCleanser.cleanseSql(sql));
    assertEquals("Obfuscating of sql failed.", expectedSql, observedSql);
  }

  @Test
  public void testDMLUpdateObfuscation() throws Exception {
    String sql =
        "UPDATE MyCatalog.\"employees\" AS emps\n"
            + "SET    \"commission_pct\" = (SELECT \"pct\" FROM \"updated_commission_percents\" AS \"ucp\" WHERE \"ucp\".\"ssn\"=\"emps\".\"ssn\"),"
            + "\"category\"='SENIOR'\n"
            + "where  \"emps\".\"job_id\" = 'IT_PROG'"
            + "AND \"emps\".\"ssn\"='Tim'";

    String expectedSql =
        "UPDATE \"5bb0d8ed\".\"9d39ef85\" AS \"2f90ab\" SET\n"
            + "\"field_5cafef8d\" = (SELECT \"field_1b0e1\"\n"
            + "FROM \"1e2ef41e\" AS \"1c3a2\"\n"
            + "WHERE \"1c3a2\".\"field_1be0e\" = \"2f90ab\".\"field_1be0e\"),\n"
            + "\"field_302bcfe\" = 'literal_91932230'\n"
            + "WHERE \"2f90ab\".\"field_bb2be0dd\" = 'literal_aa5e4dee' AND \"2f90ab\".\"field_1be0e\" = 'literal_14878'";
    ObfuscationUtils.setFullObfuscation(true);

    String observedSql =
        RequestContext.current()
            .with(
                SupportContext.CTX_KEY,
                new SupportContext(
                    "dummy",
                    "dummy",
                    new String[] {SupportContext.SupportRole.BASIC_SUPPORT_ROLE.getValue()}))
            .call(() -> SqlCleanser.cleanseSql(sql));
    assertEquals("Obfuscating of sql failed.", expectedSql, observedSql);
  }

  @Test
  public void testDMLMergeObfuscation() throws Exception {
    String sql =
        "MERGE INTO                   \"dogfood-catalog\".\"my-temp-folder\".myTable AS mt\n"
            + "USING                        \"dogfood-catalog\".\"my-temp-folder\".sourceTable AS st\n"
            + "ON                           (mt.id = st.id) \n"
            + "WHEN MATCHED THEN UPDATE SET description = st.description, role = 'some role'\n"
            + "WHEN NOT MATCHED THEN INSERT (id, description, role) VALUES (st.id, st.description, 'another role')";

    String expectedSql =
        "MERGE INTO \"d9d09ce6\".\"aeae09e6\".\"5b11d9a2\" AS \"da7\"\n"
            + "USING \"d9d09ce6\".\"aeae09e6\".\"fc13ac93\" AS \"e61\"\n"
            + "ON \"da7\".\"field_d1b\" = \"e61\".\"field_d1b\"\n"
            + "WHEN MATCHED THEN UPDATE SET\n"
            + "\"field_993583fc\" = \"e61\".\"field_993583fc\",\n"
            + "\"field_358076\" = 'literal_d9b97422'\n"
            + "WHEN NOT MATCHED THEN INSERT (\"field_d1b\", \"field_993583fc\", \"field_358076\") "
            + "(VALUES ROW(\"e61\".\"field_d1b\", \"e61\".\"field_993583fc\", 'literal_d63f5373'))";
    ObfuscationUtils.setFullObfuscation(true);

    String observedSql =
        RequestContext.current()
            .with(
                SupportContext.CTX_KEY,
                new SupportContext(
                    "dummy",
                    "dummy",
                    new String[] {SupportContext.SupportRole.BASIC_SUPPORT_ROLE.getValue()}))
            .call(() -> SqlCleanser.cleanseSql(sql));
    assertEquals("Obfuscating of sql failed.", expectedSql, observedSql);
  }
}
