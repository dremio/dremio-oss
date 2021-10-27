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
  public void cleanseSql() throws Exception {
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
}
