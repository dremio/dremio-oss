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
package com.dremio;

import java.math.BigDecimal;
import org.junit.Test;

/**
 * Tests functions that are present only in Gandiva. Assumes that code-gen includes Gandiva
 * generation.
 */
public class TestGandivaOnlyQueries extends BaseTestQuery {

  @Test
  public void testGandivaOnlyFunction() throws Exception {
    final String query = "select starts_with('testMe', 'test') from (values(1))";
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("EXPR$0").baselineValues(true).go();
  }

  @Test
  public void testDecimal_Parquet() throws Exception {

    final String query =
        "select expr$0 + cast ('3.12' as decimal(3,2)) from cp"
            + ".\"parquet/decimals"
            + ".parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(new BigDecimal("3.73031621334854424532"))
        .go();
  }

  @Test
  public void testDecimal_Parquet_TwoCols() throws Exception {

    final String query =
        "select expr$0 + expr$0 from cp" + ".\"parquet/decimals" + ".parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(new BigDecimal("1.22063242669708849064"))
        .go();
  }

  @Test
  public void testDecimal_Json() throws Exception {

    final String query =
        "select cast(decimal_value as decimal(3,2)) + cast(decimal_value as "
            + "decimal(3,2)) from cp.\"decimals.json\"";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(BigDecimal.valueOf(4.24))
        .go();
  }
}
