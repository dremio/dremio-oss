/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.expr.fn.impl;

import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestConcatFunctions extends BaseTestQuery {

  @Test
  public void testConcat() throws Exception {
    helper("s1s2", "s1", "s2");
    helper("s1", "s1", null);
    helper("s1s2s3s4", "s1", "s2", "s3", "s4");
    helper("s1s2s3s4s5s6s7s8s9s10", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10");
    helper("s1s2s4s5s6s8s9s10", "s1", "s2", null, "s4", "s5", "s6", null, "s8", "s9", "s10");
    helper("s1s2s4s5s6s8s9s10s11", "s1", "s2", null, "s4", "s5", "s6", null, "s8", "s9", "s10", "s11");
    helper("s1s2s4s5s6s8s9s10s11s12s13s14s15s16s17s18s19",
        "s1", "s2", null, "s4", "s5", "s6", null, "s8", "s9", "s10",
        "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19");
    helper("s1s2s4s5s6s8s9s10s11s12s13s14s15s16s17s18s19s20s21",
        "s1", "s2", null, "s4", "s5", "s6", null, "s8", "s9", "s10",
        "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21");
    helper("", null, null, null, null, null, null, null, null, null, null);
    helper("", null, null, null, null, null, null, null, null, null, null, null, null, null);
  }

  private void helper(String expected, String... args) throws Exception {
    final StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("SELECT concat(");

    boolean first = true;
    for(String arg : args) {
      if (first) {
        first = false;
      } else {
        queryBuilder.append(", ");
      }
      if (arg == null) {
        queryBuilder.append("cast(null as varchar(200))");
      } else {
        queryBuilder.append("'" + arg + "'");
      }
    }

    queryBuilder.append(") c1 FROM (VALUES(1))");

    testBuilder()
        .sqlQuery(queryBuilder.toString())
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues(expected)
        .go();
  }
}
