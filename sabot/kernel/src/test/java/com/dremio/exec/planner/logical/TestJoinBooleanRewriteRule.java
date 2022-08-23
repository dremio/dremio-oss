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
package com.dremio.exec.planner.logical;

import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestJoinBooleanRewriteRule extends PlanTestBase {

  @Test
  public void testEqual() throws Exception {
    String sql = "SELECT * FROM (SELECT total_children = 1 as x FROM cp.\"customer.json\") a," +
      "(SELECT total_children = 1 as x FROM cp.\"customer.json\") b WHERE a.x = b.x";

    testPlanMatchingPatterns(
      sql,
      new String[] { "HashJoin\\(condition\\=\\[\\=\\(\\$1, \\$3\\)\\], joinType\\=\\[inner\\]\\) \\: rowType \\= RecordType\\(BOOLEAN x, INTEGER \\$f1, BOOLEAN x0, INTEGER \\$f10\\)" },
      new String[] {});
  }

  @Test
  public void testIsNotDistinctFrom() throws Exception {
    String sql = "SELECT * FROM (SELECT total_children = 1 as x FROM cp.\"customer.json\") a," +
      "(SELECT total_children = 1 as x FROM cp.\"customer.json\") b WHERE a.x IS NOT DISTINCT FROM b.x";

    testPlanMatchingPatterns(
      sql,
      new String[] { "HashJoin\\(condition\\=\\[IS NOT DISTINCT FROM\\(\\$1, \\$3\\)\\], joinType\\=\\[inner\\]\\) " +
        "\\: rowType \\= RecordType\\(BOOLEAN x, INTEGER \\$f1, BOOLEAN x0, INTEGER \\$f10\\)" },
      new String[] {});
  }

  @Test
  public void testCompound() throws Exception {
    String sql = "SELECT * FROM (SELECT total_children = 1 as x, postal_code y FROM cp.\"customer.json\") a," +
      "(SELECT total_children = 1 as x, postal_code y FROM cp.\"customer.json\") b WHERE a.x IS NOT DISTINCT FROM b.x and a.y = b.y";

    testPlanMatchingPatterns(
      sql,
      new String[] { "HashJoin\\(condition\\=\\[AND\\(\\=\\(\\$1, \\$4\\), IS NOT DISTINCT FROM\\(\\$2, \\$5\\)\\)\\], " +
        "joinType\\=\\[inner\\]\\) \\: rowType \\= RecordType\\(BOOLEAN x, VARCHAR\\(65536\\) y, INTEGER \\$f2, BOOLEAN x0, VARCHAR\\(65536\\) y0, INTEGER \\$f20\\)" },
      new String[] {});
  }

}
