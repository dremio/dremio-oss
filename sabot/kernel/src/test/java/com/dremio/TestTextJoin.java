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
package com.dremio;

import org.junit.Test;

import com.dremio.common.util.TestTools;

public class TestTextJoin extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTextJoin.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @Test
  public void testTextJoin1() throws Exception {
    String query1 = String.format("select r.columns[0] as v, r.columns[1] as w, r.columns[2] as x, u.columns[0] as y, t.columns[0] as z from dfs.\"%s/uservisits/rankings.tbl\" r, "
        + " dfs.\"%s/uservisits/uservisits.tbl\" u, dfs.\"%s/uservisits/temp1.tbl\" t "
        + " where r.columns[1]=u.columns[1] and r.columns[1] = t.columns[1]", TEST_RES_PATH, TEST_RES_PATH, TEST_RES_PATH);
    test(query1);
  }

  @Test
  public void testTextJoin2() throws Exception {
    String query1 = String.format("select r.columns[0] as v, r.columns[1] as w, r.columns[2] as x, u.columns[0] as y "
        + " from dfs.\"%s/uservisits/rankings.tbl\" r, dfs.\"%s/uservisits/uservisits.tbl\" u "
        + " where r.columns[1]=u.columns[1] and r.columns[0] < 50", TEST_RES_PATH, TEST_RES_PATH);
    test(query1);
  }

}
