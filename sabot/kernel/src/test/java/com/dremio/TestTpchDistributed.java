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

import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.config.DremioConfig;
import com.dremio.test.TemporarySystemProperties;

public class TestTpchDistributed extends BaseTestQuery {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTpchDistributed.class);

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(15, TimeUnit.SECONDS);

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  private static void testDistributed(final String fileName) throws Exception {
    final String query = getFile(fileName);
    testNoResult("alter session set \"planner.slice_target\" = 10");
    test(query);
  }

  @Test
  public void tpch01() throws Exception{
    testDistributed("queries/tpch/01.sql");
  }

  @Test
  @Ignore("cartesian")
  public void tpch02() throws Exception{
    testDistributed("queries/tpch/02.sql");
  }

  @Test
  public void tpch03() throws Exception{
    testDistributed("queries/tpch/03.sql");
  }

  @Test
  public void tpch04() throws Exception{
    testDistributed("queries/tpch/04.sql");
  }

  @Test
  public void tpch05() throws Exception{
    testDistributed("queries/tpch/05.sql");
  }

  @Test
  public void tpch06() throws Exception{
    testDistributed("queries/tpch/06.sql");
  }

  @Test
  public void tpch07() throws Exception{
    testDistributed("queries/tpch/07.sql");
  }

  @Test
  public void tpch08() throws Exception{
    testDistributed("queries/tpch/08.sql");
  }

  @Test
  public void tpch09() throws Exception{
    testDistributed("queries/tpch/09.sql");
  }

  @Test
  public void tpch10() throws Exception{
    testDistributed("queries/tpch/10.sql");
  }

  @Test
  public void tpch11() throws Exception{
    testDistributed("queries/tpch/11.sql");
  }

  @Test
  public void tpch12() throws Exception{
    testDistributed("queries/tpch/12.sql");
  }

  @Test
  public void tpch13() throws Exception{
    testDistributed("queries/tpch/13.sql");
  }

  @Test
  public void tpch14() throws Exception{
    testDistributed("queries/tpch/14.sql");
  }

  @Test
  public void tpch15() throws Exception{
    try {
      properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
      testDistributed("queries/tpch/15.sql");
    } finally {
      properties.clear(DremioConfig.LEGACY_STORE_VIEWS_ENABLED);
    }
  }

  @Test
  public void tpch16() throws Exception{
    testDistributed("queries/tpch/16.sql");
  }

  @Test
  public void tpch17() throws Exception{
    testDistributed("queries/tpch/17.sql");
  }

  @Test
  public void tpch18() throws Exception{
    testDistributed("queries/tpch/18.sql");
  }

  @Test
  @Ignore("cartesian")
  public void tpch19() throws Exception{
    testDistributed("queries/tpch/19.sql");
  }

  @Test
  public void tpch19_1() throws Exception{
    testDistributed("queries/tpch/19_1.sql");
  }

  @Test
  public void tpch20() throws Exception{
    testDistributed("queries/tpch/20.sql");
  }

  @Test
  public void tpch21() throws Exception{
    testDistributed("queries/tpch/21.sql");
  }

  @Test
  public void tpch22() throws Exception{
    testDistributed("queries/tpch/22.sql");
  }

}
