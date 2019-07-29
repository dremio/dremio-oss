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
package com.dremio.exec.planner.sql.handlers.commands;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.utils.SqlUtils;

/**
 * To test that use <schema> command is not misbehaving
 */
public class TestUseSchemaHandler extends BaseTestQuery {

  private static final String USE_TEST_PATH = SqlUtils.quotedCompound(Arrays.asList("cp", "use-test"));

  @Before
  public void resetClient() throws Exception {
    updateClient((Properties) null);
  }

  @Test
  public void checkUse() throws Exception {
    // First set the default schema
    // Then run a query to confirm the content
    testBuilder()
      .sqlQuery(format("USE %s", USE_TEST_PATH))
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, format("Default schema changed to [%s]", USE_TEST_PATH))
      .go();

    testBuilder()
      .sqlQuery(format("SELECT * FROM %s", SqlUtils.quoteIdentifier("foo.json")))
      .unOrdered()
      .baselineColumns("result")
      .baselineValues("ok")
      .go();
  }

  @Test
  public void checkRelativeUse() throws Exception {
    // First set the default schema
    // Set the schema a second time, and verify the returned schema is the correct one
    // Run a query to confirm the table content
    testBuilder()
      .sqlQuery(format("USE %s", USE_TEST_PATH))
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, format("Default schema changed to [%s]", USE_TEST_PATH))
      .go();

    testBuilder()
      .sqlQuery("USE dir")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, format("Default schema changed to [%s.dir]", USE_TEST_PATH))
      .go();

    testBuilder()
      .sqlQuery(format("SELECT * FROM %s", SqlUtils.quoteIdentifier("bar.json")))
      .unOrdered()
      .baselineColumns("result")
      .baselineValues("ok")
      .go();
  }

  @Test
  public void checkResetOption() throws Exception {
    // First set the default schema
    // Reset the session
    // Set the default schema to use-test again
    // Query dir."bar.json"
    // If session was reset, table will be cp.use-test.dir."bar.json",
    // If not, table will be cp.use-test.cp.use-test.dir."bar.json"
    testBuilder()
      .sqlQuery(format("USE %s", USE_TEST_PATH))
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, format("Default schema changed to [%s]", USE_TEST_PATH))
      .go();

    testBuilder()
      .sqlQuery("ALTER SESSION RESET ALL")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "ALL updated.")
      .go();

    testBuilder()
      .sqlQuery(format("USE %s", USE_TEST_PATH))
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, format("Default schema changed to [%s]", USE_TEST_PATH))
      .go();

    // TODO DX-10729
    // Currently not passing
//    testBuilder()
//      .sqlQuery(format("SELECT * FROM dir.%s", SqlUtils.quoteIdentifier("bar.json")))
//      .unOrdered()
//      .baselineColumns("result")
//      .baselineValues("ok")
//      .go();
  }
}
