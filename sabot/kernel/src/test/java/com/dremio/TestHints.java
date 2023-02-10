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

import org.junit.Test;

/**
 * Tests for query hints.
 */
public class TestHints extends BaseTestQuery {
  /**
   * Test that we throw parser errors when using hints in parts of the query we do not allow hints to be.
   * @throws Exception
   */
  @Test
  public void invalidHintPlacement() throws Exception {
    final String[] testQueries = new String[]{
      "select 1 /*+ BADHINT */",
      "select * /*+ BROADCAST */ from INFORMATION_SCHEMA.COLUMNS",
      "select * from /*+ BROADCAST */ INFORMATION_SCHEMA.COLUMNS"
    };

    for (String query : testQueries) {
      this.errorMsgTestHelper(query, "Failure parsing the query");
    }
  }

  /**
   * Test that using a registered hint does not throw errors if used in a place we support hints.
   * @throws Exception
   */
  @Test
  public void validHintUsage() throws Exception {
    final String[] testQueries = new String[]{
      "select * from INFORMATION_SCHEMA.COLUMNS /*+ BROADCAST */",
      "select /*+ BROADCAST */ * from INFORMATION_SCHEMA.COLUMNS"
    };

    for (String query : testQueries) {
      this.test(query);
    }
  }

  /**
   * Test that we return an error when the user specifies an unregistered hint.
   * @throws Exception
   */
  @Test
  public void unregisteredHints() throws Exception {
    final String[] testQueries = new String[]{
      "select * from INFORMATION_SCHEMA.COLUMNS /*+ BADHINT */",
      "select /*+ BADHINT */ * from INFORMATION_SCHEMA.COLUMNS"
    };

    for (String query : testQueries) {
      this.errorMsgTestHelper(query, "BADHINT should be registered in the HintStrategies.");
    }
  }
}
