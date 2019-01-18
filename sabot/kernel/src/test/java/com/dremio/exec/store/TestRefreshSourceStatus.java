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

package com.dremio.exec.store;

import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestRefreshSourceStatus extends BaseTestQuery {

  @Test
  public void testRefreshSourceStatus() throws Exception {
    test("USE dfs");
    testBuilder()
      .sqlQuery("ALTER SOURCE dfs_test REFRESH STATUS")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Successfully refreshed status for source 'dfs_test'. New status is: Healthy")
      .build()
      .run();
  }

}
