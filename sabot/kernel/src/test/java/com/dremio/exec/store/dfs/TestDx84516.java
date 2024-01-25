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
package com.dremio.exec.store.dfs;

import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;

public class TestDx84516 extends BaseTestQuery {
  @BeforeClass
  public static void setup() throws Exception {
    resetSystemOption(ExecConstants.FS_PATH_TRAVERSAL_PREVENTION_ENABLED.getOptionName());
    // We can use dfs_test as it is filesystem-based source with auto-promotion enabled
    testNoResult("CREATE TABLE dfs_test.dx84516_tbl1 AS VALUES(1)");
    testNoResult("CREATE TABLE dfs_test.dx84516_tbl2 AS VALUES(2)");
  }

  @Test
  public void testEnforcementEnabled() throws Exception {
    try (AutoCloseable ac = setPathTraversalPreventionEnabled(true)) {
      errorMsgTestHelper(
        "SELECT * FROM dfs_test.dx84516_tbl1.\"..\".dx84516_tbl2",
        "Not allowed to perform directory traversal");
    }
  }

  @Test
  public void testEnforcementDisabled() throws Exception {
    try (AutoCloseable ac = setPathTraversalPreventionEnabled(false)) {
      runSQL("SELECT * FROM dfs_test.dx84516_tbl1.\"..\".dx84516_tbl2");
    }
  }

  @Test
  public void testEnforcementNoOptionOverride() {
    resetSystemOption(ExecConstants.FS_PATH_TRAVERSAL_PREVENTION_ENABLED.getOptionName());
    errorMsgTestHelper(
      "SELECT * FROM dfs_test.dx84516_tbl1.\"..\".dx84516_tbl2",
      "Not allowed to perform directory traversal");
  }

  private static AutoCloseable setPathTraversalPreventionEnabled(boolean enabled) {
    setSystemOption(ExecConstants.FS_PATH_TRAVERSAL_PREVENTION_ENABLED, Boolean.toString(enabled));
    return () ->
      resetSystemOption(ExecConstants.FS_PATH_TRAVERSAL_PREVENTION_ENABLED.getOptionName());
  }
}
