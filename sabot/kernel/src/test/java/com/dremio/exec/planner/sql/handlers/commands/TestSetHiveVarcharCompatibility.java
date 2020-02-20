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

import org.junit.Test;

import com.dremio.BaseTestQuery;

/**
 * Test failure cases for 'ALTER TABLE tbl ENABLE HIVE VARCHAR COMPATIBILITY'
 */
public class TestSetHiveVarcharCompatibility extends BaseTestQuery {

  @Test
  public void testOnSystables() throws Exception {
    errorMsgTestHelper("ALTER TABLE sys.version ENABLE HIVE VARCHAR COMPATIBILITY",
        "[sys.version] is a SYSTEM_TABLE");
  }

  @Test
  public void testOnViews() throws Exception {
    runSQL("CREATE VDS dfs_test.SYS_OP_VDS AS SELECT * FROM SYS.OPTIONS");
    errorMsgTestHelper("ALTER VDS dfs_test.SYS_OP_VDS ENABLE HIVE VARCHAR COMPATIBILITY",
        "[dfs_test.SYS_OP_VDS] is a VIEW");
    errorMsgTestHelper("ALTER VDS dfs_test.SYS_OP_VDS DISABLE HIVE VARCHAR COMPATIBILITY",
        "[dfs_test.SYS_OP_VDS] is a VIEW");
    errorMsgTestHelper("ALTER TABLE dfs_test.SYS_OP_VDS ENABLE HIVE VARCHAR COMPATIBILITY",
        "[dfs_test.SYS_OP_VDS] is a VIEW");
  }

  @Test
  public void testOnNonHiveTable() throws Exception {
    runSQL("CREATE TABLE dfs_test.SYS_OP_TABLE AS SELECT * FROM SYS.OPTIONS");
    errorMsgTestHelper("ALTER TABLE dfs_test.SYS_OP_TABLE ENABLE HIVE VARCHAR COMPATIBILITY",
        "source [dfs_test] doesn't support width property for varchars");
  }

}
