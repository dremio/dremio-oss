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
package com.dremio.sabot.op.sort.external;

import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.SortPrel;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;

public class TestOOMHandling extends BaseTestQuery {

  /**
   * query should fail during in-memory sort, as no memory available to sort the records
   * @throws Exception
   */
  @Test
  public void testExternalSortWithOOM() throws Exception {
    final String controlsString = Controls.newBuilder()
      .addException(MemoryRun.class, MemoryRun.INJECTOR_OOM_ON_SORT, OutOfMemoryException.class)
      .build();
    ControlsInjectionUtil.setControls(client, controlsString);

    String query = "select l_orderkey from cp.\"tpch/lineitem.parquet\" order by l_orderkey desc";

    try {
      test(query);

      // Should never reach here.
      Assert.fail("Query did not hit the injected out-of-memory exception in FragmentExecutor#run");
    } catch (UserException uex) {
      // Verify that query has hit the injected out-of-memory exception.
      UserBitShared.DremioPBError error = uex.getOrCreatePBError(false);
      Assert.assertEquals(UserBitShared.DremioPBError.ErrorType.RESOURCE, error.getErrorType());
      Assert.assertTrue("Error message isn't related to memory error",
        uex.getMessage().contains(UserException.MEMORY_ERROR_MSG));
      Assert.assertTrue("Error doesn't have context", uex.getMessage().contains("Allocator dominators:"));
    }
  }

  /**
   * query should fail during spill, as copier cant copy any records
   * @throws Exception
   */
  @Test
  public void testExternalSortWithOOMDuringSpill() throws Exception {
    final String controlsString = Controls.newBuilder()
      .addException(DiskRunManager.class, DiskRunManager.INJECTOR_OOM_SPILL, OutOfMemoryException.class)
      .build();

    try(AutoCloseable ac = withOption(ExecConstants.EXTERNAL_SORT_ENABLE_MICRO_SPILL, true);
        AutoCloseable with = withOption(SortPrel.LIMIT, 10486784);
        AutoCloseable withres = withOption(SortPrel.RESERVE,10485760)){

      ControlsInjectionUtil.setControls(client, controlsString);

      //query taken from TestSort
      test("CREATE TABLE dfs_test.test_sort PARTITION BY (l_modline, l_moddate) AS " +
        "SELECT l.*, l_shipdate - ((EXTRACT(DAY FROM l_shipdate) - 1) * INTERVAL '1' DAY) l_moddate, " +
        "MOD(l_linenumber,3) l_modline " +
        "FROM cp.\"tpch/lineitem.parquet\" l ORDER BY l_moddate"
      );

      // Should never reach here.
      Assert.fail("Query did not hit the injected out-of-memory exception in FragmentExecutor#run");
    } catch (UserException uex) {
      // Verify that query has hit the injected out-of-memory exception.
      UserBitShared.DremioPBError error = uex.getOrCreatePBError(false);
      Assert.assertEquals(UserBitShared.DremioPBError.ErrorType.RESOURCE, error.getErrorType());
      Assert.assertTrue("Error message isn't related to memory error",
        uex.getMessage().contains(UserException.MEMORY_ERROR_MSG));
      Assert.assertTrue("Error doesn't have context", uex.getMessage().contains("Allocator dominators:"));
    }
  }
}
