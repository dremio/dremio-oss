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

import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.dremio.sabot.exec.WorkloadTicketDepot;
import com.dremio.sabot.exec.WorkloadTicketDepotService;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.dremio.sabot.exec.fragment.FragmentExecutorBuilder;
import com.dremio.sabot.op.receiver.IncomingBuffers;

public class TestOutOfMemoryException extends BaseTestQuery {
  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  @Test
  public void testIncomingBuffersOOM() throws Exception {
    setSessionOption(ExecConstants.SLICE_TARGET, "10");

    final String controlsString = Controls.newBuilder()
      .addException(IncomingBuffers.class, IncomingBuffers.INJECTOR_DO_WORK, OutOfMemoryException.class)
      .build();
    ControlsInjectionUtil.setControls(client, controlsString);

    String query = "select l_orderkey from cp.\"tpch/lineitem.parquet\" group by l_orderkey having sum(l_quantity) > 300";

    try {
      test(query);

      // Should never reach here.
      Assert.fail("Query did not hit the injected out-of-memory exception");
    } catch (UserException uex) {
      // Verify that query has hit the injected out-of-memory exception.
      DremioPBError error = uex.getOrCreatePBError(false);
      Assert.assertEquals(DremioPBError.ErrorType.RESOURCE, error.getErrorType());
      Assert.assertTrue("Error message isn't related to memory error",
        uex.getMessage().contains(UserException.MEMORY_ERROR_MSG));

      // Verify that allocators have released all memory.
      final WorkloadTicketDepot ticketDepot =
        getBindingProvider().lookup(WorkloadTicketDepotService.class).getTicketDepot();
      final long totalAllocatedMemory = ticketDepot.getWorkloadTickets().stream()
        .mapToLong(t -> t.getAllocator().getAllocatedMemory())
        .sum();
      Assert.assertEquals(0, totalAllocatedMemory);
    }
  }

  @Test
  @Ignore
  public void testFragmentExecutorOOM() throws Exception {
    setSessionOption(ExecConstants.SLICE_TARGET, "10");

    final String controlsString = Controls.newBuilder()
        .addException(FragmentExecutor.class, FragmentExecutor.INJECTOR_DO_WORK, OutOfMemoryError.class)
        .build();
    ControlsInjectionUtil.setControls(client, controlsString);

    String query = "select l_orderkey from cp.\"tpch/lineitem.parquet\" group by l_orderkey having sum(l_quantity) > 300";

    try {
      test(query);

      // Should never reach here.
      Assert.fail("Query did not hit the injected out-of-memory exception in FragmentExecutor#run");
    } catch (UserException uex) {
      // Verify that query has hit the injected out-of-memory exception.
      DremioPBError error = uex.getOrCreatePBError(false);
      Assert.assertEquals(DremioPBError.ErrorType.RESOURCE, error.getErrorType());
      Assert.assertTrue("Error message isn't related to memory error",
          uex.getMessage().contains(UserException.MEMORY_ERROR_MSG));
      Assert.assertTrue("Error doesn't have context", uex.getMessage().contains("Allocator dominators:"));
    }
  }

  @Test
  @Ignore
  public void testFragmentExecutorBuildOOM() throws Exception {
    setSessionOption(ExecConstants.SLICE_TARGET, "10");

    final String controlsString = Controls.newBuilder()
            .addException(FragmentExecutorBuilder.class, FragmentExecutorBuilder.INJECTOR_DO_WORK, OutOfMemoryException.class)
            .build();
    ControlsInjectionUtil.setControls(client, controlsString);

    String query = "select l_orderkey from cp.\"tpch/lineitem.parquet\" group by l_orderkey having sum(l_quantity) > 300";

    try {
      test(query);

      // Should never reach here.
      Assert.fail("Query did not hit the injected out-of-memory exception in FragmentExecutorBuilder#build");
    } catch (UserException uex) {
      // Verify that query has hit the injected out-of-memory exception.
      DremioPBError error = uex.getOrCreatePBError(false);
      Assert.assertEquals(DremioPBError.ErrorType.RESOURCE, error.getErrorType());
      Assert.assertTrue("Error message isn't related to memory error",
              uex.getMessage().contains(UserException.MEMORY_ERROR_MSG));
      Assert.assertTrue("Error doesn't have context", uex.getMessage().contains("Allocator dominators:"));
    }
  }

}
