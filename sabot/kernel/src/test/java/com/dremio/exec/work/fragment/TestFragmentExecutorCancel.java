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
package com.dremio.exec.work.fragment;

import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.testing.ControlsInjectionUtil;

/**
 * Run several tpch queries and inject an OutOfMemoryException in ScanBatch that will cause an OUT_OF_MEMORY outcome to
 * be propagated downstream. Make sure the proper "memory error" message is sent to the client.
 */
@Ignore("times out on gce")
public class TestFragmentExecutorCancel extends BaseTestQuery {

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  @Test
  public void testCancelNonRunningFragments() throws Exception{
    test("alter session set \"planner.slice_target\" = 10");

    // Inject an out of memory exception in the ScanBatch
    CoordinationProtos.NodeEndpoint endpoint = nodes[0].getContext().getEndpoint();
    String controlsString = "{\"injections\":[{"
      + "\"address\":\"" + endpoint.getAddress() + "\","
      + "\"port\":\"" + endpoint.getUserPort() + "\","
      + "\"type\":\"exception\","
      + "\"siteClass\":\"" + "com.dremio.exec.physical.impl.ScanBatch" + "\","
      + "\"desc\":\"" + "next-allocate" + "\","
      + "\"nSkip\":0,"
      + "\"nFire\":1,"
        + "\"exceptionClass\":\"" + OutOfMemoryException.class.getName() + "\""
      + "}]}";
    ControlsInjectionUtil.setControls(client, controlsString);

    String query = getFile("queries/tpch/04.sql");

    try {
      test(query);
      fail("The query should have failed!!!");
    } catch(UserException uex) {
      // The query should fail
    }
  }
}
