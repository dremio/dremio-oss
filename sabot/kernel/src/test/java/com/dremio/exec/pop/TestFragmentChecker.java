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
package com.dremio.exec.pop;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.exec.maestro.AbstractMaestroObserver;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.PhysicalPlanReaderTestFactory;
import com.dremio.exec.planner.fragment.Fragment;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.fragment.PlanFragmentsIndex;
import com.dremio.exec.planner.fragment.SimpleParallelizer;
import com.dremio.exec.proto.CoordExecRPC.QueryContextInformation;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.util.Utilities;
import com.dremio.options.OptionList;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.collect.Lists;

@Ignore("DX-3872")
public class TestFragmentChecker extends PopUnitTestBase{

  @Test
  public void checkSimpleExchangePlan() throws Exception{
    print("/physical_double_exchange.json", 2, 3);

  }

  private void print(String fragmentFile, int bitCount, int expectedFragmentCount) throws Exception{
    System.out.println(String.format("=================Building plan fragments for [%s].  Allowing %d total Nodes.==================", fragmentFile, bitCount));
    PhysicalPlanReader ppr = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT);
    Fragment fragmentRoot = getRootFragment(ppr, fragmentFile);
    SimpleParallelizer par = new SimpleParallelizer(1000*1000, 5, 10, 1.2, AbstractMaestroObserver.NOOP, true, 1.5d, false);
    List<NodeEndpoint> endpoints = Lists.newArrayList();
    NodeEndpoint localBit = null;
    for(int i =0; i < bitCount; i++) {
      NodeEndpoint b1 = NodeEndpoint.newBuilder().setAddress("localhost").setFabricPort(1234+i).build();
      if (i == 0) {
        localBit = b1;
      }
      endpoints.add(b1);
    }

    final QueryContextInformation queryContextInfo = Utilities.createQueryContextInfo("dummySchemaName");
    List<PlanFragmentFull> qwu = par.getFragments(new OptionList(), localBit, QueryId.getDefaultInstance(), ppr, fragmentRoot,
        new PlanFragmentsIndex.Builder(),
        UserSession.Builder.newBuilder().withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName("foo").build()).build(),
        queryContextInfo,
        null);

    assertEquals(expectedFragmentCount,
        qwu.size() + 1 /* root fragment is not part of the getFragments() list*/);
  }

  @Test
  public void validateSingleExchangeFragment() throws Exception{
    print("/physical_single_exchange.json", 1, 2);

  }

}
