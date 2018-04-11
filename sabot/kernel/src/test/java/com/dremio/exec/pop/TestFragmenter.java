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
package com.dremio.exec.pop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Test;

import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.PhysicalPlanReaderTestFactory;
import com.dremio.exec.planner.fragment.Fragment;
import com.dremio.exec.work.foreman.ForemanSetupException;

public class TestFragmenter extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestFragmenter.class);


  @Test
  public void ensureOneFragment() throws FragmentSetupException, IOException, ForemanSetupException {
    PhysicalPlanReader ppr = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT);
    Fragment b = getRootFragment(ppr, "/physical_test1.json");
    assertEquals(1, getFragmentCount(b));
    assertEquals(0, b.getReceivingExchangePairs().size());
    assertNull(b.getSendingExchange());
  }


  @Test
  public void ensureThreeFragments() throws FragmentSetupException, IOException, ForemanSetupException {
    PhysicalPlanReader ppr = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT);
    Fragment b = getRootFragment(ppr, "/physical_double_exchange.json");
    logger.debug("Fragment SabotNode {}", b);
    assertEquals(3, getFragmentCount(b));
    assertEquals(1, b.getReceivingExchangePairs().size());
    assertNull(b.getSendingExchange());

    // get first child.
    b = b.iterator().next().getNode();
    assertEquals(1, b.getReceivingExchangePairs().size());
    assertNotNull(b.getSendingExchange());

    b = b.iterator().next().getNode();
    assertEquals(0, b.getReceivingExchangePairs().size());
    assertNotNull(b.getSendingExchange());
  }








}
