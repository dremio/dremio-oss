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

import java.io.IOException;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.ExecTest;
import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.Fragment;
import com.dremio.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import com.dremio.exec.planner.fragment.MakeFragmentsVisitor;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public abstract class PopUnitTestBase extends ExecTest {
//  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PopUnitTestBase.class);

  public static int getFragmentCount(Fragment b) {
    int i = 1;
    for (ExchangeFragmentPair p : b) {
      i += getFragmentCount(p.getNode());
    }
    return i;
  }

  public static Fragment getRootFragment(PhysicalPlanReader reader, String file) throws FragmentSetupException,
      IOException, ForemanSetupException {
    return getRootFragmentFromPlanString(reader, Files.toString(FileUtils.getResourceAsFile(file), Charsets.UTF_8));
  }


  public static Fragment getRootFragmentFromPlanString(PhysicalPlanReader reader, String planString)
      throws FragmentSetupException, IOException, ForemanSetupException {
    PhysicalPlan plan = reader.readPhysicalPlan(planString);
    PhysicalOperator o = plan.getSortedOperators(false).iterator().next();
    return o.accept(MakeFragmentsVisitor.INSTANCE, null);
  }
}
