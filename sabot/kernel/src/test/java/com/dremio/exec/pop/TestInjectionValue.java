/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.junit.Test;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.ExecTest;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.Screen;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.PhysicalPlanReaderTestFactory;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestInjectionValue extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestInjectionValue.class);

  @Test
  public void testInjected() throws Exception{
    PhysicalPlanReader r = PhysicalPlanReaderTestFactory.defaultPhysicalPlanReader(DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT);
    PhysicalPlan p = r.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/physical_screen.json"), Charsets.UTF_8));

    List<PhysicalOperator> o = p.getSortedOperators(false);

    PhysicalOperator op = o.iterator().next();
    assertEquals(Screen.class, op.getClass());
    Screen s = (Screen) op;
  }
}
