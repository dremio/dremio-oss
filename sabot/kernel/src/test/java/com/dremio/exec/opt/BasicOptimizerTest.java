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
package com.dremio.exec.opt;

import org.junit.Test;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.logical.LogicalPlan;
import com.dremio.exec.ExecTest;
import com.dremio.exec.planner.PhysicalPlanReaderTestFactory;

public class BasicOptimizerTest extends ExecTest {

    @Test
    public void parseSimplePlan() throws Exception{
        LogicalPlanPersistence lpp = PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT);
        LogicalPlan plan = LogicalPlan.parse(lpp, readResourceAsString("/scan_screen_logical.json"));
        String unparse = plan.unparse(lpp);
//        System.out.println(unparse);
        //System.out.println( new BasicOptimizer(SabotConfig.create()).convert(plan).unparse(c.getMapper().writer()));
    }
}
