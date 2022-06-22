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
package com.dremio.sabot.join.hash;

import org.junit.After;
import org.junit.Before;

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.op.join.hash.HashJoinOperator;

// Test join with build, followed by spill & then, replay.
public class TestVHashJoinSpillBuildAndReplay extends TestVHashJoinSpill {
  private final OptionManager options = testContext.getOptions();

  @Before
  public void before() {
    options.setOption(OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, HashJoinOperator.ENABLE_SPILL.getOptionName(), true));
    options.setOption(OptionValue.createLong(OptionValue.OptionType.SYSTEM, ExecConstants.TARGET_BATCH_RECORDS_MAX.getOptionName(), 65535));
    // If this option is set, the operator starts with a DiskPartition. This forces the code-path of spill write,
    // read and replay, thus testing the recursion & replay code.
    options.setOption(OptionValue.createString(OptionValue.OptionType.SYSTEM, HashJoinOperator.TEST_SPILL_MODE.getOptionName(), "buildAndReplay"));
  }

  @After
  public void after() {
    options.setOption(HashJoinOperator.ENABLE_SPILL.getDefault());
    options.setOption(ExecConstants.TARGET_BATCH_RECORDS_MAX.getDefault());
    options.setOption(HashJoinOperator.TEST_SPILL_MODE.getDefault());
  }
}
