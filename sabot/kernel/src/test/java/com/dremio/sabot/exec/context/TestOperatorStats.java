
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
package com.dremio.sabot.exec.context;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestOperatorStats {

  @Test
  public void testWaitInProcessing() throws Exception {
    OpProfileDef profileDef = new OpProfileDef(0 /*operatorId*/, 0 /*operatorType*/, 0 /*inputCount*/);
    OperatorStats stats = new OperatorStats(profileDef, null /*allocator*/);

    long startTime = System.nanoTime();
    stats.startProcessing();
    stats.startWait();
    Thread.sleep(1);
    stats.stopWait();
    stats.stopProcessing();
    long elapsedTime = System.nanoTime() - startTime;

    assertTrue("Expected wait time is non-zero, but got zero wait time", stats.getWaitNanos() > 0);
    assertTrue("Expected setup time is zero, but got non-zero setup time", stats.getSetupNanos() == 0);

    long totalTime = stats.getProcessingNanos() + stats.getSetupNanos() + stats.getWaitNanos();
    assertTrue("Expected total time (" + totalTime + ") to be <= elapsedTime (" + elapsedTime +")", totalTime <= elapsedTime);
  }

  @Test
  public void testWaitInSetup() throws Exception {
    OpProfileDef profileDef = new OpProfileDef(0 /*operatorId*/, 0 /*operatorType*/, 0 /*inputCount*/);
    OperatorStats stats = new OperatorStats(profileDef, null /*allocator*/);

    long startTime = System.nanoTime();
    stats.startProcessing();
    stats.startSetup();
    stats.startWait();
    Thread.sleep(1);
    stats.stopWait();
    stats.stopSetup();
    stats.stopProcessing();
    long elapsedTime = System.nanoTime() - startTime;

    assertTrue("Expected wait time is non-zero, but got zero wait time", stats.getWaitNanos() > 0);

    long totalTime = stats.getProcessingNanos() + stats.getSetupNanos() + stats.getWaitNanos();
    assertTrue("Expected total time (" + totalTime + ") to be <= elapsedTime (" + elapsedTime +")", totalTime <= elapsedTime);
  }
}
