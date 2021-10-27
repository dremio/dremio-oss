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
package com.dremio.exec.planner.logical;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link CancelFlag}.
 */
public class CancelFlagTest {

  @Test
  public void testIsNotStartedUntilReset () {
    CancelFlag cancelFlag = new CancelFlag(Long.MAX_VALUE);
    Assert.assertFalse("The cancel flag should not be running until first reset",
        cancelFlag.watch.isRunning());
    cancelFlag.reset();
    Assert.assertTrue("The cancel flag should be running after reset",
        cancelFlag.watch.isRunning());
  }

  @Test
  public void testIsNotRunningAfterStop () {
    CancelFlag cancelFlag = new CancelFlag(Long.MAX_VALUE);
    cancelFlag.reset();
    Assert.assertTrue("The cancel flag should be running after reset",
      cancelFlag.watch.isRunning());
    cancelFlag.stop();
    Assert.assertFalse("The cancel flag should not be running after stop",
      cancelFlag.watch.isRunning());
  }
}
