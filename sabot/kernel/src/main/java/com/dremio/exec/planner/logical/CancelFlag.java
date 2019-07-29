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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.dremio.common.VM;
import com.google.common.base.Stopwatch;

public class CancelFlag extends org.apache.calcite.util.CancelFlag {

  private final Stopwatch watch = Stopwatch.createStarted();
  private final long timeout;
  private final TimeUnit timeUnit;

  public CancelFlag(long timeout, TimeUnit timeUnit) {
    super(new AtomicBoolean());
    this.timeout = timeout;
    this.timeUnit = timeUnit;
  }

  /**
   * Reset the flag
   */
  public void reset() {
    this.atomicBoolean.set(false);
    watch.reset();
    watch.start();
  }

  public long getTimeoutInSecs() {
    final long inSecs = timeUnit.toSeconds(timeout);
    if (inSecs < 0) {
      // round it to 1 second.
      return 1;
    }
    return inSecs;
  }

  @Override
  public boolean isCancelRequested() {
    if(!VM.isDebugEnabled() && watch.elapsed(timeUnit) > timeout) {
      return true;
    }

    return super.isCancelRequested();
  }
}
