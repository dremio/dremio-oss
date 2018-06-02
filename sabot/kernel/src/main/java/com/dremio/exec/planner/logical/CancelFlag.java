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
package com.dremio.exec.planner.logical;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.dremio.exec.planner.PlannerPhase;
import com.google.common.base.Stopwatch;

public class CancelFlag extends org.apache.calcite.util.CancelFlag {

  private final Stopwatch watch = Stopwatch.createStarted();
  private final long timeout;
  private final TimeUnit timeUnit;
  private final PlannerPhase plannerPhase;

  public CancelFlag(long timeout, TimeUnit timeUnit, final PlannerPhase plannerPhase) {
    super(new AtomicBoolean());
    this.timeout = timeout;
    this.timeUnit = timeUnit;
    this.plannerPhase = plannerPhase;
  }

  public long getTimeoutInSecs() {
    final long inSecs = timeUnit.toSeconds(timeout);
    if (inSecs < 0) {
      // round it to 1 second.
      return 1;
    }
    return inSecs;
  }

  public PlannerPhase getPlannerPhase() {
    return plannerPhase;
  }

  @Override
  public boolean isCancelRequested() {
    return watch.elapsed(timeUnit) > timeout || super.isCancelRequested();
  }
}
