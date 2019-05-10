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
package com.dremio.exec.planner.observer;

import java.util.concurrent.Executor;

import com.dremio.common.SerializedExecutor;
import com.dremio.exec.work.AttemptId;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.proto.model.attempts.AttemptReason;

/**
 *
 */
public class OutOfBandQueryObserver extends AbstractQueryObserver {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OutOfBandQueryObserver.class);

  private final QueryObserver observer;
  private final Exec serializedExec;

  public OutOfBandQueryObserver(final QueryObserver observer, final Executor executor) {
    this.observer = observer;
    this.serializedExec = new Exec(executor);
  }

  @Override
  public AttemptObserver newAttempt(AttemptId attemptId, AttemptReason reason) {
    return new OutOfBandAttemptObserver(observer.newAttempt(attemptId, reason), serializedExec);
  }

  @Override
  public void execCompletion(final UserResult result) {
    serializedExec.execute(new Runnable() {
      @Override
      public void run() {
        observer.execCompletion(result);
      }
    });
  }

  private final class Exec extends SerializedExecutor<Runnable> {

    Exec(Executor underlyingExecutor) {
      super("out-of-band-observer", underlyingExecutor, false);
    }

    @Override
    protected void runException(Runnable runnable, Throwable exception) {
      logger.error("Exception occurred in out of band observer.", exception);
    }

  }

}
