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
package com.dremio.exec.planner.observer;

import com.dremio.common.SerializedExecutor;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.context.RequestContext;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.proto.model.attempts.AttemptReason;
import java.util.concurrent.Executor;
import javax.inject.Provider;

public class OutOfBandQueryObserver extends AbstractQueryObserver {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(OutOfBandQueryObserver.class);

  private final QueryObserver observer;
  private final Exec serializedExec;
  private final Provider<RequestContext> requestContextProvider;

  public OutOfBandQueryObserver(
      final QueryObserver observer,
      final Executor executor,
      final Provider<RequestContext> requestContextProvider) {
    this.observer = observer;
    this.serializedExec = new Exec(executor);
    this.requestContextProvider = requestContextProvider;
  }

  @Override
  public AttemptObserver newAttempt(AttemptId attemptId, AttemptReason reason) {
    return new OutOfBandAttemptObserver(
        observer.newAttempt(attemptId, reason), serializedExec, requestContextProvider);
  }

  @Override
  public void execCompletion(final UserResult result) {
    serializedExec.execute(
        new Runnable() {
          @Override
          public void run() {
            observer.execCompletion(result);
          }
        });
  }

  @Override
  public void planParallelized(PlanningSet planningSet) {
    serializedExec.execute(
        new Runnable() {
          @Override
          public void run() {
            observer.planParallelized(planningSet);
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
