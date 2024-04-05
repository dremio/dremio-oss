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

import com.dremio.common.utils.protos.AttemptId;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.proto.model.attempts.AttemptReason;

/** Observes the general life cycle of a QueryJob(multiple attempts). */
public interface QueryObserver {

  /**
   * called whenever the Foreman is about to start a new attempt
   *
   * @param attemptId internal QueryId for the new attempt
   * @param reason why do we need to re-attempt
   * @return AttemptObserver for the new attempt
   */
  AttemptObserver newAttempt(AttemptId attemptId, AttemptReason reason);

  /** called when QueryJob is done and no more queries will be attempted */
  void execCompletion(UserResult result);

  default void planParallelized(PlanningSet planningSet) {}
  ;
}
