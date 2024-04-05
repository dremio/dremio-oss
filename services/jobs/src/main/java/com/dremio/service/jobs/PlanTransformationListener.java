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
package com.dremio.service.jobs;

import com.dremio.exec.planner.PlannerPhase;
import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.rel.RelNode;

/**
 * Listener that is notified after every planning phase. This is used only in tests & works only
 * when the listener is in the same process as LocalJobsService.
 */
@VisibleForTesting
public interface PlanTransformationListener {

  PlanTransformationListener NO_OP = new PlanTransformationListener() {};

  default void onPhaseCompletion(
      PlannerPhase phase, RelNode before, RelNode after, long millisTaken) {}
}
