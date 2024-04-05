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
package com.dremio.service.reflection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.Set;

/** Represents all dependencies extracted from a completed refresh job */
public class ExtractedDependencies {
  private final Set<DependencyEntry> planDependencies;
  private final Set<DependencyEntry> decisionDependencies;

  ExtractedDependencies(
      Set<DependencyEntry> planDependencies, Set<DependencyEntry> decisionDependencies) {
    this.planDependencies =
        ImmutableSet.copyOf(
            Preconditions.checkNotNull(planDependencies, "plan dependencies required"));
    this.decisionDependencies =
        ImmutableSet.copyOf(
            Preconditions.checkNotNull(decisionDependencies, "decision dependencies required"));
  }

  public boolean isEmpty() {
    return planDependencies.isEmpty() && decisionDependencies.isEmpty();
  }

  /**
   * @return all dependencies extracted from the materialization plan
   */
  public Set<DependencyEntry> getPlanDependencies() {
    return planDependencies;
  }

  /**
   * @return all physical dependencies stored in the refresh decision
   */
  public Set<DependencyEntry> getDecisionDependencies() {
    return decisionDependencies;
  }
}
