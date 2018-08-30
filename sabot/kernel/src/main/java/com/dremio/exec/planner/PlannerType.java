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
package com.dremio.exec.planner;

import org.apache.calcite.plan.hep.HepMatchOrder;

import com.google.common.base.Preconditions;

public enum PlannerType {
  /**
   * Arbitrary heuristic, rules don't combine
   */
  HEP(true, false, HepMatchOrder.ARBITRARY),

  /**
   * Bottom Up heuristic, rules run in combination
   */
  HEP_BOTTOM_UP(true, true, HepMatchOrder.BOTTOM_UP),

  /**
   * Arbitrary heuristic, rules run in combination.
   */
  HEP_AC(true, true, HepMatchOrder.ARBITRARY),

  /**
   * Cost-based rule optimization where rules are fired in sets. Supports trait conversion for non root nodes.
   */
  VOLCANO(false, true, null);

  private final boolean hep;
  private final boolean combineRules;
  private final HepMatchOrder matchOrder;

  private PlannerType(boolean hep, boolean combineRules, HepMatchOrder matchOrder) {
    this.hep = hep;
    this.combineRules = combineRules;
    this.matchOrder = matchOrder;
  }

  public boolean isHep() {
    return hep;
  }

  public boolean isCombineRules() {
    return combineRules;
  }

  public HepMatchOrder getMatchOrder() {
    return Preconditions.checkNotNull(matchOrder);
  }
}
