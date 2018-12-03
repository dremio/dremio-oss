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
package com.dremio.exec.work.foreman;

import java.util.Collections;
import java.util.List;

import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.Root;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.CoordExecRPC.SharedData;
import com.google.common.base.Preconditions;

/**
 * A plan that holds physical plan as well as parallelization info.
 */
public class ExecutionPlan {
  private final double cost;
  private final Root rootOperator;
  private final List<PlanFragment> fragments;

  private final List<SharedData> sharedData;

  public ExecutionPlan(final PhysicalPlan physicalPlan, final List<PlanFragment> fragments, final List<SharedData> sharedData) {
    Preconditions.checkNotNull(physicalPlan, "physical plan is required");
    this.rootOperator = physicalPlan.getRoot();
    this.fragments = Preconditions.checkNotNull(fragments, "work unit is required");
    this.cost = physicalPlan.getCost();
    this.sharedData = sharedData;
  }

  public ExecutionPlan(final Root rootOperator, final double cost, final List<PlanFragment> fragments) {
    this.rootOperator = Preconditions.checkNotNull(rootOperator, "Root operator is required");
    this.fragments = Preconditions.checkNotNull(fragments, "work unit is required");
    this.cost = cost;
    this.sharedData = Collections.emptyList();
  }

  public double getCost(){
    return cost;
  }

  public List<PlanFragment> getFragments() {
    return fragments;
  }

  public Root getRootOperator() {
    return rootOperator;
  }

  public List<SharedData> getSharedData() {
    return sharedData;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fragments == null) ? 0 : fragments.hashCode());
    result = prime * result + ((rootOperator == null) ? 0 : rootOperator.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ExecutionPlan other = (ExecutionPlan) obj;
    if (fragments == null) {
      if (other.fragments != null) {
        return false;
      }
    } else if (!fragments.equals(other.fragments)) {
      return false;
    }
    if (rootOperator == null) {
      if (other.rootOperator != null) {
        return false;
      }
    } else if (!rootOperator.equals(other.rootOperator)) {
      return false;
    }
    return true;
  }



}
