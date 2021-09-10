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
package com.dremio.exec.planner;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.proto.UserBitShared.LayoutMaterializedViewProfile;

public final class CachedAccelDetails {

  private SubstitutionInfo substitutionInfo;
  private final Map<DremioMaterialization, RelNode> materializationStore = new HashMap<>();
  private final Map<String, LayoutMaterializedViewProfile> accelerationProfileStore = new HashMap<>();

  public void setSubstitutionInfo(SubstitutionInfo info) {
    substitutionInfo = info;
  }

  public SubstitutionInfo getSubstitutionInfo() {
    return substitutionInfo;
  }

  public void addMaterialization(DremioMaterialization materialization, RelNode target) {
    materializationStore.put(materialization, target);
  }

  public void addLmvProfile(String key, LayoutMaterializedViewProfile profile) {
    accelerationProfileStore.put(key, profile);
  }

  public LayoutMaterializedViewProfile getLmvProfile(String key) {
    return accelerationProfileStore.get(key);
  }

  public Map<DremioMaterialization, RelNode> getMaterializationStore() {
    return materializationStore;
  }

}
