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
package com.dremio.exec.store.sys.accel;

import java.util.List;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.proto.UserBitShared.QueryProfile;

/**
 * populates AccelerationDetails as a serialized byte array
 */
public interface AccelerationDetailsPopulator {

  /**
   * Report substitution
   * @param materialization
   * @param substitutions number of plans returned after substitution finished
   * @param target
   * @param millisTaken
   */
  void planSubstituted(DremioMaterialization materialization, List<RelNode> substitutions, RelNode target, long millisTaken);

  /**
   * report failures during substitution
   * @param errors list of all errors
   */
  void substitutionFailures(Iterable<String> errors);

  /**
   * Report materializations used to accelerate incoming query only if query is accelerated.
   *
   * @param info acceleration info.
   */
  void planAccelerated(SubstitutionInfo info);

  void attemptCompleted(QueryProfile profile);

  byte[] computeAcceleration();

  AccelerationDetailsPopulator NO_OP = new AccelerationDetailsPopulator() {
    @Override
    public void planSubstituted(DremioMaterialization materialization, List<RelNode> substitutions, RelNode target, long millisTaken) {
    }

    @Override
    public void substitutionFailures(Iterable<String> errors) {
    }

    @Override
    public void planAccelerated(SubstitutionInfo info) {
    }

    @Override
    public void attemptCompleted(QueryProfile profile) {
    }

    @Override
    public byte[] computeAcceleration() {
      return new byte[0];
    }
  };
}
