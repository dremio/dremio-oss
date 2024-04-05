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

import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.RelWithInfo;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.proto.UserBitShared.AccelerationProfile;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.reflection.hints.ReflectionExplanationsAndQueryDistance;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelNode;

/** populates AccelerationDetails as a serialized byte array */
public interface AccelerationDetailsPopulator {

  /**
   * Report substitution
   *
   * @param materialization
   * @param substitutions number of plans returned after substitution finished
   * @param target
   * @param millisTaken
   */
  void planSubstituted(
      DremioMaterialization materialization,
      List<RelWithInfo> substitutions,
      RelNode target,
      long millisTaken,
      boolean defaultReflection);

  /**
   * report failures during substitution
   *
   * @param errors list of all errors
   */
  void substitutionFailures(Iterable<String> errors);

  /**
   * Report materializations used to accelerate incoming query only if query is accelerated.
   *
   * @param info acceleration info.
   */
  void planAccelerated(SubstitutionInfo info);

  void addReflectionHints(
      ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance);

  void attemptCompleted(QueryProfile profile);

  byte[] computeAcceleration();

  List<String> getConsideredReflectionIds();

  List<String> getMatchedReflectionIds();

  List<String> getChosenReflectionIds();

  void planConvertedToRel(RelNode converted);

  void restoreAccelerationProfile(AccelerationProfile accelerationProfile);

  AccelerationDetailsPopulator NO_OP =
      new AccelerationDetailsPopulator() {
        @Override
        public void planSubstituted(
            DremioMaterialization materialization,
            List<RelWithInfo> substitutions,
            RelNode target,
            long millisTaken,
            boolean defaultReflection) {}

        @Override
        public void substitutionFailures(Iterable<String> errors) {}

        @Override
        public void planAccelerated(SubstitutionInfo info) {}

        @Override
        public void addReflectionHints(
            ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {}

        @Override
        public void attemptCompleted(QueryProfile profile) {}

        @Override
        public byte[] computeAcceleration() {
          return new byte[0];
        }

        @Override
        public List<String> getConsideredReflectionIds() {
          return Collections.emptyList();
        }

        @Override
        public List<String> getMatchedReflectionIds() {
          return Collections.emptyList();
        }

        @Override
        public List<String> getChosenReflectionIds() {
          return Collections.emptyList();
        }

        @Override
        public void planConvertedToRel(RelNode converted) {}

        @Override
        public void restoreAccelerationProfile(AccelerationProfile profile) {}
      };
}
