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

import java.io.IOException;

import com.dremio.common.utils.protos.BlackListOutput;
import com.dremio.common.utils.protos.HasherOutput;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalHash;
import com.google.common.base.Preconditions;

import io.protostuff.Output;

/***
 * Checks if a Materialization or Reflection is derived from a particular ReflectionGoal.
 */
public class ReflectionGoalChecker {
  public static final ReflectionGoalChecker Instance = new ReflectionGoalChecker();

  private final java.util.function.Function<Output, Output> reflectionGoalMaterializationBlackLister =
    new BlackListOutput.Builder()
      .blacklist(ReflectionGoal.getSchema(),
        "createdAt", "modifiedAt", "tag", "arrowCachingEnabled", "version", "name")
      .build(ReflectionGoal.getSchema());

  /***
   * Generaters a hash for the current configuration of a ReflectionGoal that are used to generate a Materialization or
   * ReflectionEntry.
   *
   * @param reflectionGoal
   * @return
   */
  public ReflectionGoalHash calculateReflectionGoalVersion(ReflectionGoal reflectionGoal){
    HasherOutput hasherOutput = new HasherOutput();
    Output blackListedOutput = reflectionGoalMaterializationBlackLister.apply(hasherOutput);
    try {
      reflectionGoal.writeTo(blackListedOutput, reflectionGoal);
      return new ReflectionGoalHash()
        .setHash(hasherOutput.getHasher().hash().toString());
    } catch (IOException ioException) {
      throw new RuntimeException(ioException);
    }
  }

  public boolean checkHash(ReflectionGoal goal, ReflectionEntry entry){
    if(null == entry.getReflectionGoalHash()){
      return false;
    }
    return entry.getReflectionGoalHash().equals(calculateReflectionGoalVersion(goal));
  }

  public boolean isEqual(ReflectionGoal goal, ReflectionEntry entry) {
    return isEqual(goal, entry.getGoalVersion());
  }

  public boolean isEqual(ReflectionGoal goal, String version) {
    Preconditions.checkNotNull(version);
    return version.equals(goal.getTag())
      || version.equals(String.valueOf(goal.getVersion()));
  }

  // Verify the given ReflectionGoal matches the given goal version from a separate KV Store.
  // The goal version might have a copy of the tag from a pre-4.2.0 build of Dremio (prior to the
  // KVStore interface changes that separated out the tag from the value).
  public static boolean checkGoal(ReflectionGoal goal, Materialization materialization) {
    return Instance.isEqual(goal, materialization.getReflectionGoalVersion());
  }

  public static boolean checkGoal(ReflectionGoal goal, ReflectionEntry entry){
    return Instance.isEqual(goal, entry.getGoalVersion());
  }
}
