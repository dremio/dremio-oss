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
package com.dremio.exec.catalog;

import com.dremio.exec.proto.UserBitShared.PlanPhaseProfile;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.concurrent.ThreadSafe;

/**
 * For collecting and logging table metadata access statistics to query profiles.
 *
 * <p>This class is thread-safe since there can be multiple concurrent writers for bulk catalog
 * access.
 */
@ThreadSafe
public class MetadataStatsCollector {
  private final ConcurrentLinkedQueue<PlanPhaseProfile> planPhaseProfiles =
      new ConcurrentLinkedQueue<>();

  public void addDatasetStat(String datasetPath, String type, long millisTaken) {
    planPhaseProfiles.add(
        PlanPhaseProfile.newBuilder()
            .setPhaseName(String.format("%s: %s", datasetPath, type))
            .setDurationMillis(millisTaken)
            .build());
  }

  public List<PlanPhaseProfile> getPlanPhaseProfiles() {
    return planPhaseProfiles.stream().collect(ImmutableList.toImmutableList());
  }
}
