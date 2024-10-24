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

import static com.dremio.service.job.proto.JobState.EXECUTION_PLANNING;
import static com.dremio.service.job.proto.JobState.METADATA_RETRIEVAL;
import static com.dremio.service.job.proto.JobState.PENDING;
import static com.dremio.service.job.proto.JobState.PLANNING;
import static com.dremio.service.job.proto.JobState.RUNNING;
import static com.dremio.service.job.proto.JobState.STARTING;

import com.dremio.service.job.proto.JobState;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MapFilterToJobState {
  private static String setupFilter = "";
  private static String runningFilter = "";

  static void init() {
    Map<String, List<JobState>> filterToJobStateMap = new HashMap<>();
    filterToJobStateMap.put("SETUP", Arrays.asList(PENDING, METADATA_RETRIEVAL, PLANNING));
    filterToJobStateMap.put("RUNNING", Arrays.asList(EXECUTION_PLANNING, STARTING, RUNNING));
    filterToJobStateMap = Collections.unmodifiableMap(filterToJobStateMap);

    filterToJobStateMap.get("SETUP").forEach((v) -> setupFilter += "jst==" + v.name() + ",");
    setupFilter = setupFilter.substring(0, setupFilter.length() - 1);

    filterToJobStateMap.get("RUNNING").forEach((v) -> runningFilter += "jst==" + v.name() + ",");
    runningFilter = runningFilter.substring(0, runningFilter.length() - 1);
  }

  static String map(String filterString) {
    filterString = filterString.replace("jst==\"SETUP\"", setupFilter);
    return filterString.replace("jst==\"RUNNING\"", runningFilter);
  }
}
