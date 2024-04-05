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
package com.dremio.dac.resource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

/** Statistic for exported profiles */
public class ExportProfilesStats {

  private final long jobsCount;
  private final long profilesCount;
  private final long skippedProfilesCount;
  private final String outputPath;

  @JsonCreator
  public ExportProfilesStats(
      @JsonProperty("jobsCount") long jobsCount,
      @JsonProperty("profilesCount") long profilesCount,
      @JsonProperty("skippedProfilesCount") long skippedProfilesCount,
      @JsonProperty("outputPath") String outputPath) {
    this.jobsCount = jobsCount;
    this.profilesCount = profilesCount;
    this.skippedProfilesCount = skippedProfilesCount;
    this.outputPath = outputPath;
  }

  public long getJobsCount() {
    return jobsCount;
  }

  public long getProfilesCount() {
    return profilesCount;
  }

  public long getSkippedProfilesCount() {
    return skippedProfilesCount;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public String retrieveStats(LocalDateTime fromDate, LocalDateTime toDate, boolean isTimeSet) {

    String stats =
        String.format(
            "Export completed. Jobs processed: %d, profiles processed: %d, profiles skipped: %d",
            getJobsCount(), getProfilesCount(), getSkippedProfilesCount());

    if (profilesCount == 0) {
      if (fromDate != null && toDate != null) {
        if (isTimeSet) {
          return String.format(
              "No profiles were found from %s to %s.", fromDate.toString(), toDate.toString());
        } else {
          return String.format(
              "Defaulting to %s to %s. No profiles were found for the duration.",
              fromDate.toString(), toDate.toString());
        }
      }
      return "No profiles were found.";
    }

    return stats + String.format("\nOutput path: %s", getOutputPath());
  }
}
