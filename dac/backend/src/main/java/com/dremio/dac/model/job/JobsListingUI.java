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
package com.dremio.dac.model.job;

import com.dremio.dac.util.JSONUtil;
import com.dremio.service.job.JobSummary;
import com.dremio.service.jobs.JobsService;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import java.util.List;

/** List of jobs provided to jobs page. */
public class JobsListingUI {
  private final ImmutableList<JobListingItem> jobs;
  private final String next;
  private static JobListingItem jobListingItem;

  public JobsListingUI(
      final List<JobSummary> jobs, final JobsService jobsService, final String next) {
    this.jobs =
        FluentIterable.from(jobs)
            .transform(
                new Function<JobSummary, JobListingItem>() {
                  @Override
                  public JobListingItem apply(JobSummary input) {
                    jobListingItem = new JobListingItem(input);
                    return jobListingItem;
                  }
                })
            .toList();
    this.next = next;
  }

  @JsonCreator
  public JobsListingUI(
      @JsonProperty("jobs") List<JobListingItem> jobs, @JsonProperty("next") String next) {
    this.jobs = ImmutableList.copyOf(jobs);
    this.next = next;
  }

  public ImmutableList<JobListingItem> getJobs() {
    return jobs;
  }

  public String getNext() {
    return next;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }
}
