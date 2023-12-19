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
package com.dremio.dac.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.edition.EditionProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.service.job.JobAndUserStatsRequest;
import com.dremio.service.jobs.JobsService;

/**
 * Resource that provides information about job and user activity.
 */
@RestResource
@Secured
@Path("/stats/jobsandusers")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class JobAndUserStatsResource {

  private final JobsService jobsService;
  private final String edition;
  private final OptionManager optionManager;

  @Inject
  public JobAndUserStatsResource(JobsService jobsService, EditionProvider editionProvider, OptionManager optionManager) {
    this.jobsService = jobsService;
    this.edition = editionProvider.getEdition();
    this.optionManager = optionManager;
  }

  @GET
  public JobAndUserStatsResponse getJobAndUserStats(@QueryParam("numDaysBack") @DefaultValue("7") int numberOfDaysBack,
                                                    @QueryParam("detailedStats") @DefaultValue("false") boolean detailedStats) {
    if (!optionManager.getOption(ExecConstants.ENABLE_JOBS_USER_STATS_API)) {
      throw new NotFoundException("/stats/jobsandusers is not supported");
    }
    com.dremio.service.job.JobAndUserStats jobAndUserStats;
    try {
      jobAndUserStats = jobsService.getJobAndUserStats(JobAndUserStatsRequest.newBuilder()
        .setNumDaysBack(numberOfDaysBack)
        .setDetailedStats(detailedStats)
        .build());
    } catch (Exception e) {
      throw new InternalServerErrorException(e.getMessage());
    }
    return JobAndUserStatsResponse.createJobAndUserStatsResource(jobAndUserStats, edition, detailedStats);
  }

}
