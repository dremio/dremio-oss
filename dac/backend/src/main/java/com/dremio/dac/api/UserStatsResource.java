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

import static com.dremio.dac.util.DateUtils.getStartOfLastMonth;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.edition.EditionProvider;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.jobs.JobsService;

/**
 * Resource that provides information about user activity.
 */
@APIResource
@Secured
@Path("/stats/user")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class UserStatsResource {
  private static final Logger logger = LoggerFactory.getLogger(UserStatsResource.class);
  private static final String FILTER = "(st=gt=%d;st=lt=%d)";

  private final JobsService jobsService;
  private final EditionProvider editionProvider;

  @Inject
  public UserStatsResource(JobsService jobsService, EditionProvider editionProvider) {
    this.jobsService = jobsService;
    this.editionProvider = editionProvider;
  }

  @GET
  public UserStats getActiveUserStats() {
    try {
      final UserStats.Builder activeUserStats = new UserStats.Builder();
      activeUserStats.setEdition(editionProvider.getEdition());

      final SearchJobsRequest request = SearchJobsRequest.newBuilder()
        .setFilterString(String.format(FILTER, getStartOfLastMonth(), System.currentTimeMillis()))
        .build();
      final Iterable<JobSummary> resultantJobs = jobsService.searchJobs(request);
      for (JobSummary job : resultantJobs) {
        activeUserStats.addUserStat(job.getStartTime(), job.getQueryType().name(), job.getUser());
      }
      return activeUserStats.build();
    } catch (Exception e) {
      logger.error("Error while computing active user stats", e);
      throw new InternalServerErrorException(e);
    }
  }
}
