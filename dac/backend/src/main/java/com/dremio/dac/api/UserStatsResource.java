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
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

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
  public UserStats getActiveUserStats(@QueryParam("start") @DefaultValue("0") long startEpoch ,
                                      @QueryParam("end") @DefaultValue("0") long endEpoch) {
    try {
      final UserStats.Builder activeUserStats = new UserStats.Builder();
      activeUserStats.setEdition(editionProvider.getEdition());
      final String filter = createJobFilter(startEpoch,endEpoch);
      final SearchJobsRequest request = SearchJobsRequest.newBuilder()
        .setFilterString(filter)
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

  private String createJobFilter(long startEpoch, long endEpoch) {
    final long end = endEpoch > 0 ? endEpoch : System.currentTimeMillis();
    final long start = startEpoch > 0 ? startEpoch : getStartOfLastMonth();
    final String filter = String.format(FILTER, start, end);
    return filter;
  }
}
