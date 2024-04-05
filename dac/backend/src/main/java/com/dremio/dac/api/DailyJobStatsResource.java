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

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.edition.EditionProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.service.job.JobStats;
import com.dremio.service.job.JobStatsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.jobs.JobsService;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource for daily job statistics. represents the below tuple for the last 10 days: (date ,
 * job_type , count)
 */
@APIResource
@Secured
@Path("/cluster/jobstats")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Deprecated
public class DailyJobStatsResource {
  private static final Logger logger = LoggerFactory.getLogger(DailyJobStatsResource.class);
  private static final String FILTER = "(st=gt=%d;st=lt=%d)";
  private static final String DREMIO_EDITION_FORMAT = "dremio-%s-%s";
  private static final int STAT_DURATION = 10; // stats are for 10 days by default

  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
  private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  private final JobsService jobsService;
  private final String edition;

  private final OptionManager optionManager;

  @Inject
  public DailyJobStatsResource(
      JobsService jobsService, EditionProvider editionProvider, OptionManager optionManager) {
    this.jobsService = jobsService;
    this.edition = editionProvider.getEdition();
    this.optionManager = optionManager;
  }

  @GET
  @RolesAllowed({"admin", "user"})
  public DailyJobStats getStats(
      @QueryParam("start") @DefaultValue("0") long startEpoch,
      @QueryParam("end") @DefaultValue("0") long endEpoch,
      @QueryParam("onlyDateWiseTotals") @DefaultValue("false") String onlyDateWiseTotals) {
    if (!optionManager.getOption(ExecConstants.ENABLE_DEPRECATED_JOBS_USER_STATS_API)) {
      throw new NotFoundException("/cluster/jobstats is not supported");
    }
    if (Boolean.parseBoolean(onlyDateWiseTotals)) {
      return createStatsWithonlyDateWiseTotals(startEpoch, endEpoch);
    }
    return createStats(startEpoch, endEpoch);
  }

  private DailyJobStatsResource.DailyJobStats createStatsWithonlyDateWiseTotals(
      long startEpoch, long endEpoch) {
    List<Map<String, Object>> result = Collections.synchronizedList(new ArrayList<>());

    try {
      Consumer<Long[]> consumer =
          (pair) -> {
            long iterStartDate = pair[0];
            long iterEndDate = pair[1];
            logger.debug(
                "StartTime: {}, EndTime:{}",
                sdf2.format(new Date(iterStartDate)),
                sdf2.format(new Date(iterEndDate)));

            JobStatsRequest jobStatsRequest =
                JobStatsRequest.newBuilder()
                    .setStartDate(StatsUtils.convert(iterStartDate))
                    .setEndDate(StatsUtils.convert(iterEndDate))
                    .addJobStatsType(JobStats.Type.DAILY_JOBS)
                    .build();
            JobStats jobStats = jobsService.getJobStats(jobStatsRequest);

            long total = jobStats.getCountsList().stream().mapToInt(x -> x.getCount()).sum();
            if (total > 0) {
              Map<String, Object> countMap = new HashMap<>();
              result.add(countMap);
              countMap.put("date", sdf.format(new java.util.Date(iterStartDate)));
              countMap.put("total", total);
            }
          };

      StatsUtils.executeDateWise(startEpoch, endEpoch, consumer);

      DailyJobStats dailyJobStats = new DailyJobStats(result);
      dailyJobStats.setEdition(edition);
      return dailyJobStats;

    } catch (Exception err) {
      logger.error("DailyJobStats failed.", err);
      throw new InternalServerErrorException(err);
    }
  }

  private DailyJobStatsResource.DailyJobStats createStats(long startEpoch, long endEpoch) {
    try {
      final SearchJobsRequest.Builder requestBuilder = SearchJobsRequest.newBuilder();
      final String filter = createJobFilter(startEpoch, endEpoch);
      requestBuilder.setFilterString(filter);

      final DailyJobStats stats =
          aggregateJobResults(jobsService.searchJobs(requestBuilder.build()));
      return stats;
    } catch (Exception err) {
      logger.error("DailyJobStats failed: " + err.getMessage());
      throw new InternalServerErrorException(err);
    }
  }

  private String createJobFilter(long startEpoch, long endEpoch) {
    final long end = endEpoch > 0 ? endEpoch : System.currentTimeMillis();
    final long start = startEpoch > 0 ? startEpoch : end - TimeUnit.DAYS.toMillis(STAT_DURATION);
    final String filter = String.format(FILTER, start, end);
    return filter;
  }

  @VisibleForTesting
  public DailyJobStatsResource.DailyJobStats aggregateJobResults(final Iterable<JobSummary> jobs) {

    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    // mapping of date to per-jobType stats
    final Map<String, Map<String, Object>> results = new HashMap<>();

    for (final JobSummary jobSummary : jobs) {
      final String dateKey = sdf.format(new Date(jobSummary.getStartTime()));
      final String jobType = jobSummary.getQueryType().name();

      Map<String, Object> jobTypeCounter = results.get(dateKey);
      if (jobTypeCounter == null) {
        jobTypeCounter = new HashMap<>();
      }
      jobTypeCounter.putIfAbsent("date", dateKey);
      jobTypeCounter.put("total", (Long) jobTypeCounter.getOrDefault("total", 0L) + 1L);
      jobTypeCounter.put(jobType, (Long) jobTypeCounter.getOrDefault(jobType, 0L) + 1L);

      results.put(dateKey, jobTypeCounter);
    }

    final DailyJobStats dailyJobStats = new DailyJobStats(new ArrayList<>(results.values()));
    dailyJobStats.setEdition(edition);

    return dailyJobStats;
  }

  /** holds the response of the REST call */
  public static class DailyJobStats {
    private String edition;
    private List<Map<String, Object>> jobStats;

    @JsonCreator
    DailyJobStats(@JsonProperty("jobStats") List<Map<String, Object>> jobStats) {
      this.jobStats = jobStats;
    }

    public String getEdition() {
      return edition;
    }

    public void setEdition(String edition) {
      this.edition = String.format(DREMIO_EDITION_FORMAT, edition, DremioVersionInfo.getVersion());
    }

    public List<Map<String, Object>> getJobStats() {
      return jobStats;
    }

    public void setJobStats(List<Map<String, Object>> jobStats) {
      this.jobStats = jobStats;
    }
  }
}
