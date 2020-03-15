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

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.jobs.GetJobRequest;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsService;
import com.google.common.base.Preconditions;

/**
 * Jobs API resource
 */
@APIResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/job")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class JobResource {
  private final JobsService jobs;
  private final SecurityContext securityContext;
  private final OptionManager optionManager;

  @Inject
  public JobResource(JobsService jobs, SecurityContext securityContext, SabotContext context) {
    this.jobs = jobs;
    this.securityContext = securityContext;
    this.optionManager = context.getOptionManager();
  }

  @GET
  @Path("/{id}")
  public JobStatus getJobStatus(@PathParam("id") String id) {
    try {
      GetJobRequest request = GetJobRequest.newBuilder()
        .setJobId(new JobId(id))
        .setUserName(securityContext.getUserPrincipal().getName())
        .build();
      Job job = jobs.getJob(request);

      return JobStatus.fromJob(job);
    } catch (JobNotFoundException e) {
      throw new NotFoundException(String.format("Could not find a job with id [%s]", id));
    }
  }

  @GET
  @Path("/{id}/results")
  public JobData getQueryResults(@PathParam("id") String id, @QueryParam("offset") @DefaultValue("0") Integer offset, @Valid @QueryParam("limit") @DefaultValue("100") Integer limit) {
    long queryResultsLimit = optionManager.getOption(ExecConstants.CLIENT_API_JOB_QUERY_RESULT_LIMIT);
    Preconditions.checkArgument(Math.max(limit, 1) <= queryResultsLimit,"limit can not exceed " + queryResultsLimit + " rows");
    Preconditions.checkArgument(offset >= 0,"offset can not be negative");

    try {
      GetJobRequest request = GetJobRequest.newBuilder()
        .setJobId(new JobId(id))
        .setUserName(securityContext.getUserPrincipal().getName())
        .build();
      Job job = jobs.getJob(request);


      if (job.getJobAttempt().getState() != JobState.COMPLETED) {
        throw new BadRequestException(String.format("Can not fetch details for a job that is in [%s] state.", job.getJobAttempt().getState()));
      }

      return new QueryJobResults(job, offset, limit).getData();
    } catch (JobNotFoundException e) {
      throw new NotFoundException(String.format("Could not find a job with id [%s]", id));
    }
  }

  @POST
  @Path("/{id}/cancel")
  public void cancelJob(@PathParam("id") String id) throws JobException {
    final String username = securityContext.getUserPrincipal().getName();

    try {
      jobs.cancel(username, new JobId(id), String.format("Query cancelled by user '%s'", username));
    } catch (JobNotFoundException e) {
      throw new NotFoundException(String.format("Could not find a job with id [%s]", id));
    }
  }
}
