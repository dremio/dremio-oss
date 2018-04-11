/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.jobs.Job;
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

  @Inject
  public JobResource(JobsService jobs, SecurityContext securityContext) {
    this.jobs = jobs;
    this.securityContext = securityContext;
  }

  @GET
  @Path("/{id}")
  public JobStatus getJobStatus(@PathParam("id") String id) throws JobNotFoundException {
    Job job = jobs.getJob(new JobId(id), securityContext.getUserPrincipal().getName());

    return JobStatus.fromJob(job);
  }

  @GET
  @Path("/{id}/results")
  public JobData getQueryResults(@PathParam("id") String id, @QueryParam("offset") @DefaultValue("0") Integer offset, @Valid @QueryParam("limit") @DefaultValue("100") Integer limit) throws JobNotFoundException {
    Preconditions.checkArgument(limit < 500,"limit can not exceed 500 rows");
    Job job = jobs.getJob(new JobId(id), securityContext.getUserPrincipal().getName());

    if (job.getJobAttempt().getState() != JobState.COMPLETED) {
      throw new BadRequestException(String.format("Can not fetch details for a job that is in [%s] state.", job.getJobAttempt().getState()));
    }

    return new QueryJobResults(job, offset, limit).getData();
  }
}
