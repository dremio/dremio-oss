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

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.resource.BaseResourceWithAllocator;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.errors.InvalidReflectionJobException;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.CancelReflectionJobRequest;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.ReflectionJobDetailsRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.ReflectionJobValidationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


/**
 * Jobs API resource
 */
@APIResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/job")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class JobResource extends BaseResourceWithAllocator {
  private final JobsService jobs;
  private final SecurityContext securityContext;

  @Inject
  public JobResource(JobsService jobs, SecurityContext securityContext, BufferAllocatorFactory allocatorFactory) {
    super(allocatorFactory);
    this.jobs = jobs;
    this.securityContext = securityContext;
  }

  @GET
  @Path("/{id}")
  public JobStatus getJobStatus(@PathParam("id") String id) {
    try {
      JobDetailsRequest request = JobDetailsRequest.newBuilder()
        .setJobId(com.dremio.service.job.proto.JobProtobuf.JobId.newBuilder().setId(id).build())
        .setUserName(securityContext.getUserPrincipal().getName())
        .build();
      JobDetails jobDetails = jobs.getJobDetails(request);

      return JobStatus.fromJob(jobDetails);
    } catch (JobNotFoundException e) {
      throw new NotFoundException(String.format("Could not find a job with id [%s]", id));
    }
  }

  @GET
  @Path("/{id}/results")
  public JobResourceData getQueryResults(@PathParam("id") String id, @QueryParam("offset") @DefaultValue("0") Integer offset, @Valid @QueryParam("limit") @DefaultValue("100") Integer limit) {
    Preconditions.checkArgument(limit <= 500,"limit can not exceed 500 rows");
    try {
      JobSummaryRequest request = JobSummaryRequest.newBuilder()
        .setJobId(JobProtobuf.JobId.newBuilder().setId(id).build())
        .setUserName(securityContext.getUserPrincipal().getName())
        .build();
      JobSummary jobSummary = jobs.getJobSummary(request);

      if (jobSummary.getJobState() != JobState.COMPLETED) {
        throw new BadRequestException(String.format("Can not fetch details for a job that is in [%s] state.", jobSummary.getJobState()));
      }
      // Additional wait not necessary since we check for job completion via JobState
      return new JobResourceData(jobs, jobSummary, securityContext.getUserPrincipal().getName(),
        getOrCreateAllocator("getQueryResults"),  offset, limit);
    } catch (JobNotFoundException e) {
      throw new NotFoundException(String.format("Could not find a job with id [%s]", id));
    }
  }

  @POST
  @Path("/{id}/cancel")
  public void cancelJob(@PathParam("id") String id) throws JobException {
    final String username = securityContext.getUserPrincipal().getName();

    try {
      jobs.cancel(CancelJobRequest.newBuilder()
          .setUsername(username)
          .setJobId(JobsProtoUtil.toBuf(new JobId(id)))
          .setReason(String.format("Query cancelled by user '%s'", username))
          .build());
    } catch (JobNotFoundException e) {
      throw new NotFoundException(String.format("Could not find a job with id [%s]", id));
    }
  }

  @GET
  @Path("/{id}/reflection/{reflectionId}")
  public JobStatus getReflectionJobStatus(@PathParam("id") String id,
                                          @PathParam("reflectionId") String reflectionId) {
    if (Strings.isNullOrEmpty(reflectionId)) {
      throw UserException.validationError()
        .message("reflectionId cannot be null or empty")
        .build();
    }

    try {
      JobDetailsRequest.Builder jobDetailsRequestBuilder = JobDetailsRequest.newBuilder()
        .setJobId(com.dremio.service.job.proto.JobProtobuf.JobId.newBuilder().setId(id).build())
        .setUserName(securityContext.getUserPrincipal().getName());

      ReflectionJobDetailsRequest request = ReflectionJobDetailsRequest.newBuilder()
        .setJobDetailsRequest(jobDetailsRequestBuilder.build())
        .setReflectionId(reflectionId)
        .build();

      JobDetails jobDetails = jobs.getReflectionJobDetails(request);

      return JobStatus.fromJob(jobDetails);
    } catch (JobNotFoundException e) {
      throw new NotFoundException(String.format("Could not find a job with id [%s]", id));
    }  catch (ReflectionJobValidationException e) {
      throw new InvalidReflectionJobException(e.getJobId().getId(), e.getReflectionId());
    }
  }

  @POST
  @Path("/{id}/reflection/{reflectionId}/cancel")
  public void cancelReflectionJob(@PathParam("id") String id,
                        @PathParam("reflectionId") String reflectionId) throws JobException {
    if (Strings.isNullOrEmpty(reflectionId)) {
      throw UserException.validationError()
        .message("reflectionId cannot be null or empty")
        .build();
    }

    final String username = securityContext.getUserPrincipal().getName();

    try {
      CancelJobRequest cancelJobRequest = CancelJobRequest.newBuilder()
        .setUsername(username)
        .setJobId(JobsProtoUtil.toBuf(new JobId(id)))
        .setReason(String.format("Query cancelled by user '%s'", username))
        .build();
      CancelReflectionJobRequest cancelReflectionJobRequest = CancelReflectionJobRequest.newBuilder()
        .setCancelJobRequest(cancelJobRequest)
        .setReflectionId(reflectionId)
        .build();
      jobs.cancelReflectionJob(cancelReflectionJobRequest);
    } catch (JobNotFoundException e) {
      throw new NotFoundException(String.format("Could not find a job with id [%s]", id));
    } catch (ReflectionJobValidationException e) {
      throw new InvalidReflectionJobException(e.getJobId().getId(), e.getReflectionId());
    }
  }
}
