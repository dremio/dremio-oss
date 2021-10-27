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

import static java.lang.String.format;

import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.job.JobProfileVisualizerUI;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobAnalysis.proto.PhaseData;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsService;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Resource for getting Phase Level information from Dremio.
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/queryProfile")
public class JobProfileResource {
  private final SecurityContext securityContext;
  private final JobsService jobsService;
  private final ProjectOptionManager projectOptionManager;

  @Inject
  public JobProfileResource(SecurityContext securityContext, JobsService jobsService, ProjectOptionManager projectOptionManager) {
    this.securityContext = securityContext;
    this.jobsService = jobsService;
    this.projectOptionManager = projectOptionManager;
  }

  @GET
  @Path("/{jobId}/JobProfile")
  @Produces(MediaType.APPLICATION_JSON)
  public List<PhaseData> getJobProfile(@PathParam("jobId") String jobId,
                                       @QueryParam("attempt") @DefaultValue("0") int attempt) throws JsonProcessingException, ClassNotFoundException {
    final UserBitShared.QueryProfile profile;
    JobProfileVisualizerUI jobProfileVisualizerUI;
    try {
      final String username = securityContext.getUserPrincipal().getName();
      QueryProfileRequest request = QueryProfileRequest.newBuilder()
        .setJobId(JobProtobuf.JobId.newBuilder()
          .setId(jobId)
          .build())
        .setAttempt(attempt)
        .setUserName(username)
        .build();
      profile = jobsService.getProfile(request);
    } catch (JobNotFoundException ignored) {
      // TODO: should this be JobResourceNotFoundException?
      throw new NotFoundException(format("Profile for JobId [%s] and Attempt [%d] not found.", jobId, attempt));
    }
    jobProfileVisualizerUI = new JobProfileVisualizerUI();
    return jobProfileVisualizerUI.getPhaseDetail(profile);
  }
}
