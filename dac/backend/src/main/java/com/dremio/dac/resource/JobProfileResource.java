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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import com.dremio.common.utils.ProtobufUtils;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.job.JobProfileOperatorInfo;
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
                                       @QueryParam("attempt") @DefaultValue("1") int attempt) throws JsonProcessingException, ClassNotFoundException {
    final UserBitShared.QueryProfile profile;
    int attemptIndex = attempt - 1;
    try {
      final String username = securityContext.getUserPrincipal().getName();
      QueryProfileRequest request = QueryProfileRequest.newBuilder()
        .setJobId(JobProtobuf.JobId.newBuilder()
          .setId(jobId)
          .build())
        .setAttempt(attemptIndex)
        .setUserName(username)
        .build();
      profile = jobsService.getProfile(request);
    } catch (JobNotFoundException ignored) {
      // TODO: should this be JobResourceNotFoundException?
      throw new NotFoundException(format("Profile for JobId [%s] and Attempt [%d] not found.", jobId, attemptIndex));
    }
    JobProfileVisualizerUI jobProfileVisualizerUI = new JobProfileVisualizerUI(profile);
    return jobProfileVisualizerUI.getJobProfileInfo();
  }


  @GET

  @Path("/{jobId}/JobProfile/OperatorDetails")

  @Produces(MediaType.APPLICATION_JSON)

  public JobProfileOperatorInfo getJobProfileOperator(@PathParam("jobId") String jobId,
                                                        @QueryParam("phaseId") @NotNull String phaseId,
                                                        @QueryParam("operatorId") @NotNull String  operatorId,
                                                        @QueryParam("attempt") @DefaultValue("1") int attempt) {
    int intPhaseId;
    int intOperatorId;
    int attemptIndex = attempt - 1;
    try {
      intPhaseId = Integer.parseInt(phaseId);
      intOperatorId = Integer.parseInt(operatorId);
    } catch (NumberFormatException ex) {
      throw new NumberFormatException("Please Send Integer Numbers as String for PhaseId and OperatorId");
    }
    final UserBitShared.QueryProfile profile;
    try {
      final String username = securityContext.getUserPrincipal().getName();
      QueryProfileRequest request = QueryProfileRequest.newBuilder()
        .setJobId(JobProtobuf.JobId.newBuilder()
          .setId(jobId)
          .build())
        .setAttempt(attemptIndex)
        .setUserName(username)
        .build();

      profile = jobsService.getProfile(request);
    } catch (JobNotFoundException ignored) {
      // TODO: should this be JobResourceNotFoundException?
      throw new NotFoundException(format("Profile for JobId [%s] and Attempt [%d] not found.", jobId, attemptIndex));
    }

    return new JobProfileOperatorInfo(profile, intPhaseId, intOperatorId);
  }

  @GET
  @Path("/GetJobProfileFromURL")
  @Produces(MediaType.APPLICATION_JSON)
  public List<PhaseData> getJobProfile(@QueryParam("profileJsonFileURL") String profileJsonFileURL) throws IOException, JsonProcessingException, ClassNotFoundException {
    final UserBitShared.QueryProfile profile;
    HttpResponse response = executeRequest(profileJsonFileURL);
    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    String line = "";
    StringBuilder sb = new StringBuilder();
    while ((line = rd.readLine()) != null) {
      sb.append(line + "\n");
    }
    byte[] bytes = sb.toString().getBytes();
    profile = ProtobufUtils.fromJSONString(UserBitShared.QueryProfile.class, new String(bytes));
    JobProfileVisualizerUI jobProfileVisualizerUI = new JobProfileVisualizerUI(profile);
    return jobProfileVisualizerUI.getJobProfileInfo();
  }

  /**
   * This method is to fetch the http response from the url provided
   */
  private HttpResponse executeRequest(String url) throws IOException {
    HttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(url);
    HttpResponse response = client.execute(request);
    return response;
  }
}
