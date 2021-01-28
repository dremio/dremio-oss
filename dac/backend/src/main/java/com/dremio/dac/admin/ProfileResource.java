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
package com.dremio.dac.admin;

import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_HTML;

import java.io.IOException;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import org.glassfish.jersey.server.mvc.Viewable;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.annotations.TemporaryAccess;
import com.dremio.dac.resource.NotificationResponse;
import com.dremio.dac.resource.NotificationResponse.ResponseType;
import com.dremio.dac.server.admin.profile.ProfileWrapper;
import com.dremio.dac.service.errors.InvalidReflectionJobException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.serialization.InstanceSerializer;
import com.dremio.exec.serialization.ProtoSerializer;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.CancelReflectionJobRequest;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.ReflectionJobProfileRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobWarningException;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.ReflectionJobValidationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

/**
 * Resource for getting profiles from Dremio.
 */
@Secured
@RolesAllowed({"admin", "user"})
@RestResource
@Path("/profiles")
public class ProfileResource {

  // this is only visible to expose the external profile viewer in the test APIs
  @VisibleForTesting
  public static final InstanceSerializer<QueryProfile> SERIALIZER = ProtoSerializer.of(QueryProfile.class);
  private final JobsService jobsService;
  private final ProjectOptionManager projectOptionManager;
  private final SecurityContext securityContext;

  @Inject
  public ProfileResource(JobsService jobsService, ProjectOptionManager projectOptionManager, SecurityContext securityContext) {
    this.jobsService = jobsService;
    this.projectOptionManager = projectOptionManager;
    this.securityContext = securityContext;
  }

  @GET
  @Path("/cancel/{queryid}")
  @Produces(MediaType.TEXT_PLAIN)
  public NotificationResponse cancelQuery(@PathParam("queryid") String queryId) {
    try {
      final String username = securityContext.getUserPrincipal().getName();
      jobsService.cancel(CancelJobRequest.newBuilder()
          .setUsername(username)
          .setJobId(JobsProtoUtil.toBuf(new JobId(queryId)))
          .setReason(String.format("Query cancelled by user '%s'", username))
          .build());
      return new NotificationResponse(ResponseType.OK, "Job cancellation requested");
    } catch(JobWarningException e) {
      return new NotificationResponse(ResponseType.WARN, e.getMessage());
    } catch(JobException e) {
      return new NotificationResponse(ResponseType.ERROR, e.getMessage());
    }
  }

  @GET
  @Path("/{queryid}.json")
  @Produces(APPLICATION_JSON)
  public String getProfileJSON(@PathParam("queryid") String queryId,
      @QueryParam("attempt") @DefaultValue("0") int attempt) throws IOException {
    final QueryProfile profile;
    try {
      final String username = securityContext.getUserPrincipal().getName();
      QueryProfileRequest request = QueryProfileRequest.newBuilder()
        .setJobId(JobProtobuf.JobId.newBuilder()
          .setId(queryId)
          .build())
        .setAttempt(attempt)
        .setUserName(username)
        .build();
      profile = jobsService.getProfile(request);
    } catch (JobNotFoundException ignored) {
      // TODO: should this be JobResourceNotFoundException?
      throw new NotFoundException(format("Profile for JobId [%s] and Attempt [%d] not found.", queryId, attempt));
    }
    return new String(SERIALIZER.serialize(profile));
  }

  @GET
  @Path("/{queryid}")
  @Produces(TEXT_HTML)
  @TemporaryAccess
  public Viewable getProfile(@PathParam("queryid") String queryId,
      @QueryParam("attempt") @DefaultValue("0") int attempt) {
    final QueryProfile profile;
    try {
      final String username = securityContext.getUserPrincipal().getName();
      QueryProfileRequest request = QueryProfileRequest.newBuilder()
        .setJobId(JobProtobuf.JobId.newBuilder()
          .setId(queryId)
          .build())
        .setAttempt(attempt)
        .setUserName(username)
        .build();
      profile = jobsService.getProfile(request);
    } catch (JobNotFoundException ignored) {
      // TODO: should this be JobResourceNotFoundException?
      throw new NotFoundException(format("Profile for JobId [%s] and Attempt [%d] not found.", queryId, attempt));
    }
    final boolean debug = projectOptionManager.getOption(ExecConstants.DEBUG_QUERY_PROFILE);
    return renderProfile(profile, debug);
  }

  @GET
  @Path("/cancel/{queryid}/reflection/{reflectionId}")
  @Produces(MediaType.TEXT_PLAIN)
  public NotificationResponse cancelReflectionJob(@PathParam("queryid") String queryId,
                                                  @PathParam("reflectionId") String reflectionId) {
    if (Strings.isNullOrEmpty(reflectionId)) {
      throw UserException.validationError()
        .message("reflectionId cannot be null or empty")
        .build();
    }

    try {
      final String username = securityContext.getUserPrincipal().getName();
      CancelJobRequest cancelJobRequest = CancelJobRequest.newBuilder()
        .setUsername(username)
        .setJobId(JobsProtoUtil.toBuf(new JobId(queryId)))
        .setReason(String.format("Query cancelled by user '%s'", username))
        .build();
      CancelReflectionJobRequest request = CancelReflectionJobRequest.newBuilder().setCancelJobRequest(cancelJobRequest)
        .setReflectionId(reflectionId).build();

      jobsService.cancelReflectionJob(request);

      return new NotificationResponse(ResponseType.OK, "Job cancellation requested");
    } catch(JobWarningException e) {
      return new NotificationResponse(ResponseType.WARN, e.getMessage());
    } catch(JobException e) {
      return new NotificationResponse(ResponseType.ERROR, e.getMessage());
    }
  }

  @GET
  @Path("/{queryid}/reflection/{reflectionId}")
  @Produces(TEXT_HTML)
  public Viewable getReflectionJobProfile(@PathParam("queryid") String queryId,
                             @QueryParam("attempt") @DefaultValue("0") int attempt,
                             @PathParam("reflectionId") String reflectionId) {
    if (Strings.isNullOrEmpty(reflectionId)) {
      throw UserException.validationError()
        .message("reflectionId cannot be null or empty")
        .build();
    }

    final QueryProfile profile;
    try {
      final String username = securityContext.getUserPrincipal().getName();
      QueryProfileRequest request = QueryProfileRequest.newBuilder()
        .setJobId(JobProtobuf.JobId.newBuilder()
          .setId(queryId)
          .build())
        .setAttempt(attempt)
        .setUserName(username)
        .build();

      ReflectionJobProfileRequest.Builder builder = ReflectionJobProfileRequest.newBuilder().setQueryProfileRequest(request)
        .setReflectionId(reflectionId);

      profile = jobsService.getReflectionJobProfile(builder.build());
    } catch (JobNotFoundException ignored) {
      // TODO: should this be JobResourceNotFoundException?
      throw new NotFoundException(format("Profile for JobId [%s] and Attempt [%d] not found.", queryId, attempt));
    } catch (ReflectionJobValidationException e) {
      throw new InvalidReflectionJobException(e.getJobId().getId(), e.getReflectionId());
    }
    final boolean debug = projectOptionManager.getOption(ExecConstants.DEBUG_QUERY_PROFILE);
    return renderProfile(profile, debug);
  }

  @GET
  @Path("/reflection/{reflectionId}/{queryid}.json")
  @Produces(APPLICATION_JSON)
  public String getReflectionJobProfileJSON(@PathParam("queryid") String queryId,
                               @QueryParam("attempt") @DefaultValue("0") int attempt,
                               @PathParam("reflectionId") String reflectionId) throws IOException {
    final QueryProfile profile;
    try {
      final String username = securityContext.getUserPrincipal().getName();
      QueryProfileRequest request = QueryProfileRequest.newBuilder()
        .setJobId(JobProtobuf.JobId.newBuilder()
          .setId(queryId)
          .build())
        .setAttempt(attempt)
        .setUserName(username)
        .build();

      ReflectionJobProfileRequest.Builder builder = ReflectionJobProfileRequest.newBuilder().setQueryProfileRequest(request)
        .setReflectionId(reflectionId);

      profile = jobsService.getReflectionJobProfile(builder.build());
    } catch (JobNotFoundException ignored) {
      // TODO: should this be JobResourceNotFoundException?
      throw new NotFoundException(format("Profile for JobId [%s] and Attempt [%d] not found.", queryId, attempt));
    } catch (ReflectionJobValidationException e) {
      throw new InvalidReflectionJobException(e.getJobId().getId(), e.getReflectionId());
    }
    return new String(SERIALIZER.serialize(profile));
  }

  // this is only visible to expose the external profile viewer in the test APIs
  @VisibleForTesting
  public static Viewable renderProfile(QueryProfile profile, boolean includeDebugColumns) {
    if(profile == null){
      throw new BadRequestException("Failed to get query profile.");
    }

    try {
      ProfileWrapper wrapper = new ProfileWrapper(profile, includeDebugColumns);
      return new Viewable("/rest/profile/profile.ftl", wrapper);
    } catch (Exception e){
      throw new BadRequestException("Failed to get query profile.", e);
    }
  }

}
