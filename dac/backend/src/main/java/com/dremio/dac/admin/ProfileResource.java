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
package com.dremio.dac.admin;

import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_HTML;

import java.io.IOException;

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

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.resource.NotificationResponse;
import com.dremio.dac.resource.NotificationResponse.ResponseType;
import com.dremio.dac.server.admin.profile.ProfileWrapper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.SchemaUserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.serialization.InstanceSerializer;
import com.dremio.exec.serialization.ProtoSerializer;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobWarningException;
import com.dremio.service.jobs.JobsService;
import com.google.common.annotations.VisibleForTesting;

/**
 * Resource for getting profiles from Dremio.
 */
// TODO DX-3158 - learn how we can re-enable auth and still get regression to work
//@Secured
//@RolesAllowed({"admin", "user"})
@RestResource
@Path("/profiles")
public class ProfileResource {

  // this is only visible to expose the external profile viewer in the test APIs
  @VisibleForTesting
  public static final InstanceSerializer<QueryProfile> SERIALIZER = new ProtoSerializer<>(SchemaUserBitShared.QueryProfile.MERGE, SchemaUserBitShared.QueryProfile.WRITE);
  private final JobsService jobsService;
  private final SabotContext context;
  private final SecurityContext securityContext;

  @Inject
  public ProfileResource(JobsService jobsService, SabotContext context, SecurityContext securityContext) {
    this.jobsService = jobsService;
    this.context = context;
    this.securityContext = securityContext;
  }

  @GET
  @Path("/cancel/{queryid}")
  @Produces(MediaType.TEXT_PLAIN)
  public NotificationResponse cancelQuery(@PathParam("queryid") String queryId) {
    try {
      jobsService.cancel(null, new JobId(queryId), String.format("Query cancelled by user '%s'",
          securityContext.getUserPrincipal().getName()));
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
      profile = jobsService.getProfile(new JobId(queryId), attempt);
    } catch (JobNotFoundException ignored) {
      // TODO: should this be JobResourceNotFoundException?
      throw new NotFoundException(format("Profile for JobId [%s] and Attempt [%d] not found.", queryId, attempt));
    }
    return new String(SERIALIZER.serialize(profile));
  }

  @GET
  @Path("/{queryid}")
  @Produces(TEXT_HTML)
  public Viewable getProfile(@PathParam("queryid") String queryId,
      @QueryParam("attempt") @DefaultValue("0") int attempt) {
    final QueryProfile profile;
    try {
      profile = jobsService.getProfile(new JobId(queryId), attempt);
    } catch (JobNotFoundException ignored) {
      // TODO: should this be JobResourceNotFoundException?
      throw new NotFoundException(format("Profile for JobId [%s] and Attempt [%d] not found.", queryId, attempt));
    }
    final boolean debug = context.getOptionManager().getOption(ExecConstants.DEBUG_QUERY_PROFILE);
    return renderProfile(profile, debug);
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
