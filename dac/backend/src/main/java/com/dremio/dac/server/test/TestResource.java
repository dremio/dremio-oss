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
package com.dremio.dac.server.test;

import static com.dremio.dac.server.test.SampleDataPopulator.DEFAULT_USER_NAME;
import static com.dremio.exec.proto.UserBitShared.PlanPhaseProfile;
import static com.dremio.service.users.SystemUser.SYSTEM_USER;
import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_HTML;

import com.dremio.dac.admin.ProfileResource;
import com.dremio.dac.annotations.Bootstrap;
import com.dremio.dac.annotations.RestResourceUsedForTesting;
import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.server.DACSecurityContext;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.source.SourceService;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.serialization.InstanceSerializer;
import com.dremio.exec.serialization.ProtoSerializer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.options.OptionManager;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.catalogpubsub.CatalogEventMessagePublisherProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.users.SimpleUserService;
import com.dremio.service.users.UserService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import org.glassfish.jersey.server.mvc.Viewable;

/** Test Resource. */
@RestResourceUsedForTesting
@Path("/test")
public class TestResource {

  private final LegacyKVStoreProvider legacyKVStoreProvider;
  private final KVStoreProvider kvStoreProvider;
  private final SabotContext context;
  private final UserService userService;
  private final SecurityContext security;
  private ConnectionReader connectionReader;
  private final CollaborationHelper collaborationService;
  private final JobsService jobsService;
  private final CatalogService catalogService;
  private final ReflectionServiceHelper reflectionHelper;
  private final OptionManager optionManager;
  private final CatalogEventMessagePublisherProvider catalogEventMessagePublisherProvider;
  private final InstanceSerializer<QueryProfile> serializer;
  @Context private ResourceContext resourceContext;

  @Inject
  public TestResource(
      SabotContext context,
      UserService userService,
      LegacyKVStoreProvider legacyKVStoreProvider,
      KVStoreProvider kvStoreProvider,
      JobsService jobsService,
      CatalogService catalogService,
      ReflectionServiceHelper reflectionHelper,
      SecurityContext security,
      ConnectionReader connectionReader,
      CollaborationHelper collaborationService,
      OptionManager optionManager,
      CatalogEventMessagePublisherProvider catalogEventMessagePublisherProvider) {
    this.legacyKVStoreProvider = legacyKVStoreProvider;
    this.kvStoreProvider = kvStoreProvider;
    this.context = context;
    this.userService = userService;
    this.jobsService = jobsService;
    this.catalogService = catalogService;
    this.reflectionHelper = reflectionHelper;
    this.security = security;
    this.connectionReader = connectionReader;
    this.collaborationService = collaborationService;
    this.optionManager = optionManager;
    this.catalogEventMessagePublisherProvider = catalogEventMessagePublisherProvider;
    this.serializer =
        ProtoSerializer.of(
            QueryProfile.class,
            (int) optionManager.getOption(ExecConstants.QUERY_PROFILE_MAX_FIELD_SIZE));
  }

  @Bootstrap
  @POST
  @Path("create")
  public void createTestDataset() throws Exception {
    setSecurityContext();

    refreshNow("cp");

    // TODO: Clean up this mess
    SampleDataPopulator.addDefaultFirstUser(
        userService,
        new NamespaceServiceImpl(
            kvStoreProvider, new CatalogStatusEventsImpl(), catalogEventMessagePublisherProvider));
    NamespaceService nsWithAuth = context.getNamespaceService(DEFAULT_USER_NAME);
    DatasetVersionMutator ds =
        new DatasetVersionMutator(
            legacyKVStoreProvider, jobsService, catalogService, optionManager, context);
    SourceService sourceService =
        new SourceService(
            Clock.systemUTC(),
            optionManager,
            nsWithAuth,
            ds,
            catalogService,
            reflectionHelper,
            collaborationService,
            connectionReader,
            security);
    // Closing sdp means remove the temporary directory
    @SuppressWarnings("resource")
    SampleDataPopulator sdp =
        new SampleDataPopulator(
            context,
            sourceService,
            ds,
            userService,
            nsWithAuth,
            DEFAULT_USER_NAME,
            collaborationService);
    sdp.populateInitialData();
  }

  public void refreshNow(String... sources) throws NamespaceException {
    for (String source : sources) {
      ((CatalogServiceImpl) catalogService)
          .refreshSource(
              new NamespaceKey(source),
              CatalogService.REFRESH_EVERYTHING_NOW,
              CatalogServiceImpl.UpdateType.FULL);
    }
  }

  @POST
  @Path("clear")
  public void clearTestDataset() throws Exception {
    setSecurityContext();

    if (userService instanceof SimpleUserService) {
      ((SimpleUserService) userService).clearHasAnyUser();
    }

    TestUtilities.clear(catalogService, legacyKVStoreProvider, kvStoreProvider, null, null);
  }

  @GET
  @Path("/render_external_profile")
  @Produces(TEXT_HTML)
  public Viewable submitExternalProfile() {
    return new Viewable("/rest/profile/submitExternalProfile.ftl");
  }

  @POST
  @Path("/render_external_profile")
  @Produces(TEXT_HTML)
  public Viewable renderExternalProfile(@FormParam("profileJsonText") String profileJsonText)
      throws IOException {
    QueryProfile profile = serializer.deserialize(profileJsonText);
    return ProfileResource.renderProfile(profile, true);
  }

  @GET
  @Path("/render_external_profile_file")
  @Produces(TEXT_HTML)
  public Viewable submitExternalProfileFile() {
    return new Viewable("/rest/profile/submitExternalProfileFile.ftl");
  }

  @POST
  @Path("/render_external_profile_file")
  @Produces(TEXT_HTML)
  public Viewable renderExternalProfileFile(
      @FormParam("profileJsonFile") String profileJsonFileText) throws IOException {
    java.nio.file.Path profilePath = Paths.get(profileJsonFileText);
    byte[] data = Files.readAllBytes(profilePath);
    QueryProfile profile = serializer.deserialize(data);
    return ProfileResource.renderProfile(profile, true);
  }

  @GET
  @Path("isSecure")
  public Response isSecure(@Context HttpServletRequest request) throws Exception {
    // this is used for testing is jersey is away that SSL is enabled
    if (request.isSecure()) {
      return Response.ok().build();
    }

    return Response.serverError().build();
  }

  @GET
  @Path("/get_plan/{queryid}")
  public String getExecutorSelectionParams(
      @PathParam("queryid") String queryId,
      @QueryParam("attempt") @DefaultValue("0") int attempt,
      @QueryParam("planphase") String planPhase) {
    final QueryProfile profile;
    try {
      QueryProfileRequest request =
          QueryProfileRequest.newBuilder()
              .setJobId(JobProtobuf.JobId.newBuilder().setId(queryId).build())
              .setAttempt(attempt)
              .setUserName(DEFAULT_USER_NAME)
              .build();
      profile = jobsService.getProfile(request);
    } catch (JobNotFoundException ignored) {
      // TODO: should this be JobResourceNotFoundException?
      throw new NotFoundException(
          format("Profile for JobId [%s] and Attempt [%d] not found.", queryId, attempt));
    }
    for (PlanPhaseProfile planPhaseProfile : profile.getPlanPhasesList()) {
      if (planPhase.equals(planPhaseProfile.getPhaseName())) {
        return planPhaseProfile.getPlan();
      }
    }
    return "Not Found";
  }

  @GET
  @Path("/getDate")
  @Produces(APPLICATION_JSON)
  public LocalDate getDate() {
    return LocalDate.of(2011, 2, 3);
  }

  @GET
  @Path("/getDateTime")
  @Produces(APPLICATION_JSON)
  public LocalDateTime getDateTime() {
    return LocalDateTime.of(2011, 2, 3, 0, 0, 0);
  }

  /** The test REST API runs with no auth, so we explicitly set the SYSTEM user here. */
  private void setSecurityContext() {
    final ContainerRequestContext requestContext =
        resourceContext.getResource(ContainerRequestContext.class);

    requestContext.setSecurityContext(
        new DACSecurityContext(
            new UserName(SYSTEM_USER.getUserName()), SYSTEM_USER, requestContext));
  }
}
