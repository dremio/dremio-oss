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
package com.dremio.dac.server.test;

import static com.dremio.dac.server.test.SampleDataPopulator.DEFAULT_USER_NAME;
import static javax.ws.rs.core.MediaType.TEXT_HTML;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.glassfish.jersey.server.mvc.Viewable;

import com.dremio.dac.admin.ProfileResource;
import com.dremio.dac.annotations.Bootstrap;
import com.dremio.dac.annotations.RestResourceUsedForTesting;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.source.SourceService;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.users.UserService;

/**
 * Test Resource.
 */
@RestResourceUsedForTesting
@Path("/test")
public class TestResource {

  private final KVStoreProvider provider;
  private final SabotContext context;
  private final UserService userService;
  private final InitializerRegistry init;
  private final SecurityContext security;
  private ConnectionReader connectionReader;
  private final CollaborationHelper collaborationService;
  private final JobsService jobsService;
  private final CatalogService catalogService;
  private final ReflectionServiceHelper reflectionHelper;

  @Inject
  public TestResource(InitializerRegistry init, SabotContext context, UserService userService,
                      KVStoreProvider provider, JobsService jobsService,
                      CatalogService catalogService, ReflectionServiceHelper reflectionHelper,
                      SecurityContext security, ConnectionReader connectionReader, CollaborationHelper collaborationService) {
    this.init = init;
    this.provider = provider;
    this.context = context;
    this.userService = userService;
    this.jobsService = jobsService;
    this.catalogService = catalogService;
    this.reflectionHelper = reflectionHelper;
    this.security = security;
    this.connectionReader = connectionReader;
    this.collaborationService = collaborationService;
  }

  @Bootstrap
  @POST
  @Path("create")
  public void createTestDataset() throws Exception {
    refreshNow("cp");

    // TODO: Clean up this mess
    SampleDataPopulator.addDefaultFirstUser(userService, new NamespaceServiceImpl(provider));
    NamespaceService nsWithAuth = context.getNamespaceService(DEFAULT_USER_NAME);
    DatasetVersionMutator ds = newDS(nsWithAuth);
    // Closing sdp means remove the temporary directory
    @SuppressWarnings("resource")
    SampleDataPopulator sdp = new SampleDataPopulator(context, newSourceService(nsWithAuth, ds), ds,
        userService, nsWithAuth, DEFAULT_USER_NAME);
    sdp.populateInitialData();
  }

  private DatasetVersionMutator newDS(NamespaceService nsWithAuth) {
    return new DatasetVersionMutator(init, provider, nsWithAuth, jobsService, catalogService);
  }

  private SourceService newSourceService(NamespaceService nsWithAuth, DatasetVersionMutator ds) {
    return new SourceService(nsWithAuth, ds, catalogService, reflectionHelper, collaborationService, connectionReader, security);
  }

  public void refreshNow(String...sources) throws NamespaceException {
    for(String source : sources) {
      context.getCatalogService().refreshSource(new NamespaceKey(source), CatalogService.REFRESH_EVERYTHING_NOW, CatalogService.UpdateType.FULL);
    }
  }

  @POST
  @Path("clear")
  public void clearTestDataset() throws Exception {
    TestUtilities.clear(catalogService, provider, null, null);
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
  public Viewable renderExternalProfile(@FormParam("profileJsonText") String profileJsonText) throws IOException {
    QueryProfile profile = ProfileResource.SERIALIZER.deserialize(profileJsonText.getBytes());
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
  public Viewable renderExternalProfileFile(@FormParam("profileJsonFile") String profileJsonFileText) throws IOException {
    java.nio.file.Path profilePath = Paths.get(profileJsonFileText);
    byte[] data = Files.readAllBytes(profilePath);
    QueryProfile profile = ProfileResource.SERIALIZER.deserialize(data);
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
}
