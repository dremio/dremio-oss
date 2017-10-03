/*
 * Copyright (C) 2017 Dremio Corporation
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

import javax.inject.Inject;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.glassfish.jersey.server.mvc.Viewable;

import com.dremio.dac.admin.ProfileResource;
import com.dremio.dac.annotations.RestResourceUsedForTesting;
import com.dremio.dac.server.SourceToStoragePluginConfig;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.source.SourceService;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.users.UserService;
import com.google.common.collect.Sets;

/**
 * Test Resource.
 */
@RestResourceUsedForTesting
@Path("/test")
public class TestResource {

  private final KVStoreProvider provider;
  private final SabotContext context;
  private final UserService userService;
  private final StoragePluginRegistry plugins;
  private final SourceToStoragePluginConfig configurator;
  private final InitializerRegistry init;
  private final JobsService jobsService;
  private final CatalogService catalogService;

  @Inject
  public TestResource(InitializerRegistry init, SabotContext context, UserService userService,
                      KVStoreProvider provider, StoragePluginRegistry plugins,
                      SourceToStoragePluginConfig configurator, JobsService jobsService,
                      CatalogService catalogService) {
    this.init = init;
    this.provider = provider;
    this.context = context;
    this.userService = userService;
    this.plugins = plugins;
    this.configurator = configurator;
    this.jobsService = jobsService;
    this.catalogService = catalogService;
  }

  @POST
  @Path("create")
  public void createTestDataset() throws Exception {
    context.getStorage().updateNamespace(Sets.newHashSet("cp"), CatalogService.REFRESH_EVERYTHING_NOW);

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
    return new DatasetVersionMutator(init, provider, nsWithAuth, jobsService);
  }

  private SourceService newSourceService(NamespaceService nsWithAuth, DatasetVersionMutator ds) {
    return new SourceService(plugins, nsWithAuth, configurator, ds, catalogService);
  }

  @POST
  @Path("clear")
  public void clearTestDataset() throws Exception {
    ((LocalKVStoreProvider) provider).deleteEverything();
    context.getStorage().updateNamespace(Sets.newHashSet("__jobResultsStore", "__home", "__accelerator", "__datasetDownload", "$scratch"),
      CatalogService.REFRESH_EVERYTHING_NOW);
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
}
