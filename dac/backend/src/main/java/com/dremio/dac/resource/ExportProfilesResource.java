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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.server.admin.profile.ProfilesExporter;
import com.dremio.datastore.api.LegacyKVStoreProvider;

/**
 * Export profiles resource
 */
@RestResource
@Secured
@RolesAllowed({"admin"})
@Path("/export-profiles")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class ExportProfilesResource {

  private final Provider<LegacyKVStoreProvider> kvStoreProviderProvider;

  @Inject
  public ExportProfilesResource(Provider<LegacyKVStoreProvider> kvStoreProviderProvider) {
    this.kvStoreProviderProvider = kvStoreProviderProvider;
  }

  @POST
  public ExportProfilesStats exportProfiles(ExportProfilesParams exportParams)
    throws Exception {
    final LegacyKVStoreProvider kvStoreProvider = kvStoreProviderProvider.get();
    ProfilesExporter exporter = getExporter(exportParams);

    return exporter.export(kvStoreProvider);
  }

  public static ProfilesExporter getExporter(ExportProfilesParams exportParams) {
    return new ProfilesExporter(exportParams);
  }
}
