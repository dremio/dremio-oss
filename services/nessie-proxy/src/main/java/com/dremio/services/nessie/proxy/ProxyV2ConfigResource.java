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
package com.dremio.services.nessie.proxy;

import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Path;
import org.projectnessie.api.v2.http.HttpConfigApi;
import org.projectnessie.client.api.GetRepositoryConfigBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.RepositoryConfigResponse;
import org.projectnessie.model.UpdateRepositoryConfigRequest;
import org.projectnessie.model.UpdateRepositoryConfigResponse;
import org.projectnessie.model.ser.Views;

@RequestScoped
@Path("api/v2/config")
public class ProxyV2ConfigResource implements HttpConfigApi {
  private final NessieApiV2 api;

  private static final Set<String> SUPPORTED_TYPES =
      ImmutableSet.of(RepositoryConfig.Type.GARBAGE_COLLECTOR.name());

  @Inject
  public ProxyV2ConfigResource(NessieApiV2 api) {
    this.api = api;
  }

  @Override
  @JsonView(Views.V2.class)
  public NessieConfiguration getConfig() {
    return ImmutableNessieConfiguration.builder().from(api.getConfig()).actualApiVersion(2).build();
  }

  @Override
  public RepositoryConfigResponse getRepositoryConfig(List<String> repositoryConfigTypes) {
    GetRepositoryConfigBuilder request = api.getRepositoryConfig();
    for (String typeName : repositoryConfigTypes) {
      request.type(RepositoryConfigTypeProxy.forName(typeName));
    }
    return request.get();
  }

  @Override
  public UpdateRepositoryConfigResponse updateRepositoryConfig(
      UpdateRepositoryConfigRequest request) throws NessieConflictException {

    return api.updateRepositoryConfig().repositoryConfig(request.getConfig()).update();
  }

  private static final class RepositoryConfigTypeProxy implements RepositoryConfig.Type {
    private final String typeName;

    private RepositoryConfigTypeProxy(String typeName) {
      this.typeName = typeName;
    }

    static RepositoryConfig.Type forName(String name) {
      return new RepositoryConfigTypeProxy(name);
    }

    @Override
    public String name() {
      return typeName;
    }

    @Override
    public Class<? extends RepositoryConfig> type() {
      throw new UnsupportedOperationException(
          "Java implementation class is not available in proxy types");
    }
  }
}
