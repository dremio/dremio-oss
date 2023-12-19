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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.plugins.dataplane.store.NessiePluginConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;


public class ITDataplanePluginSourceConfig extends ITDataplanePluginTestSetup {

  @Test
  public void testInvalidNessieApiVersionInURLDuringSourceSetup() {
    assertThatThrownBy(() -> createDataplanePluginWithNessieEndpoint(createNessieURIString().replace("v2", "v1")))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Invalid API version");
  }

  @Test
  public void testInvalidProtocolInURLDuringSourceSetup() {
    assertThatThrownBy(() -> createDataplanePluginWithNessieEndpoint("localhost"))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("must be a valid http or https address");
  }

  @Test
  public void testInvalidPortInURLDuringSourceSetup() {
    assertThatThrownBy(() -> createDataplanePluginWithNessieEndpoint("http://localhost"))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Make sure that Nessie endpoint URL is valid");
  }

  @Test
  public void testInvalidHostInURLDuringSourceSetup() {
    assertThatThrownBy(() -> createDataplanePluginWithNessieEndpoint("http://:19120"))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Make sure that Nessie endpoint URL is valid");
  }

  public static void createDataplanePluginWithNessieEndpoint(String nessieEndpoint) {
    NessiePluginConfig nessiePluginConfig = prepareConnectionConf(BUCKET_NAME);
    nessiePluginConfig.nessieEndpoint = nessieEndpoint;

    SourceConfig sourceConfig = new SourceConfig()
      .setConnectionConf(nessiePluginConfig)
      .setName(DATAPLANE_PLUGIN_NAME+"_wrongURL")
      .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);

    CatalogServiceImpl catalogImpl = (CatalogServiceImpl) getSabotContext().getCatalogService();
    catalogImpl.getSystemUserCatalog().createSource(sourceConfig);
  }
}
