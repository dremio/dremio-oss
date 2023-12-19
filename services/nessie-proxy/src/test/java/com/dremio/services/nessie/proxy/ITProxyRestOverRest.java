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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.jaxrs.tests.AbstractRelativeReferences;
import org.projectnessie.jaxrs.tests.BaseTestNessieRest;

/**
 * Verifies the Nessie REST API proxy implementation by running API tests against the proxy
 * configured with a Nessie REST client as the backend.
 */
public class ITProxyRestOverRest extends BaseTestNessieRest {

  @RegisterExtension
  private static NessieProxyJaxRsExtension proxy = new NessieProxyJaxRsExtension(RestClientProducer.class);

  @Override
  protected boolean isNewModel() {
    return true; // nessie-runner-maven-plugin (in pom.xml) uses the IN_MEMORY store type
  }

  @Override
  protected boolean fullPagingSupport() {
    return true;
  }

  @SuppressWarnings("JUnitMalformedDeclaration")
  @BeforeEach
  void checkApiVersion(NessieClientFactory clientFactory) {
    // REST-over-REST proxy use cases are supported only for API v2
    assumeThat(clientFactory.apiVersion()).isEqualTo(NessieApiVersion.V2);
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void testActualApiVersion() {
    assertThat(api().getConfig().getActualApiVersion()).isEqualTo(2);
  }

  /**
   * Workaround to obtain proper test display names in surefire reports.
   */
  @Nested
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public class RelativeReferences extends AbstractRelativeReferences {
    protected RelativeReferences() {
      super(ITProxyRestOverRest.this);
    }
  }
}
