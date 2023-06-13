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

import static org.projectnessie.client.NessieConfigConstants.CONF_FORCE_URL_CONNECTION_CLIENT;

import java.util.Collections;

import javax.enterprise.inject.Produces;
import javax.inject.Singleton;

import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientBuilder;

public class RestClientProducer {

  @Produces
  @Singleton
  public NessieApiV2 createClient() {
    return HttpClientBuilder.builder()
      .fromConfig(Collections.singletonMap(CONF_FORCE_URL_CONNECTION_CLIENT, "true")::get)
      .withUri(createNessieURIString())
      .build(NessieApiV2.class);
  }

  private static String createNessieURIString() {
    return System.getProperty("nessie.server.url") +"/api/v2";
  }
}
