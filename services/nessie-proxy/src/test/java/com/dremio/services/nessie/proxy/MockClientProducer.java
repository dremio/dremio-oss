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

import javax.enterprise.inject.Produces;
import javax.inject.Singleton;
import org.mockito.Mockito;
import org.projectnessie.client.api.NessieApiV2;

/**
 * this client connects to the externally running nessie server whose REST services we are proxying
 * in the tests
 */
public class MockClientProducer {

  private static NessieApiV2 client = Mockito.mock(NessieApiV2.class);

  @Produces
  @Singleton
  public NessieApiV2 createClient() {
    return client;
  }

  public static NessieApiV2 clientMock() {
    return client;
  }
}
