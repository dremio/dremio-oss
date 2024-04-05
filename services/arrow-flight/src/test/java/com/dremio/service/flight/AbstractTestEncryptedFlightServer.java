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
package com.dremio.service.flight;

import static org.junit.Assert.assertNotNull;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.Test;

/** Tests for encryption options with the Flight endpoint service. */
public abstract class AbstractTestEncryptedFlightServer extends BaseFlightQueryTest {
  public abstract String getAuthMode();

  @Test(expected = FlightRuntimeException.class)
  public void testFlightEncryptedClientNoCerts() throws Exception {
    try (final FlightClient ignored = openFlightClient(DUMMY_USER, DUMMY_PASSWORD, getAuthMode())) {
      assertNotNull(ignored);
    }
  }

  @Test
  public void testFlightEncryptedClientWithServerCerts() throws Exception {
    try (final FlightClient ignored =
        openEncryptedFlightClient(
            DUMMY_USER, DUMMY_PASSWORD, getCertificateStream(), getAuthMode())) {
      assertNotNull(ignored);
    }
  }

  @Test(expected = FlightRuntimeException.class)
  public void testFlightClientUnencryptedServerEncrypted() throws Exception {
    try (final FlightClient ignored = openFlightClient(DUMMY_USER, DUMMY_PASSWORD, getAuthMode())) {
      assertNotNull(ignored);
    }
  }
}
