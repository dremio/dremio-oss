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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.dremio.config.DremioConfig;

/**
 * Unit tests for DremioFlightService
 */
public class TestDremioFlightService {

  @Test
  public void testDefaultPort() {
    final DremioConfig config = DremioConfig.create();
    assertTrue(config.hasPath(DremioConfig.FLIGHT_SERVICE_PORT_INT));
    assertEquals(32010, config.getInt(DremioConfig.FLIGHT_SERVICE_PORT_INT));
  }

  @Test
  public void testDefaultAuthenticationMode() {
    final DremioConfig config = DremioConfig.create();
    assertTrue(config.hasPath(DremioConfig.FLIGHT_SERVICE_AUTHENTICATION_MODE));
    assertEquals(DremioFlightService.FLIGHT_AUTH2_AUTH_MODE,
      config.getString(DremioConfig.FLIGHT_SERVICE_AUTHENTICATION_MODE));
  }
}
