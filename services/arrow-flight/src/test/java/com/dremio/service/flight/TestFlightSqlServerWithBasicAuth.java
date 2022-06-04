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

import static com.dremio.service.flight.BaseFlightQueryTest.setupBaseFlightQueryTest;

import org.apache.arrow.flight.CallOption;
import org.junit.BeforeClass;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.dremio.service.flight.impl.FlightWorkManager;

/**
 * Test FlightServer with basic authentication using FlightSql producer.
 */
@RunWith(Enclosed.class)
public class TestFlightSqlServerWithBasicAuth {

  private static void setup() throws Exception {
    setupBaseFlightQueryTest(
      false,
      true,
      "flight.endpoint.port",
      FlightWorkManager.RunQueryResponseHandlerFactory.DEFAULT,
      DremioFlightService.FLIGHT_LEGACY_AUTH_MODE);
  }

  /**
   * Query execution tests.
   */
  public static class QueryExecutionTests extends AbstractTestFlightSqlServer {
    public QueryExecutionTests(ExecutionMode executionMode) {
      super(executionMode);
    }

    @BeforeClass
    public static void setup() throws Exception {
      TestFlightSqlServerWithBasicAuth.setup();
    }

    @Override
    protected String getAuthMode() {
      return DremioFlightService.FLIGHT_LEGACY_AUTH_MODE;
    }

    @Override
    CallOption[] getCallOptions() {
      return new CallOption[0];
    }
  }

  /**
   * Catalog methods tests.
   */
  public static class CatalogMethodsTests extends AbstractTestFlightSqlServerCatalogMethods {
    @BeforeClass
    public static void setup() throws Exception {
      TestFlightSqlServerWithBasicAuth.setup();
    }

    @Override
    CallOption[] getCallOptions() {
      return new CallOption[0];
    }
  }
}
