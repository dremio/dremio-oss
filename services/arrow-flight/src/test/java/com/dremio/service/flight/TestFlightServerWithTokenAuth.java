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

import com.dremio.common.util.TestTools;
import com.dremio.service.flight.FlightClientUtils.FlightClientWrapper;
import com.dremio.service.flight.impl.FlightWorkManager;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.HeaderCallOption;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/** Test FlightServer with bearer token authentication. */
@Ignore("DX-91148")
public class TestFlightServerWithTokenAuth extends AbstractTestFlightServer {

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(180, TimeUnit.SECONDS);

  @BeforeClass
  public static void setup() throws Exception {
    setupBaseFlightQueryTest(
        false,
        true,
        "flight.endpoint.port",
        FlightWorkManager.RunQueryResponseHandlerFactory.DEFAULT);
  }

  @Override
  protected String getAuthMode() {
    return DremioFlightService.FLIGHT_AUTH2_AUTH_MODE;
  }

  @Test
  public void testSelectAfterUse() throws Exception {
    executeQueryWithStringResults(getFlightClientWrapper(), "USE cp");
    final List<String> results =
        executeQueryWithStringResults(
            getFlightClientWrapper(), "SELECT * FROM \"10k_rows.parquet\" LIMIT 1");
    assertEquals(Collections.singletonList("val"), results);
  }

  @Test
  public void testKeepSchemaParameter() throws Exception {
    // Simulate ";schema=cp" like in a JDBC request
    final FlightCallHeaders flightCallHeaders = new FlightCallHeaders();
    flightCallHeaders.insert("schema", "cp");
    final CallOption headerCallOption = new HeaderCallOption(flightCallHeaders);

    try (final FlightClientWrapper wrapper =
        openFlightClientWrapperWithOptions(
            DUMMY_USER,
            DUMMY_PASSWORD,
            getAuthMode(),
            Collections.singletonList(headerCallOption))) {
      // Use Flight SQL client to simulate a JDBC DatabaseMetadata call before statements
      wrapper.getSqlClient().getTables(null, null, null, null, true, wrapper.getOptions());

      // Execute statement
      final List<String> results =
          executeQueryWithStringResults(wrapper, "SELECT * FROM \"10k_rows.parquet\" LIMIT 1");

      // Assert
      assertEquals(Collections.singletonList("val"), results);
    }
  }

  @Override
  CallOption[] getCallOptions() {
    final FlightClientWrapper wrapper = getFlightClientWrapper();
    return new CallOption[] {wrapper.getTokenCallOption()};
  }
}
