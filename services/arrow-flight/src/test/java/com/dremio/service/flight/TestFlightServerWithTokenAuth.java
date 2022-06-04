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

import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.CallOption;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.service.flight.impl.FlightWorkManager;

/**
 * Test FlightServer with bearer token authentication.
 */
public class TestFlightServerWithTokenAuth extends AbstractTestFlightServer {
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
    executeQueryWithStringResults("USE cp");
    final List<String> results = executeQueryWithStringResults("SELECT * FROM \"10k_rows.parquet\" LIMIT 1");
    assertEquals(Collections.singletonList("val"), results);
  }

  @Override
  CallOption[] getCallOptions() {
    final FlightClientUtils.FlightClientWrapper wrapper = getFlightClientWrapper();
    return new CallOption[] {wrapper.getTokenCallOption()};
  }
}
