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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.FieldVector;

import com.dremio.DremioTestWrapper;

/**
 * Wrapper class for a test which uses a FlightClient to execute a query, check expected column metadata,
 * and validate against baseline results. Only unordered comparisons of results are supported currently.
 */
public class FlightQueryTestWrapper {
  private final FlightClientUtils.FlightClientWrapper flightClientWrapper;
  private final List<Map<String, Object>> baselineRecords;
  private final String query;

  public FlightQueryTestWrapper(FlightClientUtils.FlightClientWrapper flightClientWrapper, List<Map<String, Object>> baselineRecords, String query) {
    this.flightClientWrapper = flightClientWrapper;
    this.baselineRecords = baselineRecords;
    this.query = query;
  }

  public void run() throws Exception {
    final FlightClient client = flightClientWrapper.getClient();
    final FlightInfo flightInfo = (DremioFlightService.FLIGHT_LEGACY_AUTH_MODE.equals(flightClientWrapper.getAuthMode()))?
      client.getInfo(FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8))):
      client.getInfo(FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8)), flightClientWrapper.getTokenCallOption());
    final List<Map<String, Object>> flightResults = new ArrayList<>();

    // Assumption: flightInfo only has one endpoint and the location in the
    // flightInfo is the same as the original endpoint.
    try (FlightStream flightStream = (DremioFlightService.FLIGHT_LEGACY_AUTH_MODE.equals(flightClientWrapper.getAuthMode()))?
      client.getStream(flightInfo.getEndpoints().get(0).getTicket()):
      client.getStream(flightInfo.getEndpoints().get(0).getTicket(), flightClientWrapper.getTokenCallOption())) {
      while (flightStream.next()) {
        for (int i = 0; i < flightStream.getRoot().getRowCount(); i++) {
          final Map<String, Object> currentRowMap = new LinkedHashMap<>();
          for (FieldVector vector : flightStream.getRoot().getFieldVectors()) {
            currentRowMap.put(vector.getName(), vector.getObject(i));
          }
          flightResults.add(currentRowMap);
        }
      }
    }

    DremioTestWrapper.compareResults(baselineRecords, flightResults);
  }
}
