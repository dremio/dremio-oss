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

import java.sql.SQLException;
import java.util.Collections;

import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.apache.arrow.flight.CallOption;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.service.flight.impl.FlightWorkManager;

/**
 * Test FlightServer with basic authentication using FlightSql producer.
 */
public class TestFlightSqlServerWithBasicAuth extends AbstractTestFlightSqlServer {
  @BeforeClass
  public static void setup() throws Exception {
    setupBaseFlightQueryTest(
      false,
      true,
      "flight.endpoint.port",
      FlightWorkManager.RunQueryResponseHandlerFactory.DEFAULT,
      DremioFlightService.FLIGHT_LEGACY_AUTH_MODE);
  }

  @Override
  protected String getAuthMode() {
    return DremioFlightService.FLIGHT_LEGACY_AUTH_MODE;
  }

  @Override
  CallOption[] getCallOptions() {
    return new CallOption[0];
  }

  @Test
  public void testGetTablesWithoutFiltering() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      null, false);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertTrue(root.getRowCount() > 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetTablesFilteringByCatalogPattern() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables("DREMIO", null, null,
      null, false);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertTrue(root.getRowCount() > 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetTablesFilteringBySchemaPattern() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, "INFORMATION_SCHEMA", null,
      null, false);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertTrue(root.getRowCount() > 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetTablesFilteringByTablePattern() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, null, "COLUMNS",
      null, false);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertTrue(root.getRowCount() > 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetTablesFilteringByTableTypePattern() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      Collections.singletonList("system_table"), false);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertTrue(root.getRowCount() > 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
