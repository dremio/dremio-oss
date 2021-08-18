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

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractTestFlightSqlServer extends AbstractTestFlightServer {

  @Override
  public FlightInfo getFlightInfo(String query) throws SQLException {
    final FlightClientUtils.FlightClientWrapper clientWrapper = getFlightClientWrapper();

    final FlightSqlClient.PreparedStatement preparedStatement =
      clientWrapper.getSqlClient().prepare(query, getCallOptions());

    return preparedStatement.execute();
  }

  @Test
  public void testGetCatalogs() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    CallOption[] callOptions = getCallOptions();

    FlightInfo flightInfo = flightSqlClient.getCatalogs(callOptions);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), callOptions)) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertEquals(1, root.getRowCount());

      String catalogName = ((VarCharVector) root.getVector("catalog_name")).getObject(0).toString();
      Assert.assertEquals("DREMIO", catalogName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetTablesWithoutFiltering() throws Exception {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      null, false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), getCallOptions())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 28);
    }
  }

  @Test
  public void testGetTablesFilteringByCatalogPattern() throws Exception {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables("DREMIO", null, null,
      null, false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 28);
    }
  }

  @Test
  public void testGetTablesFilteringBySchemaPattern() throws Exception {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, "INFORMATION_SCHEMA",
      null, null, false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 5);
    }
  }

  @Test
  public void testGetTablesFilteringByTablePattern() throws Exception {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, null, "COLUMNS",
      null, false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 1);
    }
  }

  @Test
  public void testGetTablesFilteringByTableTypePattern() throws Exception {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      Collections.singletonList("SYSTEM_TABLE"), false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 28);
    }
  }
}
