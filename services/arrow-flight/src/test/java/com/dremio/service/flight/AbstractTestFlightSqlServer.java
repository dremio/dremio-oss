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
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.Collections;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.expr.fn.impl.RegexpUtil;
import com.dremio.exec.planner.sql.handlers.commands.MetadataProviderConditions;
import com.dremio.exec.proto.UserProtos;

import com.google.common.collect.ImmutableList;

public abstract class AbstractTestFlightSqlServer extends AbstractTestFlightServer {

  @Override
  public FlightInfo getFlightInfo(String query) throws SQLException {
    final FlightClientUtils.FlightClientWrapper clientWrapper = getFlightClientWrapper();

    final FlightSqlClient.PreparedStatement preparedStatement =
      clientWrapper.getSqlClient().prepare(query, getCallOptions());

    return preparedStatement.execute(getCallOptions());
  }

  @Test
  public void testGetCatalogs() throws Exception {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    CallOption[] callOptions = getCallOptions();

    FlightInfo flightInfo = flightSqlClient.getCatalogs(callOptions);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), callOptions)) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertEquals(1, root.getRowCount());

      String catalogName = ((VarCharVector) root.getVector("catalog_name")).getObject(0).toString();
      Assert.assertEquals("DREMIO", catalogName);
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

  @Test
  public void testGetTablesFilteringByMultiTableTypes() throws Exception {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      ImmutableList.of("TABLE", "VIEW"), false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 0);
    }
  }

  private void testGetSchemas(String catalog, String schemaPattern, boolean expectNonEmptyResult) throws Exception {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getSchemas(catalog, schemaPattern, getCallOptions());
    try (
      FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), getCallOptions())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();

      Predicate<String> catalogNamePredicate = MetadataProviderConditions.getCatalogNamePredicate(
        catalog != null ? UserProtos.LikeFilter.newBuilder().setPattern(catalog).build() : null);
      Pattern schemaFilterPattern = schemaPattern != null ? Pattern.compile(RegexpUtil.sqlToRegexLike(schemaPattern)) :
        Pattern.compile(".*");

      VarCharVector catalogNameVector = (VarCharVector) root.getVector("catalog_name");
      VarCharVector schemaNameVector = (VarCharVector) root.getVector("schema_name");

      for (int i = 0; i < root.getRowCount(); i++) {
        String catalogName = catalogNameVector.getObject(i).toString();
        String schemaName = schemaNameVector.getObject(i).toString();

        Assert.assertTrue(catalogNamePredicate.test(catalogName));
        Assert.assertTrue(schemaFilterPattern.matcher(schemaName).matches());
      }

      if (expectNonEmptyResult) {
        Assert.assertTrue(root.getRowCount() > 0);
      }
    }
  }

  @Test
  public void testGetSchemasWithNoFilter() throws Exception {
    testGetSchemas(null, null, true);
  }

  @Test
  public void testGetSchemasWithBothFilters() throws Exception {
    testGetSchemas("DREMIO", "INFORMATION_SCHEMA", true);
  }

  @Test
  public void testGetSchemasWithCatalog() throws Exception {
    testGetSchemas("DREMIO", null, true);
  }

  @Test
  public void testGetSchemasWithSchemaFilterPattern() throws Exception {
    testGetSchemas(null, "sys", true);
  }

  @Test
  public void testGetSchemasWithNonMatchingSchemaFilter() throws Exception {
    testGetSchemas(null, "NON_EXISTING_SCHEMA", false);
  }

  @Test
  public void testGetSchemasWithNonMatchingCatalog() throws Exception {
    testGetSchemas("NON_EXISTING_CATALOG", null, false);
  }
}
