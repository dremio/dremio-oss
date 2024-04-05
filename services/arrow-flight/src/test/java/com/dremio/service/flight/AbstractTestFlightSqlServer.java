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

import static java.util.Arrays.asList;

import com.dremio.service.flight.FlightClientUtils.FlightClientWrapper;
import java.sql.SQLException;
import java.util.Collection;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Base class for Flight SQL query execution tests. */
@RunWith(Parameterized.class)
public abstract class AbstractTestFlightSqlServer extends AbstractTestFlightServer {

  private final ExecutionMode executionMode;
  private FlightSqlClient flightSqlClient;

  enum ExecutionMode {
    STATEMENT,
    PREPARED_STATEMENT
  }

  @Parameterized.Parameters(name = "ExecutionMode: {0}")
  public static Collection<ExecutionMode> parameters() {
    return asList(ExecutionMode.STATEMENT, ExecutionMode.PREPARED_STATEMENT);
  }

  @Before
  public void setUp() {
    flightSqlClient = getFlightClientWrapper().getSqlClient();
  }

  public AbstractTestFlightSqlServer(ExecutionMode executionMode) {
    this.executionMode = executionMode;
  }

  private FlightInfo executeStatement(String query) {
    return flightSqlClient.execute(query, getCallOptions());
  }

  private FlightInfo executePreparedStatement(String query) throws SQLException {
    final FlightSqlClient.PreparedStatement preparedStatement =
        flightSqlClient.prepare(query, getCallOptions());
    return preparedStatement.execute(getCallOptions());
  }

  @Override
  public FlightInfo getFlightInfo(FlightClientWrapper flightClientWrapper, String query)
      throws SQLException {
    switch (executionMode) {
      case STATEMENT:
        return executeStatement(query);
      case PREPARED_STATEMENT:
        return executePreparedStatement(query);
      default:
        throw new IllegalStateException();
    }
  }
}
