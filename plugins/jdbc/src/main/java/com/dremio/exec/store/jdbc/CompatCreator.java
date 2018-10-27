/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.datasources.SharedPoolDataSource;

import com.google.common.base.Preconditions;

/**
 * Creates a RDBMS specific objects.
 */
public final class CompatCreator {
  private CompatCreator(){}

  static {
    // If multiple JDBC sources tries to get a datasource at the same time, it might cause a deadlock situation
    // as one driver might try to register itself with DriverManager, causing another driver to load (from
    // ServiceLoader) and trying to also register itself...
    // Forcing DriverManager to initialize first in a single thread context to prevent this deadlock situation.
    // See DBCP-272 and DX-8683 for more details.
    DriverManager.getDrivers();
  }
  public static DataSource getDataSource(ConnectionPoolDataSource cpds, String driver, String url, String username, String password) throws SQLException {

    Preconditions.checkNotNull(driver);

    if (cpds != null) {
      // Configuration supplied a driver-specific CPDS.
      return newSharedDataSource(cpds);
    }

    // Use the ConnectionPoolDataSource supplied by Apache DBCP.
    final BasicDataSource source = new BasicDataSource();
    // unbounded number of connections since we could have hundreds of queries running many threads simultaneously.
    source.setMaxTotal(Integer.MAX_VALUE);
    source.setTestOnBorrow(true);
    source.setValidationQueryTimeout(1);
    source.setDriverClassName(driver);
    source.setUrl(url);

    if (username != null) {
      source.setUsername(username);
    }

    if (password != null) {
      source.setPassword(password);
    }
    return source;
  }

  private static SharedPoolDataSource newSharedDataSource(final ConnectionPoolDataSource source) {
    SharedPoolDataSource ds = new SharedPoolDataSource();
    ds.setConnectionPoolDataSource(source);
    ds.setMaxTotal(Integer.MAX_VALUE);
    ds.setDefaultTestOnBorrow(true);
    ds.setValidationQueryTimeout(1);
    return ds;
  }


}
