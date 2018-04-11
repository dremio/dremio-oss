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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.datasources.SharedPoolDataSource;
import org.apache.commons.lang3.reflect.MethodUtils;

/**
 * Creates a RDBMS specific objects.
 */
public final class CompatCreator {

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(CompatCreator.class);

  public static final String DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver";
  public static final String MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  public static final String MSSQL_POOLED_DATASOURCE = "com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource";
  public static final String MYSQL_DRIVER = "org.mariadb.jdbc.Driver";
  public static final String MYSQL_POOLED_DATASOURCE = "org.mariadb.jdbc.MariaDbDataSource";
  public static final String ORACLE_DRIVER = "oracle.jdbc.OracleDriver";
  public static final String ORACLE_POOLED_DATASOURCE = "oracle.jdbc.pool.OracleConnectionPoolDataSource";
  public static final String POSTGRES_DRIVER = "org.postgresql.Driver";
  public static final String POSTGRES_POOLED_DATASOURCE = "org.postgresql.ds.PGConnectionPoolDataSource";
  public static final String REDSHIFT_DRIVER = "com.amazon.redshift.jdbc4.Driver";

  // Needed for test framework which uses HSQLDB and Derby
  public static final String HSQLDB_DRIVER = "org.hsqldb.jdbcDriver";
  public static final String DERBY_DRIVER = "org.apache.derby.jdbc.ClientDriver";
  public static final String EMBEDDED_DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";


  // Needed for handling upgrades of Oracle data sources before 1.2. These versions don't save the username and
  // password independently of the connection URL. We need to extract the username and password.
  private static final Pattern ORACLE_URL_USER_PASSWORD_FINDER = Pattern.compile("^jdbc:oracle:thin:(?<User>.+)/(?<Password>.+)@.+");

  private CompatCreator(){}

  static {
    // If multiple JDBC sources tries to get a datasource at the same time, it might cause a deadlock situation
    // as one driver might try to register itself with DriverManager, causing another driver to load (from
    // ServiceLoader) and trying to also register itself...
    // Forcing DriverManager to initialize first in a single thread context to prevent this deadlock situation.
    // See DBCP-272 and DX-8683 for more details.
    DriverManager.getDrivers();
  }
  public static DataSource getDataSource(String driver, String url, String username, String password) throws SQLException {

    switch(driver){

    case MYSQL_DRIVER: {
      try {
        final ConnectionPoolDataSource source = (ConnectionPoolDataSource) Class.forName(MYSQL_POOLED_DATASOURCE).newInstance();
        MethodUtils.invokeExactMethod(source, "setURL", url);

        if(username != null) {
          MethodUtils.invokeExactMethod(source, "setUser", username);
        }

        if(password != null) {
          MethodUtils.invokeExactMethod(source, "setPassword", password);
        }

        return newSharedDataSource(source);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Cannot instantiate MySQL datasource", e);
      }
    }

    case MSSQL_DRIVER: {
      try {
        final ConnectionPoolDataSource source = (ConnectionPoolDataSource) Class.forName(MSSQL_POOLED_DATASOURCE).newInstance();
        MethodUtils.invokeExactMethod(source, "setURL", url);

        if(username != null) {
          MethodUtils.invokeExactMethod(source, "setUser", username);
        }

        if(password != null) {
          MethodUtils.invokeExactMethod(source, "setPassword", password);
        }

        return newSharedDataSource(source);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Cannot instantiate MSSQL datasource", e);
      }
    }

    case ORACLE_DRIVER: {
      try {
        final ConnectionPoolDataSource source = (ConnectionPoolDataSource) Class.forName(ORACLE_POOLED_DATASOURCE).newInstance();
        MethodUtils.invokeExactMethod(source, "setURL", url);

        // Determine username and password from the connection URL. With versions of Dremio earlier than 1.2, we saved
        // only the connection URL which have the username and password embedded. After 1.2, we started saving username
        // and password because we switched to using OracleConnectionPoolDataSource, which has bugs if user/password
        // aren't set explicitly. This check is required when upgrading a source from before 1.2 to 1.2+.
        if (username == null || password == null) {
          final Matcher urlMatcher = ORACLE_URL_USER_PASSWORD_FINDER.matcher(url);

          if (urlMatcher.matches()) {
            username = urlMatcher.group("User");
            password = urlMatcher.group("Password");
          }
        }

        MethodUtils.invokeExactMethod(source, "setUser", username);
        MethodUtils.invokeExactMethod(source, "setPassword", password);

        return newSharedDataSource(source);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Cannot instantiate Oracle datasource", e);
      }
    }

    case POSTGRES_DRIVER:
      // We cannot use the PGConnectionPoolDataSource in this driver while having the Redshift
      // driver in the same classpath. The call to SetUrl() parses the URL and discards unrecognized
      // properties, including the OpenSourceSubProtocolOverride property. Internally when you call
      // getPooledConnection() with this class, it'll pass its generated URL to the DriverManager, which
      // leads to the Redshift driver getting loaded.
    case REDSHIFT_DRIVER:
      // they recommend using middleware connection pool, fall through to default impl.
    case DB2_DRIVER:
      // TODO: Figure out how to configure db2. For now, fall through to default behavior.
      // DB2 has a DB2ConnectionPoolDataSource which implements ConnectionPoolDataSource, but this class does not
      // have a documented setURL() function, so we can't use it without supplying connection information differently.
    default: {
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
    }
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
