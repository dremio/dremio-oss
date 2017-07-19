/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.reflect.MethodUtils;

/**
 * Creates a RDBMS specific objects.
 */
public final class CompatCreator {

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(CompatCreator.class);

  public static final String DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver";
  public static final String MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  public static final String MSSQL_DATASOURCE = "com.microsoft.sqlserver.jdbc.SQLServerDataSource";
  public static final String MYSQL_DRIVER = "org.mariadb.jdbc.Driver";
  public static final String MYSQL_DATASOURCE = "org.mariadb.jdbc.MariaDbDataSource";
  public static final String ORACLE_DRIVER = "oracle.jdbc.OracleDriver";
  public static final String ORACLE_DATASOURCE = "oracle.jdbc.pool.OracleDataSource";
  public static final String POSTGRES_DRIVER = "org.postgresql.Driver";
  public static final String REDSHIFT_DRIVER = "com.amazon.redshift.jdbc4.Driver";

  // Needed for test framework which uses HSQLDB and Derby
  public static final String HSQLDB_DRIVER = "org.hsqldb.jdbcDriver";
  public static final String DERBY_DRIVER = "org.apache.derby.jdbc.ClientDriver";

  private CompatCreator(){}

  public static DataSource getDataSource(String driver, String url, String username, String password) throws SQLException {

    switch(driver){

    case MYSQL_DRIVER: {
      try {
        final DataSource source = (DataSource) Class.forName(MYSQL_DATASOURCE).newInstance();
        MethodUtils.invokeExactMethod(source, "setURL", url);

        if(username != null) {
          MethodUtils.invokeExactMethod(source, "setUser", username);
        }

        if(password != null) {
          MethodUtils.invokeExactMethod(source, "setPassword", password);
        }
        return source;
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Cannot instantiate MySQL datasource", e);
      }
    }

    case MSSQL_DRIVER: {
      try {
        final DataSource source = (DataSource) Class.forName(MSSQL_DATASOURCE).newInstance();
        MethodUtils.invokeExactMethod(source, "setURL", url);

        if(username != null) {
          MethodUtils.invokeExactMethod(source, "setUser", username);
        }

        if(password != null) {
          MethodUtils.invokeExactMethod(source, "setPassword", password);
        }
        return source;
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Cannot instantiate MSSQL datasource", e);
      }
    }

    case ORACLE_DRIVER: {
      try {
        final DataSource source = (DataSource) Class.forName(ORACLE_DATASOURCE).newInstance();
        MethodUtils.invokeExactMethod(source, "setURL", url);

        if(username != null) {
          MethodUtils.invokeExactMethod(source, "setUser", username);
        }

        if(password != null) {
          MethodUtils.invokeExactMethod(source, "setPassword", password);
        }
        return source;
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Cannot instantiate Oracle datasource", e);
      }
    }

    case POSTGRES_DRIVER:
    case REDSHIFT_DRIVER:
      // they recommend using middleware connection pool, fall through to default impl.
    case DB2_DRIVER:
      // DB2SimpleDataSource source = new DB2SimpleDataSource();
      // TODO: Figure out how to configure db2. For now, fall through to default behavior.
    default: {
      final BasicDataSource source = new BasicDataSource();
      // unbounded number of connections since we could have hundreds of queries running many threads simultaneously.
      source.setMaxActive(Integer.MAX_VALUE);
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


}
