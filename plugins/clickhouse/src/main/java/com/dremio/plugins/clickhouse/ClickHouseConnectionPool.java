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
package com.dremio.plugins.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Connection pool for ClickHouse JDBC connections. */
public class ClickHouseConnectionPool {

  private static final Logger logger = LoggerFactory.getLogger(ClickHouseConnectionPool.class);
  private static final String[] DRIVER_CLASS_NAMES = {
    "com.dremio.clickhouse.clickhouse.jdbc.ClickHouseDriver",
    "com.clickhouse.jdbc.ClickHouseDriver"
  };

  private final String jdbcUrl;
  private final String database;
  private final String username;
  private final String password;
  private final int maxConnections;
  private final ConcurrentHashMap<String, ConnectionWrapper> connections;
  private final AtomicInteger connectionCounter;

  public ClickHouseConnectionPool(
      String host,
      int port,
      String database,
      String username,
      String password,
      boolean useSsl,
      int maxConnections) {
    this.jdbcUrl = String.format("jdbc:clickhouse://%s:%d/%s", host, port, database);
    this.database = database;
    this.username = username;
    this.password = password;
    this.maxConnections = maxConnections;
    this.connections = new ConcurrentHashMap<>();
    this.connectionCounter = new AtomicInteger(0);

    loadDriver();
  }

  public Connection getConnection() throws SQLException {
    // Simple connection creation (in production, implement proper pooling)
    Properties props = new Properties();
    props.setProperty("user", username);
    props.setProperty("password", password);
    props.setProperty("connect_timeout", "30000");
    props.setProperty("socket_timeout", "60000");
    props.setProperty("compress", "0");

    return DriverManager.getConnection(jdbcUrl, props);
  }

  public void closeConnection(Connection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        logger.error("Error closing connection", e);
      }
    }
  }

  /** Test connection to ClickHouse. */
  public boolean testConnection() {
    Connection conn = null;
    try {
      conn = getConnection();
      return conn != null && !conn.isClosed();
    } catch (SQLException e) {
      logger.error("Connection test failed: {}", e.getMessage());
      return false;
    } finally {
      closeConnection(conn);
    }
  }

  /** Get list of tables from ClickHouse. */
  public List<TablePath> getTables() throws SQLException {
    List<TablePath> tables = new ArrayList<>();
    Connection conn = null;
    try {
      conn = getConnection();
      List<String> databases = getDatabases(conn);
      logger.info(
          "Discovering ClickHouse tables via jdbcUrl={}, configuredDatabase={}, databases={}",
          jdbcUrl,
          database,
          databases);
      for (String databaseName : databases) {
        String sql = "SHOW TABLES FROM " + quoteIdentifier(databaseName);
        logger.info("Executing ClickHouse metadata query: {}", sql);
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
          while (rs.next()) {
            tables.add(new TablePath(databaseName, rs.getString(1)));
          }
        } catch (SQLException e) {
          logger.error(
              "Failed ClickHouse metadata query for database {} using sql {}: {}",
              databaseName,
              sql,
              e.getMessage(),
              e);
          throw e;
        }
      }
    } finally {
      closeConnection(conn);
    }
    logger.info("Discovered {} ClickHouse tables for source database setting {}", tables.size(), database);
    return tables;
  }

  /** Get column metadata for a table. */
  public List<ColumnMetaData> getColumns(String databaseName, String tableName) throws SQLException {
    List<ColumnMetaData> columns = new ArrayList<>();
    Connection conn = null;
    try {
      conn = getConnection();
      String sql =
          "DESCRIBE TABLE " + quoteIdentifier(databaseName) + "." + quoteIdentifier(tableName);
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String typeName = rs.getString("type");
          columns.add(
              new ColumnMetaData(rs.getString("name"), typeName, toSqlType(typeName), 0));
        }
      }
    } finally {
      closeConnection(conn);
    }
    return columns;
  }

  private List<String> getDatabases(Connection conn) throws SQLException {
    if (!shouldDiscoverAllDatabases()) {
      logger.info("Using configured ClickHouse database only: {}", database);
      return List.of(database);
    }

    List<String> databases = new ArrayList<>();
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SHOW DATABASES")) {
      while (rs.next()) {
        String databaseName = rs.getString(1);
        if (!isSystemDatabase(databaseName)) {
          databases.add(databaseName);
        }
      }
    } catch (SQLException e) {
      logger.error("Failed ClickHouse database discovery query SHOW DATABASES: {}", e.getMessage(), e);
      throw e;
    }
    return databases;
  }

  private boolean shouldDiscoverAllDatabases() {
    return database == null || database.isBlank() || "default".equalsIgnoreCase(database);
  }

  private boolean isSystemDatabase(String databaseName) {
    return "system".equalsIgnoreCase(databaseName)
        || "information_schema".equalsIgnoreCase(databaseName);
  }

  private void loadDriver() {
    for (String driverClassName : DRIVER_CLASS_NAMES) {
      try {
        Class.forName(driverClassName);
        logger.info("Loaded ClickHouse JDBC driver {}", driverClassName);
        return;
      } catch (ClassNotFoundException e) {
        logger.debug("ClickHouse JDBC driver class not available: {}", driverClassName);
      }
    }

    logger.error(
        "ClickHouse JDBC driver not found. Tried driver classes {}",
        List.of(DRIVER_CLASS_NAMES));
  }

  private String quoteIdentifier(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  private int toSqlType(String typeName) {
    if (typeName == null) {
      return Types.VARCHAR;
    }

    String normalized = typeName.toUpperCase();
    if (normalized.startsWith("INT") || normalized.startsWith("UINT")) {
      return Types.BIGINT;
    } else if (normalized.startsWith("FLOAT")) {
      return Types.FLOAT;
    } else if (normalized.startsWith("DECIMAL")) {
      return Types.DECIMAL;
    } else if (normalized.startsWith("DATE")) {
      return Types.TIMESTAMP;
    } else if (normalized.startsWith("BOOL")) {
      return Types.BOOLEAN;
    }

    return Types.VARCHAR;
  }

  /** Close all connections. */
  public void close() {
    for (ConnectionWrapper wrapper : connections.values()) {
      closeConnection(wrapper.connection);
    }
    connections.clear();
  }

  /** Wrapper for connection with metadata. */
  private static class ConnectionWrapper {
    final Connection connection;
    final long createdAt;
    final String threadName;

    ConnectionWrapper(Connection connection) {
      this.connection = connection;
      this.createdAt = System.currentTimeMillis();
      this.threadName = Thread.currentThread().getName();
    }
  }

  /** Column metadata holder. */
  public static class ColumnMetaData {
    private final String name;
    private final String typeName;
    private final int dataType;
    private final int columnSize;

    public ColumnMetaData(String name, String typeName, int dataType, int columnSize) {
      this.name = name;
      this.typeName = typeName;
      this.dataType = dataType;
      this.columnSize = columnSize;
    }

    public String getName() {
      return name;
    }

    public String getTypeName() {
      return typeName;
    }

    public int getDataType() {
      return dataType;
    }

    public int getColumnSize() {
      return columnSize;
    }
  }

  /** Table path holder. */
  public static class TablePath {
    private final String databaseName;
    private final String tableName;

    public TablePath(String databaseName, String tableName) {
      this.databaseName = databaseName;
      this.tableName = tableName;
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public String getTableName() {
      return tableName;
    }
  }
}
