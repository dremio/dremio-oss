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

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ClickHouse storage plugin for Dremio. */
public class ClickHouseStoragePlugin implements StoragePlugin, SupportsListingDatasets {

  private static final Logger logger = LoggerFactory.getLogger(ClickHouseStoragePlugin.class);

  private final ClickHouseStoragePluginConfig config;
  private final PluginSabotContext context;
  private final String name;
  private final ClickHouseConnectionPool connectionPool;
  private final ClickHouseQueryBuilder queryBuilder;

  public ClickHouseStoragePlugin(
      ClickHouseStoragePluginConfig config, PluginSabotContext context, String name) {
    this.config = config;
    this.context = context;
    this.name = name;
    this.connectionPool =
        new ClickHouseConnectionPool(
            config.hostname,
            config.port,
            config.database,
            config.username,
            config.password.get(),
            config.useSsl,
            10 // max connections
            );
    this.queryBuilder = new ClickHouseQueryBuilder(config.database);
  }

  @Override
  public void start() throws IOException {
    boolean connected = connectionPool.testConnection();
    if (!connected) {
      throw new IOException("Failed to connect to ClickHouse");
    }
    logger.info("ClickHouse plugin started successfully for source: {}", name);
  }

  @Override
  public void close() throws IOException {
    connectionPool.close();
    logger.info("ClickHouse plugin closed for source: {}", name);
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public SourceState getState() {
    try {
      if (connectionPool.testConnection()) {
        return SourceState.goodState("Connected to ClickHouse");
      }
      return SourceState.badState("Connection failed");
    } catch (Exception e) {
      return SourceState.badState(e.getMessage());
    }
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return new SourceCapabilities();
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return ClickHouseRulesFactory.class;
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
      throws ConnectorException {
    return () -> {
      try {
        List<ClickHouseConnectionPool.TablePath> tables = connectionPool.getTables();
        List<DatasetHandle> handles = new ArrayList<>();

        for (ClickHouseConnectionPool.TablePath table : tables) {
          EntityPath path =
              new EntityPath(List.of(name, table.getDatabaseName(), table.getTableName()));
          handles.add(
              new ClickHouseDatasetHandle(
                  path, table.getDatabaseName(), table.getTableName(), "TABLE"));
        }

        return handles.iterator();

      } catch (SQLException e) {
        throw new ConnectorException("Failed to list tables", e);
      }
    };
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath path, GetDatasetOption... options)
      throws ConnectorException {
    if (path.getComponents().isEmpty()) {
      return Optional.empty();
    }

    List<String> components = path.getComponents();
    if (components.size() < 3) {
      return Optional.empty();
    }

    String databaseName = components.get(components.size() - 2);
    String tableName = components.get(components.size() - 1);
    try {
      for (ClickHouseConnectionPool.TablePath table : connectionPool.getTables()) {
        if (table.getDatabaseName().equals(databaseName) && table.getTableName().equals(tableName)) {
          return Optional.of(
              new ClickHouseDatasetHandle(path, databaseName, tableName, "TABLE"));
        }
      }
      return Optional.empty();

    } catch (SQLException e) {
      throw new ConnectorException("Failed to get table handle", e);
    }
  }

  @Override
  public PartitionChunkListing listPartitionChunks(
      DatasetHandle datasetHandle, ListPartitionChunkOption... options) throws ConnectorException {
    return () -> Collections.emptyIterator();
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle, PartitionChunkListing chunkListing, GetMetadataOption... options)
      throws ConnectorException {
    if (!(datasetHandle instanceof ClickHouseDatasetHandle)) {
      throw new ConnectorException("Invalid dataset handle type");
    }

    ClickHouseDatasetHandle clickHouseHandle = (ClickHouseDatasetHandle) datasetHandle;

    try {
      List<ClickHouseConnectionPool.ColumnMetaData> columns =
          connectionPool.getColumns(
              clickHouseHandle.getDatabaseName(), clickHouseHandle.getTableName());

      List<Field> fields = new ArrayList<>();
      for (ClickHouseConnectionPool.ColumnMetaData col : columns) {
        // Use nullable field - simplified for compilation
        Field field = Field.nullable(col.getName(), new ArrowType.Utf8());
        fields.add(field);
      }

      final Schema schema = new Schema(fields);

      return new DatasetMetadata() {
        @Override
        public Schema getRecordSchema() {
          return schema;
        }

        @Override
        public DatasetStats getDatasetStats() {
          // Column metadata loading should not fail just because row-count stats are unavailable.
          // Return an unknown record count with a positive generic scan factor.
          return DatasetStats.of(ScanCostFactor.OTHER.getFactor());
        }
      };

    } catch (SQLException e) {
      throw new ConnectorException("Failed to get table metadata", e);
    }
  }

  @Override
  public boolean containerExists(EntityPath containerPath, GetMetadataOption... options) {
    try {
      List<String> components = containerPath.getComponents();
      if (components.isEmpty()) {
        return false;
      }

      if (components.size() == 1) {
        return name.equals(components.get(0));
      }

      if (components.size() == 2) {
        String databaseName = components.get(1);
        for (ClickHouseConnectionPool.TablePath table : connectionPool.getTables()) {
          if (table.getDatabaseName().equals(databaseName)) {
            return true;
          }
        }
        return false;
      }

      String databaseName = components.get(components.size() - 2);
      String tableName = components.get(components.size() - 1);
      for (ClickHouseConnectionPool.TablePath table : connectionPool.getTables()) {
        if (table.getDatabaseName().equals(databaseName) && table.getTableName().equals(tableName)) {
          return true;
        }
      }
      return false;
    } catch (SQLException e) {
      return false;
    }
  }

  /** Execute a query and return results. */
  public ResultSet executeQuery(String sql) throws SQLException {
    Connection conn = connectionPool.getConnection();
    return conn.createStatement().executeQuery(sql);
  }

  /** Get the query builder. */
  public ClickHouseQueryBuilder getQueryBuilder() {
    return queryBuilder;
  }

  /** Dataset handle for ClickHouse tables. */
  public static class ClickHouseDatasetHandle implements DatasetHandle {
    private final EntityPath path;
    private final String databaseName;
    private final String tableName;
    private final String tableType;

    public ClickHouseDatasetHandle(
        EntityPath path, String databaseName, String tableName, String tableType) {
      this.path = path;
      this.databaseName = databaseName;
      this.tableName = tableName;
      this.tableType = tableType;
    }

    @Override
    public EntityPath getDatasetPath() {
      return path;
    }

    public String getTableName() {
      return tableName;
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public String getTableType() {
      return tableType;
    }
  }
}
