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
package com.dremio.exec.sql;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergAutoClustering extends BaseTestQuery {

  // **************************************************************************
  //  Test Cases for CREATE TABLE
  // **************************************************************************

  @Test
  public void createTableWithClusterKeysAndSortOrder() throws Exception {
    final String newTblName = "createTableWithClusterKeysAndSortOrder";
    final String ctasQuery =
        String.format(
            "CREATE TABLE %s.%s (id int, name varchar) LOCALSORT BY (id) CLUSTER BY (name) ",
            TEMP_SCHEMA, newTblName);
    validateErrorRunWithOtherOption(newTblName, ctasQuery);
  }

  @Test
  public void createTableWithClusterKeysAndSortOrderAndPartition() throws Exception {
    final String newTblName = "createTableWithClusterKeysAndSortOrderAndPartition";
    final String ctasQuery =
        String.format(
            "CREATE TABLE %s.%s (id int, name varchar) PARTITION BY (id) LOCALSORT BY (id) CLUSTER BY (name)",
            TEMP_SCHEMA, newTblName);
    validateErrorRunWithOtherOption(newTblName, ctasQuery);
  }

  @Test
  public void createTableWithClusterKeysAndPartition() throws Exception {
    final String newTblName = "createTableWithClusterKeysAndPartition";
    final String ctasQuery =
        String.format(
            "CREATE TABLE %s.%s (id int, name varchar) PARTITION BY (id) CLUSTER BY (name) ",
            TEMP_SCHEMA, newTblName);
    validateErrorRunWithOtherOption(newTblName, ctasQuery);
  }

  @Test
  public void createTableWithClusterKeys() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "createTableWithClusterKeys";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar) CLUSTER BY (name)",
              TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets set
        validateSortColumnsFromMetadataJson(metadataJsonString, true);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets set
        final String showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
        validateTableProperty(showTablePropertiesQuery, true);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void createTableWithMultiClusterKeys() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "createTableWithMultiClusterKeys";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar) CLUSTER BY (name, id)",
              TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets set
        validateSortColumnsFromMetadataJson(metadataJsonString, true);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets set
        final String showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
        validateTableProperty(showTablePropertiesQuery, true);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void ctasWithClusterKeys() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "ctasWithClusterKeys";
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  CLUSTER BY (o_orderkey) "
                  + " AS SELECT * from dfs.\""
                  + parquetFiles
                  + "\" limit 1",
              TEMP_SCHEMA_HADOOP,
              newTblName);

      try {
        runSQL(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets set
        validateSortColumnsFromMetadataJson(metadataJsonString, true);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets set
        final String showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA_HADOOP, newTblName);
        validateTableProperty(showTablePropertiesQuery, true);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void ctasWithMultiClusterKeys() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "ctasWithMultiClusterKeys";
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  CLUSTER BY (o_orderkey) "
                  + " AS SELECT * from dfs.\""
                  + parquetFiles
                  + "\" limit 1",
              TEMP_SCHEMA_HADOOP,
              newTblName);

      try {
        runSQL(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets set
        validateSortColumnsFromMetadataJson(metadataJsonString, true);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets set
        final String showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA_HADOOP, newTblName);
        validateTableProperty(showTablePropertiesQuery, true);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  // **************************************************************************
  //  Test Cases for ALTER TABLE
  // **************************************************************************

  @Test
  public void alterClusteringKeyOnPartitionedTable() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "alterClusteringKeyOnPartitionedTable";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar) PARTITION BY (id)",
              TEMP_SCHEMA, newTblName);
      final String alterQuery =
          String.format("ALTER TABLE %s.%s CLUSTER BY (name)", TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        validateErrorRunWithOtherOption(newTblName, alterQuery);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void alterClusteringKeyOnSortOrderTable() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "alterClusteringKeyOnSortOrderTable";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar) LOCALSORT BY (id)",
              TEMP_SCHEMA, newTblName);
      final String alterQuery =
          String.format("ALTER TABLE %s.%s CLUSTER BY (name)", TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        validateErrorRunWithOtherOption(newTblName, alterQuery);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void alterSortOrderOnClusterKeyTable() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "alterSortOrderOnClusterKeyTable";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar) CLUSTER BY (id)", TEMP_SCHEMA, newTblName);
      final String alterQuery =
          String.format("ALTER TABLE %s.%s LOCALSORT BY (name)", TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        validateErrorRunWithOtherOption(newTblName, alterQuery);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void alterPartitionOnClusterKeyTable() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "alterPartitionOnClusterKeyTable";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar) CLUSTER BY (id)", TEMP_SCHEMA, newTblName);
      final String alterQuery =
          String.format(
              "ALTER TABLE %s.%s ADD PARTITION FIELD IDENTITY(name)", TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        validateErrorRunWithOtherOption(newTblName, alterQuery);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void alterSortOrderWithIncorrectColumnName() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "alterSortOrderWithIncorrectColumnName";
      final String ctasQuery =
          String.format("CREATE TABLE %s.%s (id int, name varchar)", TEMP_SCHEMA, newTblName);
      final String alterQueryIncorrectName =
          String.format("ALTER TABLE %s.%s LOCALSORT BY (name1)", TEMP_SCHEMA, newTblName);
      final String alterQueryIncorrectCase =
          String.format("ALTER TABLE %s.%s LOCALSORT BY (Name)", TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        assertThrows(UserRemoteException.class, () -> runSQL(alterQueryIncorrectName));
        assertThrows(UserRemoteException.class, () -> runSQL(alterQueryIncorrectCase));
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void alterClusterKeyOnUnclusteredTable() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "alterClusterKeyOnUnclusteredTable";
      final String ctasQuery =
          String.format("CREATE TABLE %s.%s (id int, name varchar)", TEMP_SCHEMA, newTblName);
      final String alterQuery =
          String.format("ALTER TABLE %s.%s CLUSTER BY (name)", TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order not set
        validateSortColumnsFromMetadataJson(metadataJsonString, false);
        // Verify that dremio_use_sort_order_as_clustering_columns not set
        String showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
        validateTableProperty(showTablePropertiesQuery, false);

        runSQL(alterQuery);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets set
        showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
        validateTableProperty(showTablePropertiesQuery, true);
        metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets set
        validateSortColumnsFromMetadataJson(metadataJsonString, true);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void alterClusterKeyOnClusteredTable() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "alterClusterKeyOnClusteredTable";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar) CLUSTER BY (id)", TEMP_SCHEMA, newTblName);
      final String alterQuery =
          String.format("ALTER TABLE %s.%s CLUSTER BY (name)", TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets set
        validateSortColumnsFromMetadataJson(metadataJsonString, true);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets set
        String showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
        validateTableProperty(showTablePropertiesQuery, true);
        runSQL(alterQuery);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets set
        showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
        validateTableProperty(showTablePropertiesQuery, true);
        metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets set, and there are two sort orders
        validateSortColumnsFromMetadataJson(metadataJsonString, true);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void alterDropClusterKey() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "alterDropClusterKey";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar) CLUSTER BY (id)", TEMP_SCHEMA, newTblName);
      final String alterQuery =
          String.format("ALTER TABLE %s.%s DROP CLUSTERING KEY", TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets set
        validateSortColumnsFromMetadataJson(metadataJsonString, true);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets set
        String showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
        validateTableProperty(showTablePropertiesQuery, true);
        runSQL(alterQuery);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets unset
        showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
        validateTableProperty(showTablePropertiesQuery, false);
        metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets unset
        validateSortColumnsFromMetadataJson(metadataJsonString, false);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void alterDropClusterKeyOnUnclusteredTable() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "alterDropClusterKeyOnUnclusteredTable";
      final String ctasQuery =
          String.format("CREATE TABLE %s.%s (id int, name varchar)", TEMP_SCHEMA, newTblName);
      final String alterQuery =
          String.format("ALTER TABLE %s.%s DROP CLUSTERING KEY", TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        assertThrows(UserException.class, () -> runSQL(alterQuery));
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void alterMultipleClusterKeyOnClusteredTable() throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      final String newTblName = "alterMultipleClusterKeyOnClusteredTable";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar, description varchar) CLUSTER BY (id, name)",
              TEMP_SCHEMA, newTblName);
      final String alterQuery =
          String.format("ALTER TABLE %s.%s CLUSTER BY (description)", TEMP_SCHEMA, newTblName);

      try {
        runSQL(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets set
        validateSortColumnsFromMetadataJson(metadataJsonString, true);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets set
        String showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
        validateTableProperty(showTablePropertiesQuery, true);
        runSQL(alterQuery);
        // Verify that dremio_use_sort_order_as_clustering_columns table property gets set
        showTablePropertiesQuery =
            String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
        validateTableProperty(showTablePropertiesQuery, true);
        metadataJsonString = getMetadataJsonString(newTblName);
        // Verify that the sort order gets set, and there are two sort orders
        validateSortColumnsFromMetadataJson(metadataJsonString, true);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  private void validateTableProperty(String showTablePropertiesQuery, boolean propertyExpected)
      throws Exception {
    List<QueryDataBatch> queryDataBatches =
        BaseTestQuery.testRunAndReturn(UserBitShared.QueryType.SQL, showTablePropertiesQuery);
    String resultString = getResultString(queryDataBatches, ",", false);
    if (propertyExpected) {
      Assert.assertNotNull(resultString);
      Assert.assertTrue(resultString.contains("dremio_use_sort_order_as_clustering_columns,true"));
    } else {
      Assert.assertTrue(
          resultString == null
              || !resultString.contains("dremio_use_sort_order_as_clustering_columns,true"));
    }
  }

  private void validateErrorRunWithOtherOption(String tableName, String queryString)
      throws Exception {
    try (AutoCloseable ignored = enableIcebergAutoCluster()) {
      try {
        Exception e = assertThrows(UserException.class, () -> runSQL(queryString));
        assertTrue(e.getMessage().toLowerCase().contains("cluster"));
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  private void validateSortColumnsFromMetadataJson(String metadataJson, boolean sortOrderExpected) {
    JsonElement metadataJsonElement = new JsonParser().parse(metadataJson);
    JsonObject metadataJsonObject = metadataJsonElement.getAsJsonObject();
    if (!sortOrderExpected) {
      // Verify that the default sort order id set to 0
      Assert.assertTrue(metadataJsonObject.has("default-sort-order-id"));
      Assert.assertEquals(0, metadataJsonObject.get("default-sort-order-id").getAsInt());
    } else {
      // verify that there are sort order defined
      Assert.assertTrue(metadataJsonObject.has("sort-orders"));
      int expectedNumOfSortOrders = metadataJsonObject.get("sort-orders").getAsJsonArray().size();

      // Verify that the default sort order ID is the one that recently added
      JsonObject sortOrderJsonObject =
          metadataJsonObject
              .get("sort-orders")
              .getAsJsonArray()
              .get(expectedNumOfSortOrders - 1)
              .getAsJsonObject();
      int orderId = sortOrderJsonObject.get("order-id").getAsInt();
      Assert.assertTrue(metadataJsonObject.has("default-sort-order-id"));
      Assert.assertEquals(orderId, metadataJsonObject.get("default-sort-order-id").getAsInt());
    }
  }
}
