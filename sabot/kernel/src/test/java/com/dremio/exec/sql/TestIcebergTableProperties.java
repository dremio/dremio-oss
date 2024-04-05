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

import com.dremio.BaseTestQuery;
import com.dremio.PlanTestBase;
import com.dremio.common.util.TestTools;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergTableProperties extends PlanTestBase {

  @Test
  public void ctasWithTableProperties() throws Exception {
    try (AutoCloseable ignored = enableIcebergTablePropertiesSupportFlag()) {
      final String newTblName = "ctasWithTableProperties";
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s TBLPROPERTIES ('property_name' = 'property_value') "
                  + " AS SELECT * from dfs.\""
                  + parquetFiles
                  + "\" limit 1",
              TEMP_SCHEMA_HADOOP,
              newTblName);
      final String showTablePropertiesQuery =
          String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA_HADOOP, newTblName);

      try {
        test(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        validateTablePropertiesFromMetadataJson(
            metadataJsonString, "property_name", "property_value");
        List<String> expectedResult =
            Stream.of("property_name", "property_value").collect(Collectors.toList());
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedResult);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void ctasWithMultiTableProperties() throws Exception {
    try (AutoCloseable ignored = enableIcebergTablePropertiesSupportFlag()) {
      final String newTblName = "ctasWithMultiTableProperties";
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s TBLPROPERTIES ('property_name' = 'property_value', 'property_name1' = 'property_value1') "
                  + " AS SELECT * from dfs.\""
                  + parquetFiles
                  + "\" limit 1",
              TEMP_SCHEMA_HADOOP,
              newTblName);
      final String showTablePropertiesQuery =
          String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA_HADOOP, newTblName);

      try {
        test(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        validateTablePropertiesFromMetadataJson(
            metadataJsonString, "property_name", "property_value");
        validateTablePropertiesFromMetadataJson(
            metadataJsonString, "property_name1", "property_value1");
        List<String> expectedResult =
            Stream.of("property_name", "property_value", "property_name1", "property_value1")
                .collect(Collectors.toList());
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedResult);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void createTableWithMultiTableProperties() throws Exception {
    try (AutoCloseable ignored = enableIcebergTablePropertiesSupportFlag()) {
      final String newTblName = "createTableWithMultiTableProperties";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3)) TBLPROPERTIES ('property_name' = 'property_value','property_name1' = 'property_value1')",
              TEMP_SCHEMA, newTblName);
      final String showTablePropertiesQuery =
          String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);

      try {
        test(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        validateTablePropertiesFromMetadataJson(
            metadataJsonString, "property_name", "property_value");
        validateTablePropertiesFromMetadataJson(
            metadataJsonString, "property_name1", "property_value1");
        List<String> expectedResult =
            Stream.of("property_name", "property_value", "property_name1", "property_value1")
                .collect(Collectors.toList());
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedResult);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void createTableWithTableProperties() throws Exception {
    try (AutoCloseable ignored = enableIcebergTablePropertiesSupportFlag()) {
      final String newTblName = "createTableWithTableProperties";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3)) TBLPROPERTIES ('property_name' = 'property_value')",
              TEMP_SCHEMA, newTblName);
      final String showTablePropertiesQuery =
          String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);

      try {
        test(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        validateTablePropertiesFromMetadataJson(
            metadataJsonString, "property_name", "property_value");
        List<String> expectedResult =
            Stream.of("property_name", "property_value").collect(Collectors.toList());
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedResult);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void createTableWithTablePropertiesSetUnset() throws Exception {
    try (AutoCloseable ignored = enableIcebergTablePropertiesSupportFlag()) {
      final String newTblName = "createTableWithTablePropertiesSetUnset";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3)) TBLPROPERTIES ('property_name' = 'property_value')",
              TEMP_SCHEMA, newTblName);
      final String showTablePropertiesQuery =
          String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
      final String setQuery =
          String.format(
              "ALTER TABLE %s.%s SET TBLPROPERTIES ('property_name' = 'new_property_value')",
              TEMP_SCHEMA, newTblName);
      final String unsetQuery =
          String.format(
              "ALTER TABLE %s.%s UNSET TBLPROPERTIES ('property_name')", TEMP_SCHEMA, newTblName);

      try {
        test(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        validateTablePropertiesFromMetadataJson(
            metadataJsonString, "property_name", "property_value");
        List<String> expectedResult =
            Stream.of("property_name", "property_value").collect(Collectors.toList());
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedResult);
        test(setQuery);
        List<String> expectedNewResult =
            Stream.of("property_name", "new_property_value").collect(Collectors.toList());
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedNewResult);
        test(unsetQuery);
        validateTablePropertiesNotExistViaShowTblproperties(
            showTablePropertiesQuery, "property_name");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void createTableWithoutTablePropertiesThenSetUnset() throws Exception {
    try (AutoCloseable ignored = enableIcebergTablePropertiesSupportFlag()) {
      final String newTblName = "createTableWithoutTablePropertiesThenSetUnset";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3))",
              TEMP_SCHEMA, newTblName);
      final String showTablePropertiesQuery =
          String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
      final String setQuery =
          String.format(
              "ALTER TABLE %s.%s SET TBLPROPERTIES ('property_name' = 'new_property_value')",
              TEMP_SCHEMA, newTblName);
      final String unsetQuery =
          String.format(
              "ALTER TABLE %s.%s UNSET TBLPROPERTIES ('property_name')", TEMP_SCHEMA, newTblName);

      try {
        test(ctasQuery);
        validateTablePropertiesNotExistViaShowTblproperties(
            showTablePropertiesQuery, "property_name");
        test(setQuery);
        List<String> expectedNewResult =
            Stream.of("property_name", "new_property_value").collect(Collectors.toList());
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedNewResult);
        test(unsetQuery);
        validateTablePropertiesNotExistViaShowTblproperties(
            showTablePropertiesQuery, "property_name");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void createTableThenSetMultiplePropertiesUnset() throws Exception {
    try (AutoCloseable ignored = enableIcebergTablePropertiesSupportFlag()) {
      final String newTblName = "createTableThenSetMultiplePropertiesUnset";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3)) TBLPROPERTIES ('property_name' = 'new_property_value', 'property_name1' = 'new_property_value1')",
              TEMP_SCHEMA, newTblName);
      final String showTablePropertiesQuery =
          String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
      final String setQuery =
          String.format(
              "ALTER TABLE %s.%s SET TBLPROPERTIES ('property_name' = 'new_property_value', 'property_name1' = 'new_property_value1')",
              TEMP_SCHEMA, newTblName);
      final String unsetQuery =
          String.format(
              "ALTER TABLE %s.%s UNSET TBLPROPERTIES ('property_name', 'property_name1')",
              TEMP_SCHEMA, newTblName);

      try {
        test(ctasQuery);
        List<String> expectedResult =
            Stream.of("property_name", "property_value", "property_name1", "property_value1")
                .collect(Collectors.toList());
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedResult);
        test(setQuery);
        List<String> expectedNewResult =
            Stream.of(
                    "property_name", "new_property_value", "property_name1", "new_property_value1")
                .collect(Collectors.toList());
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedNewResult);
        test(unsetQuery);
        validateTablePropertiesNotExistViaShowTblproperties(
            showTablePropertiesQuery, "property_name");
        validateTablePropertiesNotExistViaShowTblproperties(
            showTablePropertiesQuery, "property_name1");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void createTableThenUnsetNonExistingProperty() throws Exception {
    try (AutoCloseable ignored = enableIcebergTablePropertiesSupportFlag()) {
      final String newTblName = "createTableThenUnsetNonExistingProperty";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (id int, name varchar, distance Decimal(38, 3)) TBLPROPERTIES ('property_name' = 'property_value')",
              TEMP_SCHEMA, newTblName);
      final String showTablePropertiesQuery =
          String.format("SHOW TBLPROPERTIES %s.%s ", TEMP_SCHEMA, newTblName);
      final String unsetQuery =
          String.format(
              "ALTER TABLE %s.%s UNSET TBLPROPERTIES ('property_name1')", TEMP_SCHEMA, newTblName);

      try {
        test(ctasQuery);
        String metadataJsonString = getMetadataJsonString(newTblName);
        validateTablePropertiesFromMetadataJson(
            metadataJsonString, "property_name", "property_value");
        List<String> expectedResult =
            Stream.of("property_name", "property_value").collect(Collectors.toList());
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedResult);
        test(unsetQuery);
        validateTablePropertiesViaShowTblproperties(showTablePropertiesQuery, expectedResult);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  private String getMetadataJsonString(String tableName) throws IOException {
    String metadataLoc = getDfsTestTmpSchemaLocation() + String.format("/%s/metadata", tableName);
    File metadataFolder = new File(metadataLoc);
    String metadataJson = "";

    Assert.assertNotNull(metadataFolder.listFiles());
    for (File currFile : metadataFolder.listFiles()) {
      if (currFile.getName().endsWith("metadata.json")) {
        // We'll only have one metadata.json file as this is the first transaction for table
        // temp_table0
        metadataJson =
            new String(
                java.nio.file.Files.readAllBytes(Paths.get(currFile.getPath())),
                StandardCharsets.US_ASCII);
        break;
      }
    }
    Assert.assertNotNull(metadataJson);
    return metadataJson;
  }

  private void validateTablePropertiesFromMetadataJson(
      String metadataJson, String propertyName, String propertyValue) {
    JsonElement metadataJsonElement = new JsonParser().parse(metadataJson);
    JsonObject metadataJsonObject = metadataJsonElement.getAsJsonObject();

    JsonObject propertiesJson = metadataJsonObject.getAsJsonObject("properties");
    Assert.assertTrue(propertiesJson.has(propertyName));
    Assert.assertEquals(propertyValue, propertiesJson.get(propertyName).getAsString());
  }

  private void validateTablePropertiesViaShowTblproperties(
      String showTablePropertiesQuery, List<String> expectedResults) throws Exception {
    List<QueryDataBatch> queryDataBatches =
        BaseTestQuery.testRunAndReturn(UserBitShared.QueryType.SQL, showTablePropertiesQuery);
    String resultString = getResultString(queryDataBatches, ",", false);
    Assert.assertNotNull(resultString);
    for (String expectedResult : expectedResults) {
      Assert.assertTrue(resultString.contains(expectedResult));
    }
  }

  private void validateTablePropertiesNotExistViaShowTblproperties(
      String showTablePropertiesQuery, String propertyString) throws Exception {
    List<QueryDataBatch> queryDataBatches =
        BaseTestQuery.testRunAndReturn(UserBitShared.QueryType.SQL, showTablePropertiesQuery);
    String resultString = getResultString(queryDataBatches, ",", false);
    Assert.assertNotNull(resultString);
    Assert.assertTrue(
        resultString == null || resultString.isEmpty() || !resultString.contains(propertyString));
  }
}
