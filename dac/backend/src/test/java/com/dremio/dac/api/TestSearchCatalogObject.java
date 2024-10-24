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
package com.dremio.dac.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TestSearchCatalogObject {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testSerialization() throws Exception {
    List<String> path = Arrays.asList("root", "folder", "file");
    List<String> labels = Arrays.asList("label1", "label2");
    List<String> columns = Arrays.asList("column1", "column2");

    SearchCatalogObject searchCatalogObject =
        new SearchCatalogObject(
            path,
            "main",
            "TABLE",
            labels,
            "This is a wiki page.",
            "2023-08-15T10:00:00Z",
            "2023-08-16T12:00:00Z",
            columns,
            "SELECT * FROM table");

    String json = objectMapper.writeValueAsString(searchCatalogObject);

    String expectedJson =
        "{\"path\":[\"root\",\"folder\",\"file\"],"
            + "\"branch\":\"main\","
            + "\"type\":\"TABLE\","
            + "\"labels\":[\"label1\",\"label2\"],"
            + "\"wiki\":\"This is a wiki page.\","
            + "\"createdAt\":\"2023-08-15T10:00:00Z\","
            + "\"lastModifiedAt\":\"2023-08-16T12:00:00Z\","
            + "\"columns\":[\"column1\",\"column2\"],"
            + "\"functionSql\":\"SELECT * FROM table\"}";

    assertEquals(expectedJson, json);
  }

  @Test
  public void testDeserialization() throws Exception {
    String json =
        "{\"path\":[\"root\",\"folder\",\"file\"],"
            + "\"branch\":\"main\","
            + "\"type\":\"TABLE\","
            + "\"labels\":[\"label1\",\"label2\"],"
            + "\"wiki\":\"This is a wiki page.\","
            + "\"createdAt\":\"2023-08-15T10:00:00Z\","
            + "\"lastModifiedAt\":\"2023-08-16T12:00:00Z\","
            + "\"columns\":[\"column1\",\"column2\"],"
            + "\"functionSql\":\"SELECT * FROM table\"}";

    SearchCatalogObject searchCatalogObject =
        objectMapper.readValue(json, SearchCatalogObject.class);

    assertEquals(Arrays.asList("root", "folder", "file"), searchCatalogObject.getPath());
    assertEquals("main", searchCatalogObject.getBranch());
    assertEquals("TABLE", searchCatalogObject.getType());
    assertEquals(Arrays.asList("label1", "label2"), searchCatalogObject.getLabels());
    assertEquals("This is a wiki page.", searchCatalogObject.getWiki());
    assertEquals("2023-08-15T10:00:00Z", searchCatalogObject.getCreatedAt());
    assertEquals("2023-08-16T12:00:00Z", searchCatalogObject.getLastModifiedAt());
    assertEquals(Arrays.asList("column1", "column2"), searchCatalogObject.getColumns());
    assertEquals("SELECT * FROM table", searchCatalogObject.getFunctionSql());
  }
}
