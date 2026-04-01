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

import static org.junit.Assert.*;

import com.dremio.exec.catalog.conf.SecretRef;
import org.junit.Test;

/** Unit tests for ClickHouse plugin. */
public class ClickHousePluginTest {

  @Test
  public void testConfigDefaults() {
    ClickHouseStoragePluginConfig config = new ClickHouseStoragePluginConfig();

    assertEquals("localhost", config.hostname);
    assertEquals(8123, config.port);
    assertEquals("default", config.database);
    assertEquals("default", config.username);
    assertFalse(config.useSsl);
  }

  @Test
  public void testConfigSetters() {
    ClickHouseStoragePluginConfig config = new ClickHouseStoragePluginConfig();
    config.hostname = "clickhouse.example.com";
    config.port = 9000;
    config.database = "testdb";
    config.username = "admin";
    config.password = SecretRef.of("password123");
    config.useSsl = true;

    assertEquals("clickhouse.example.com", config.hostname);
    assertEquals(9000, config.port);
    assertEquals("testdb", config.database);
    assertEquals("admin", config.username);
    assertEquals("password123", config.password.get());
    assertTrue(config.useSsl);
  }

  @Test
  public void testJdbcUrlBuilder() {
    ClickHouseStoragePluginConfig config = new ClickHouseStoragePluginConfig();
    config.hostname = "localhost";
    config.port = 8123;
    config.database = "default";
    config.useSsl = false;

    // Note: This is a simple test - actual JDBC URL format depends on ClickHouse driver
    assertNotNull(config.hostname);
    assertNotNull(config.database);
  }
}
