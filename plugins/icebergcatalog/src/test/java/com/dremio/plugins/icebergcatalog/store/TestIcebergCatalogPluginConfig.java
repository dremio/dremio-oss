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
package com.dremio.plugins.icebergcatalog.store;

import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_ENABLED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.CacheProperties;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestIcebergCatalogPluginConfig extends BaseTestQuery {

  static class TestAbstractIcebergCatalogPluginConfig extends IcebergCatalogPluginConfig {

    public TestAbstractIcebergCatalogPluginConfig() {}

    @Override
    public CatalogAccessor createCatalog(Configuration config, SabotContext context) {
      return null;
    }
  }

  private final TestAbstractIcebergCatalogPluginConfig testAbstractIcebergCatalogPluginConfig =
      new TestAbstractIcebergCatalogPluginConfig();

  @Test
  public void testIcebergCatalogPluginConfigShouldPass() throws Exception {
    try (AutoCloseable ignored = withSystemOption(RESTCATALOG_PLUGIN_ENABLED, true)) {
      testAbstractIcebergCatalogPluginConfig.validateOnStart(getSabotContext());
    }
  }

  @Test
  public void testIcebergCatalogPluginConfigShouldThrowException() {
    assertThatThrownBy(
            () -> testAbstractIcebergCatalogPluginConfig.validateOnStart(getSabotContext()))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Iceberg Catalog Source is not supported.");
  }

  @Test
  public void testGetCacheProperties() {
    CacheProperties cacheProperties = testAbstractIcebergCatalogPluginConfig.getCacheProperties();
    assertEquals(
        testAbstractIcebergCatalogPluginConfig.isCachingEnabled,
        cacheProperties.isCachingEnabled(null));
    assertEquals(
        testAbstractIcebergCatalogPluginConfig.maxCacheSpacePct,
        cacheProperties.cacheMaxSpaceLimitPct());
  }
}
