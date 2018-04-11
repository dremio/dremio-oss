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
package com.dremio.exec.sql.hive;

import org.junit.Test;

import com.dremio.exec.hive.HiveTestBase;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.CatalogService.UpdateType;
import com.dremio.service.namespace.NamespaceKey;

public class TestMetadataRefresh extends HiveTestBase {

  @Test
  public void ensureFirstTableRefresh() throws Exception {
    // refresh the catalog to ensure we have a base config in the kvstore.
    getBindingProvider().lookup(CatalogService.class).refreshSource(new NamespaceKey("hive"), null, UpdateType.FULL);

    // now try to read refresh a table where we have a DatasetConfig but not a ReadDefinition.
    testBuilder()
    .sqlQuery("ALTER TABLE hive.`default`.kv REFRESH METADATA")
    .unOrdered()
    .baselineColumns("ok", "summary")
    .baselineValues(true, "Metadata for table 'hive.default.kv' refreshed.")
    .build().run();
  }
}
