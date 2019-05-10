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
package com.dremio.exec.store;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.service.namespace.source.proto.SourceConfig;

public class TestMetadataRefresh extends BaseTestQuery {

  @Test
  public void testRefresh() throws Exception {
    Path root = Paths.get(getDfsTestTmpSchemaLocation(), "blue", "metadata_refresh");
    Files.createDirectories(root);

    Files.write(root.resolve("f1.json"), "{a:1}".getBytes(), StandardOpenOption.CREATE);

    // load the metadata
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Metadata for table 'dfs_test.blue.metadata_refresh' refreshed.")
      .build().run();

    testBuilder()
      .sqlQuery("select count(*) as a from dfs_test.blue.metadata_refresh")
      .unOrdered()
      .baselineColumns("a")
      .baselineValues(1L)
      .build().run();

    // pause to ensure mtime changes on directory
    Thread.sleep(1200);

    // write a second file.
    Files.write(root.resolve("f2.json"), "{a:1}".getBytes(), StandardOpenOption.CREATE);

    // no change expected, metadata cached.
    testBuilder()
      .sqlQuery("select count(*) as a from dfs_test.blue.metadata_refresh")
      .unOrdered()
      .baselineColumns("a")
      .baselineValues(1L)
      .build().run();

    // refresh metadata
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Metadata for table 'dfs_test.blue.metadata_refresh' refreshed.")
      .build().run();

    // expect two rows.
    testBuilder()
      .sqlQuery("select count(*) as a from dfs_test.blue.metadata_refresh")
      .unOrdered()
      .baselineColumns("a")
      .baselineValues(2L)
      .build().run();

    // refresh again, no change expected.
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table 'dfs_test.blue.metadata_refresh' read signature reviewed but source stated metadata is unchanged, no refresh occurred.")
      .build().run();

    // verify that REFRESH METADATA optional paramaters are being handled properly

    // LAZY UPDATE, no change expected
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA LAZY UPDATE")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table 'dfs_test.blue.metadata_refresh' read signature reviewed but source stated metadata is unchanged, no refresh occurred.")
      .build().run();

    // FORCE UPDATE on a PHYSICAL_DATASET_SOURCE_FILE, source should be refreshed
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh.\"f1.json\" REFRESH METADATA FORCE UPDATE")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Metadata for table 'dfs_test.blue.metadata_refresh.f1.json' refreshed.")
      .build().run();

    // LAZY UPDATE on a PHYSICAL_DATASET_SOURCE_FILE, source should be refreshed
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh.\"f1.json\" REFRESH METADATA LAZY UPDATE")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Metadata for table 'dfs_test.blue.metadata_refresh.f1.json' refreshed.")
      .build().run();

    // MAINTAIN WHEN MISSING, no change expected
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA MAINTAIN WHEN MISSING")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table 'dfs_test.blue.metadata_refresh' read signature reviewed but source stated metadata is unchanged, no refresh occurred.")
      .build().run();

    // DELETE WHEN MISSING, no change expected
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA DELETE WHEN MISSING")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table 'dfs_test.blue.metadata_refresh' read signature reviewed but source stated metadata is unchanged, no refresh occurred.")
      .build().run();

    // verify that we can pass in multiple parameters, no change expected
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA AVOID PROMOTION LAZY UPDATE MAINTAIN WHEN MISSING")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table 'dfs_test.blue.metadata_refresh' read signature reviewed but source stated metadata is unchanged, no refresh occurred.")
      .build().run();

    // delete the directory
    Files.delete(root.resolve("f1.json"));
    Files.delete(root.resolve("f2.json"));
    Files.delete(root);

    // MAINTAIN WHEN MISSING with missing data, no change expected
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA MAINTAIN WHEN MISSING")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table 'dfs_test.blue.metadata_refresh' read signature reviewed but source stated metadata is unchanged, no refresh occurred.")
      .build().run();

    // DELETE WHEN MISSING with missing data, expect all metadata to be removed
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA DELETE WHEN MISSING")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table 'dfs_test.blue.metadata_refresh' no longer exists, metadata removed.")
      .build().run();
  }

  @Test
  public void testRefreshWithoutAutoPromote() throws Exception {
    Path root = Paths.get(getDfsTestTmpSchemaLocation(), "blue", "metadata_refresh");
    Files.createDirectories(root);

    Files.write(root.resolve("f1.json"), "{a:1}".getBytes(), StandardOpenOption.CREATE);

    CatalogServiceImpl catalog = (CatalogServiceImpl) nodes[0].getContext().getCatalogService();
    String name = "dfs_test_without_autopromote";

    {
      SourceConfig c = new SourceConfig();
      InternalFileConf conf = new InternalFileConf();
      conf.connection = "file:///";
      conf.path = getDfsTestTmpSchemaLocation();
      c.setConnectionConf(conf);
      c.setName(name);
      c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
      catalog.getSystemUserCatalog().createSource(c);
    }

    // wait for source to be created
    Thread.sleep(1200);

    try {
      testBuilder()
        .sqlQuery("select count(*) as a from %s.blue.metadata_refresh", name)
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(1L)
        .build().run();
      fail("Source should be unavailable.");
    } catch (Exception e) {
      assertTrue(e.getMessage()
        .contains(String.format("Table '%s.blue.metadata_refresh' not found", name)));
    }

    // AUTO PROMOTION, data source should be promoted and Table metadata should be refreshed
    testBuilder()
      .sqlQuery("ALTER TABLE %s.blue.metadata_refresh REFRESH METADATA AUTO PROMOTION", name)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, String.format("Metadata for table '%s.blue.metadata_refresh' refreshed.", name))
      .build().run();

    testBuilder()
        .sqlQuery("select count(*) as a from %s.blue.metadata_refresh", name)
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(1L)
        .build().run();

    // delete all data from source
    Files.delete(root.resolve("f1.json"));
    Files.delete(root);

    // AVOID PROMOTION, no change expected
    testBuilder()
      .sqlQuery("ALTER TABLE %s.blue.metadata_refresh REFRESH METADATA AVOID PROMOTION MAINTAIN WHEN MISSING", name)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, String.format("Table '%s.blue.metadata_refresh' read signature reviewed but source stated metadata is unchanged, no refresh occurred.", name))
      .build().run();

    // verify we still have table information, but missing data
    try {
      testBuilder()
        .sqlQuery("select count(*) as a from %s.blue.metadata_refresh", name)
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(1L)
        .build().run();
      fail("Data should be unavailable.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("INVALID_DATASET_METADATA"));
      assertFalse(e.getMessage()
        .contains(String.format("Table '%s.blue.metadata_refresh' not found", name)));
    }

    // cleanup
    catalog.deleteSource(name);
  }
}
